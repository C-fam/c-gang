# -*- coding: utf-8 -*-
import os
import json
import logging
import asyncio
import re
from datetime import datetime, timezone
from math import ceil
from typing import Dict, Set, Optional, List, Any

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound, APIError

# --- ログ設定 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- 環境変数の読み込み ---
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")  # Discord Botのトークン
if TOKEN is None:
    logger.critical("BOT_TOKEN not found in environment variables. Exiting.")
    exit(1)

GOOGLE_CREDENTIALS_STR = os.getenv("GOOGLE_CREDENTIALS")  # .envにJSON形式で格納
if GOOGLE_CREDENTIALS_STR is None:
    logger.critical("GOOGLE_CREDENTIALS not found in environment variables. Exiting.")
    exit(1)

# --- Google Sheets 認証 ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SPREADSHEET_NAME = "C's Point Management Sheet"  # スプレッドシート名

# シート名定義
UID_LIST_SHEET = "UID_List"
GUILD_CONFIG_SHEET = "guild_config"
GRANTED_HISTORY_SHEET = "granted_history"
BONUS_LOG_SHEET = "Bonus_Log"

try:
    CREDS_DICT = json.loads(GOOGLE_CREDENTIALS_STR)
    CREDS = ServiceAccountCredentials.from_json_keyfile_dict(CREDS_DICT, SCOPE)
    GSPREAD_CLIENT = gspread.authorize(CREDS)
    SPREADSHEET = GSPREAD_CLIENT.open(SPREADSHEET_NAME)
    logger.info(f"Successfully connected to Google Spreadsheet: {SPREADSHEET_NAME}")
except json.JSONDecodeError as e:
    logger.critical(f"Failed to parse GOOGLE_CREDENTIALS: {e}. Exiting.")
    exit(1)
except Exception as e:
    logger.critical(f"Failed to authorize or open Google Sheets '{SPREADSHEET_NAME}': {e}. Exiting.")
    exit(1)

# --- 補助関数 ---
def format_iso_time(iso_str: Optional[str]) -> str:
    """ISO8601文字列を 'YYYY-MM-DD HH:MM:SS UTC' 形式に変換"""
    if not iso_str:
        return ""
    try:
        # オフセット情報（例: +00:00）があれば除去
        if '+' in iso_str:
            iso_str = iso_str.split('+')[0]
        # ミリ秒情報（例: .123456）があれば除去
        if '.' in iso_str:
             iso_str = iso_str.split('.')[0]
        dt = datetime.fromisoformat(iso_str).replace(tzinfo=timezone.utc) # UTCとして扱う
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        logger.warning(f"Could not parse date: {iso_str}. Returning original string.")
        # パースできない場合、元の文字列をそのまま返すか、エラーを示す文字列を返すか選択
        return iso_str

def parse_duration_to_seconds(text: str) -> int:
    """'15s', '30m', '2h', '1d' のような文字列を秒数に変換"""
    match = re.fullmatch(r"(\d+)\s*([smhd])", text.lower().strip())
    if not match:
        logger.warning(f"Invalid duration format: '{text}'. Using default 15s.")
        return 15  # デフォルト15秒
    num, unit = int(match.group(1)), match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return num * multipliers[unit]

# 定数：Embed の色
EMBED_COLOR = discord.Color(0x836EF9) # 元の #836EF9

# --- DataManager クラス ---
class DataManager:
    def __init__(self):
        """ボット全体のデータ管理を行うクラス"""
        self.valid_uids: Set[str] = set()             # 登録済UID一覧
        self.user_image_map: Dict[str, str] = {}      # UID -> 画像URL
        self.guild_config: Dict[str, Dict[str, Any]] = {} # {guild_id: {"server_name", "channel_id", "role_id", "message_id", "bonus_role_id"}}
        self.granted_history: Dict[str, List[Dict[str, str]]] = {} # {guild_id: [{"uid", "username", "time"}, ...]}

    async def _get_or_create_worksheet(self, sheet_name: str, rows: str = "1000", cols: str = "10") -> Optional[gspread.Worksheet]:
        """指定された名前のワークシートを取得、なければ作成して返す (非同期)"""
        def _sync_get_or_create():
            try:
                return SPREADSHEET.worksheet(sheet_name)
            except WorksheetNotFound:
                logger.info(f"Worksheet '{sheet_name}' not found, creating new one.")
                try:
                    # rows/cols は gspread v6+ では直接指定非推奨。必要なら後でリサイズ。
                    new_ws = SPREADSHEET.add_worksheet(title=sheet_name, rows=int(rows), cols=int(cols))
                    # ヘッダーが必要なシートもあるため、呼び出し元で適切に処理
                    return new_ws
                except APIError as e:
                    logger.error(f"Failed to create worksheet '{sheet_name}': {e}")
                    return None
            except APIError as e:
                logger.error(f"API error while getting worksheet '{sheet_name}': {e}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error getting or creating worksheet '{sheet_name}': {e}")
                return None
        return await asyncio.to_thread(_sync_get_or_create)

    async def load_uid_list_from_sheet(self):
        """'UID_List' シートから UID と 画像URL を読み込む (A列: Discord ID, B列: UID, C列: IMGURL 想定)"""
        ws = await self._get_or_create_worksheet(UID_LIST_SHEET, rows="1000", cols="3")
        if not ws:
            logger.error(f"Could not get or create '{UID_LIST_SHEET}' sheet.")
            return

        def _fetch_data():
            try:
                # head=1 で1行目をヘッダーとして辞書のリストを取得
                return ws.get_all_records(head=1)
            except APIError as e:
                logger.error(f"API error fetching data from '{UID_LIST_SHEET}': {e}")
                return []
            except Exception as e:
                logger.error(f"Error fetching data from '{UID_LIST_SHEET}': {e}")
                return []

        rows = await asyncio.to_thread(_fetch_data)

        new_uids = set()
        new_image_map = {}
        loaded_count = 0
        for row in rows:
            uid = str(row.get("UID", "")).strip() # B列
            img_url = str(row.get("IMGURL", "")).strip() # C列
            if uid:
                new_uids.add(uid)
                if img_url:
                    new_image_map[uid] = img_url
                loaded_count +=1

        self.valid_uids = new_uids
        self.user_image_map = new_image_map
        logger.info(f"Loaded {loaded_count} UIDs and {len(new_image_map)} image URLs from '{UID_LIST_SHEET}' sheet.")

    async def load_guild_config_sheet(self):
        """'guild_config' シートから設定を読み込み self.guild_config へ"""
        config = {}
        ws = await self._get_or_create_worksheet(GUILD_CONFIG_SHEET, rows="100", cols="6") # bonus_role_id 列追加
        if not ws:
            logger.error(f"Could not get or create '{GUILD_CONFIG_SHEET}' sheet.")
            return

        def _load():
            loaded_config = {}
            try:
                records = ws.get_all_records(head=1)
                for row in records:
                    guild_id = str(row.get("guild_id", "")).strip()
                    if guild_id:
                        try:
                            loaded_config[guild_id] = {
                                # すべて文字列として読み込む
                                "server_name": str(row.get("server_name", "")),
                                "channel_id": str(row.get("channel_id", "")).strip(),
                                "role_id": str(row.get("role_id", "")).strip(),
                                "message_id": str(row.get("message_id", "")).strip(),
                                "bonus_role_id": str(row.get("bonus_role_id", "")).strip(), # bonus_role_id も文字列で
                            }
                        except Exception as e:
                             logger.warning(f"Skipping invalid row in {GUILD_CONFIG_SHEET} for guild_id {guild_id}: {e} - Row data: {row}")
            except APIError as e:
                 logger.error(f"API error loading '{GUILD_CONFIG_SHEET}': {e}")
            except Exception as e:
                # ヘッダーがない、形式が違うなどでエラーになる可能性
                logger.error(f"Error reading '{GUILD_CONFIG_SHEET}' sheet: {e}. Ensure header row exists and format is correct.")
            return loaded_config

        self.guild_config = await asyncio.to_thread(_load)
        logger.info(f"Loaded guild configurations for {len(self.guild_config)} guilds from '{GUILD_CONFIG_SHEET}'.")

    async def save_guild_config_sheet(self):
        """self.guild_config を 'guild_config' シートに上書き保存"""
        ws = await self._get_or_create_worksheet(GUILD_CONFIG_SHEET, rows="100", cols="6")
        if not ws:
            logger.error(f"Could not get or create '{GUILD_CONFIG_SHEET}' sheet for saving.")
            return

        headers = ["guild_id", "server_name", "channel_id", "role_id", "message_id", "bonus_role_id"]
        data_to_write = [headers]
        for gid, conf in self.guild_config.items():
            row = [
                str(gid), # 文字列で保存
                str(conf.get("server_name", "")),
                str(conf.get("channel_id", "")),    # 文字列で保存
                str(conf.get("role_id", "")),       # 文字列で保存
                str(conf.get("message_id", "")),   # 文字列で保存
                str(conf.get("bonus_role_id", "")), # 文字列で保存
            ]
            data_to_write.append(row)

        def _update():
            try:
                ws.clear()
                # value_input_option='USER_ENTERED' で Sheets 側の自動型変換を抑制
                ws.update('A1', data_to_write, value_input_option='USER_ENTERED')
                logger.info(f"Guild config sheet '{GUILD_CONFIG_SHEET}' saved successfully.")
            except APIError as e:
                logger.error(f"API error saving '{GUILD_CONFIG_SHEET}' sheet: {e}")
            except Exception as e:
                 logger.error(f"Unexpected error saving '{GUILD_CONFIG_SHEET}' sheet: {e}")

        await asyncio.to_thread(_update)

    async def load_granted_history_sheet(self):
        """'granted_history' シートを読み込み self.granted_history に格納"""
        history = {}
        ws = await self._get_or_create_worksheet(GRANTED_HISTORY_SHEET, rows="1000", cols="4")
        if not ws:
            logger.error(f"Could not get or create '{GRANTED_HISTORY_SHEET}' sheet.")
            return

        def _load():
            loaded_history = {}
            try:
                records = ws.get_all_records(head=1)
                for row in records:
                    guild_id = str(row.get("guild_id", "")).strip()
                    if guild_id:
                         # UIDはシングルクォート付きの場合もあるのでそのまま文字列で読み込む
                         uid = str(row.get("uid", "")).strip()
                         username = str(row.get("username", "")).strip()
                         time_str = str(row.get("time", "")).strip() # YYYY-MM-DD HH:MM:SS UTC 形式想定

                         loaded_history.setdefault(guild_id, []).append({
                            "uid": uid,
                            "username": username,
                            "time": time_str # 読み込み時はパースせず文字列のまま
                        })
            except APIError as e:
                logger.error(f"API error loading '{GRANTED_HISTORY_SHEET}': {e}")
            except Exception as e:
                logger.error(f"Error reading '{GRANTED_HISTORY_SHEET}' sheet: {e}")
            return loaded_history

        self.granted_history = await asyncio.to_thread(_load)
        logger.info(f"Loaded granted history for {len(self.granted_history)} guilds from '{GRANTED_HISTORY_SHEET}'.")

    async def save_granted_history_sheet(self):
        """self.granted_history を 'granted_history' シートに上書き保存"""
        ws = await self._get_or_create_worksheet(GRANTED_HISTORY_SHEET, rows="1000", cols="4")
        if not ws:
            logger.error(f"Could not get or create '{GRANTED_HISTORY_SHEET}' sheet for saving.")
            return

        headers = ["guild_id", "uid", "username", "time"]
        data_to_write = [headers]
        for gid, records in self.granted_history.items():
            for record in records:
                raw_uid = str(record.get("uid", ""))
                # UID が数字のみの場合、Google Sheetsが数値と誤認しないようにシングルクォートを付与 (USER_ENTEREDでも念のため)
                uid_str = f"'{raw_uid}" if raw_uid.isdigit() and not raw_uid.startswith("'") else raw_uid

                time_val = record.get("time", "") # ISO format か フォーマット済み文字列
                time_str = format_iso_time(time_val) # 保存前に 'YYYY-MM-DD HH:MM:SS UTC' 形式に統一

                row = [
                    str(gid), # 文字列で保存
                    uid_str,  # シングルクォート付きまたはそのままの文字列
                    str(record.get("username", "")),
                    time_str
                ]
                data_to_write.append(row)

        def _update():
            try:
                ws.clear()
                ws.update('A1', data_to_write, value_input_option='USER_ENTERED')
                logger.info(f"Granted history sheet '{GRANTED_HISTORY_SHEET}' saved successfully.")
            except APIError as e:
                logger.error(f"API error saving '{GRANTED_HISTORY_SHEET}' sheet: {e}")
            except Exception as e:
                logger.error(f"Unexpected error saving '{GRANTED_HISTORY_SHEET}' sheet: {e}")

        await asyncio.to_thread(_update)

    async def append_bonus_log_to_sheet(self, guild_id: str, username: str, uid: str, timestamp: str):
        """'Bonus_Log' シートへ新しいログを1行追記"""
        ws = await self._get_or_create_worksheet(BONUS_LOG_SHEET, rows="1000", cols="4") # bonus log 用
        if not ws:
            logger.error(f"Could not get or create '{BONUS_LOG_SHEET}' sheet for logging.")
            return

        # UID が数字のみの場合、シングルクォートを付与
        uid_str = f"'{uid}" if uid.isdigit() and not uid.startswith("'") else uid
        time_str = format_iso_time(timestamp) # YYYY-MM-DD HH:MM:SS UTC

        def _ensure_header_and_append():
            try:
                header = ws.row_values(1)
                expected_header = ["guild_id", "username", "uid", "timestamp"]
                if not header or header != expected_header:
                    ws.insert_row(expected_header, 1)
                    logger.info(f"Header written to '{BONUS_LOG_SHEET}' sheet.")
                    # ヘッダー挿入後、追記
                    ws.append_row([str(guild_id), username, uid_str, time_str], value_input_option='USER_ENTERED')
                else:
                    # ヘッダーがあれば追記のみ
                    ws.append_row([str(guild_id), username, uid_str, time_str], value_input_option='USER_ENTERED')
                # logger.info(f"Appended bonus log for user {uid} in guild {guild_id}.") # ログ追記は頻繁なので抑制しても良い
            except APIError as e:
                logger.error(f"API error appending log to '{BONUS_LOG_SHEET}': {e}")
            except Exception as e:
                logger.error(f"Failed to append log to sheet '{BONUS_LOG_SHEET}': {e}")

        await asyncio.to_thread(_ensure_header_and_append)

    async def load_all_data(self):
        """Bot起動時に全データを読み込む"""
        logger.info("Loading all data from Google Sheets...")
        await self.load_uid_list_from_sheet()
        await self.load_guild_config_sheet()
        await self.load_granted_history_sheet()
        # Bonus_Log は追記型なので起動時に読み込む必要はない
        logger.info("Finished loading initial data.")

    # save_all_data は通常不要
    # async def save_all_data(self): ...

data_manager = DataManager()

# --- Discord Bot の準備 ---
intents = discord.Intents.default()
intents.members = True # メンバー情報の取得に必要
intents.message_content = True # v1.5以降、メッセージ内容の取得に必要 (今は使っていないがあれば安心)
bot = commands.Bot(command_prefix="!", intents=intents)

# --- 永続的な UI コンポーネント ---

# 1. Check Eligibility ボタン (ロール付与処理)
class CheckEligibilityButton(discord.ui.Button):
    def __init__(self, custom_id="check_eligibility_button_v2"): # IDは変更しないことが望ましい
        super().__init__(
            custom_id=custom_id,
            label="Check Eligibility",
            style=discord.ButtonStyle.primary
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild:
             return await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        guild_id_str = str(interaction.guild.id)
        user_id_str = str(interaction.user.id)

        # --- UID チェック ---
        if user_id_str not in data_manager.valid_uids:
            logger.info(f"Eligibility check failed for UID: {user_id_str} in guild {guild_id_str}. Not in valid_uids.")
            return await interaction.response.send_message(
                f"Sorry, you are not on the eligibility list (Your UID: {user_id_str}).", ephemeral=True
            )

        # --- ギルド設定チェック ---
        guild_config = data_manager.guild_config.get(guild_id_str)
        if not guild_config:
            logger.warning(f"No setup found for guild {guild_id_str} (Name: {interaction.guild.name}).")
            return await interaction.response.send_message(
                "Bot setup is not complete in this server. Please ask an administrator to run `/setup`.", ephemeral=True
            )

        # --- ロールID検証と取得 ---
        role_id_str = guild_config.get("role_id")
        if not role_id_str or not role_id_str.isdigit():
            logger.error(f"Invalid or missing role_id in config for guild {guild_id_str}.")
            return await interaction.response.send_message(
                "Configuration error: Role ID is invalid. Please contact an administrator.", ephemeral=True
            )
        role_id = int(role_id_str)
        role = interaction.guild.get_role(role_id)
        if not role:
            logger.warning(f"Configured role (ID: {role_id}) not found in guild {guild_id_str}.")
            return await interaction.response.send_message(
                "Configuration error: The assigned role could not be found. Please contact an administrator.", ephemeral=True
            )

        # --- 既にロールを持っているかチェック ---
        if isinstance(interaction.user, discord.Member) and role in interaction.user.roles:
            return await interaction.response.send_message(
                f"You already have the {role.mention} role.", ephemeral=True
            )

        # --- ロール付与実行 ---
        try:
            if isinstance(interaction.user, discord.Member):
                await interaction.user.add_roles(role, reason="Eligibility check passed")
                logger.info(f"Granted role '{role.name}' to user {interaction.user} (ID: {user_id_str}) in guild {guild_id_str}.")
            else:
                 # interaction.user が Member でない場合（DMなどでは発生しないはずだが念のため）
                 logger.warning(f"Cannot grant role to user {user_id_str} as they are not a Member object.")
                 return await interaction.response.send_message("Could not retrieve your member information to grant the role.", ephemeral=True)

        except discord.Forbidden:
            logger.error(f"Failed to grant role '{role.name}' to user {user_id_str} in guild {guild_id_str}. Bot lacks permissions.")
            return await interaction.response.send_message(
                "Error: I couldn't grant the role. Please ensure I have the 'Manage Roles' permission and my role is above the target role.", ephemeral=True
            )
        except discord.HTTPException as e:
             logger.error(f"Failed to grant role '{role.name}' due to an HTTP error: {e}")
             return await interaction.response.send_message(
                f"An error occurred while trying to grant the role: {e}", ephemeral=True
            )

        # --- 成功メッセージ送信（画像付き） ---
        response_text = f"You are **eligible** (UID: {user_id_str}). Role {role.mention} has been granted!"
        response_embed = None
        image_url = data_manager.user_image_map.get(user_id_str)
        if image_url:
            response_embed = discord.Embed(
                title="Eligibility Confirmed & Your C Image",
                description=f"Role {role.mention} granted!",
                color=EMBED_COLOR
            )
            response_embed.set_image(url=image_url)
            response_embed.set_footer(text=f"UID: {user_id_str}")

        await interaction.response.send_message(
            content=response_text if not response_embed else None, # Embedがある場合は content は空にするのが一般的
            embed=response_embed,
            ephemeral=True
        )

        # --- バックグラウンドで履歴保存 ---
        # Logシートへの追記は削除
        async def background_save_history():
            timestamp = datetime.now(timezone.utc).isoformat() # UTCで記録
            history_entry = {
                "uid": user_id_str,
                "username": str(interaction.user), # username#discriminator
                "time": timestamp
            }
            # メモリ上の履歴に追加
            data_manager.granted_history.setdefault(guild_id_str, []).append(history_entry)
            try:
                # Google Sheets に保存 (非同期)
                await data_manager.save_granted_history_sheet()
            except Exception as e:
                # バックグラウンドタスクでのエラーはユーザーには通知せず、ログに残す
                logger.error(f"Error saving granted history for user {user_id_str} in guild {guild_id_str}: {e}")

        asyncio.create_task(background_save_history())


# 2. Check Your C ボタン (画像確認のみ)
class CheckYourCButton(discord.ui.Button):
    def __init__(self, custom_id="check_your_c_button_v2"):
        super().__init__(
            custom_id=custom_id,
            label="Check Your C",
            style=discord.ButtonStyle.secondary # Primaryと区別するためSecondaryに変更
        )

    async def callback(self, interaction: discord.Interaction):
        user_id_str = str(interaction.user.id)

        # UID登録チェック (ロール付与はしない)
        if user_id_str not in data_manager.valid_uids:
            return await interaction.response.send_message(
                f"Your UID ({user_id_str}) is not found in the registered list.",
                ephemeral=True
            )

        # 画像URL取得と表示
        image_url = data_manager.user_image_map.get(user_id_str)
        if not image_url:
            return await interaction.response.send_message(
                "Your UID is registered, but no image URL is associated with it.",
                ephemeral=True
            )

        embed = discord.Embed(
            title="Your C Image",
            description="Here is the C image associated with your UID.",
            color=EMBED_COLOR
        )
        embed.set_image(url=image_url)
        embed.set_footer(text=f"UID: {user_id_str}")
        await interaction.response.send_message(embed=embed, ephemeral=True)


# 3. CombinedView: 2つのボタンをまとめる (永続ビュー)
class CombinedView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None) # 永続化
        # 永続ビューのボタンには custom_id が必須
        self.add_item(CheckEligibilityButton()) # custom_id はボタンクラス内で定義
        self.add_item(CheckYourCButton())       # custom_id はボタンクラス内で定義

# 4. Bonus Button (一時的なボタン)
class BonusButton(discord.ui.Button):
    # log_func は data_manager.append_bonus_log_to_sheet を想定
    def __init__(self, log_func: callable, guild_id: str):
        super().__init__(label="Claim Bonus", style=discord.ButtonStyle.success, custom_id=f"bonus_claim_{guild_id}_{datetime.now(timezone.utc).timestamp()}") # 一意なID生成
        self.log_func = log_func
        self.guild_id = guild_id # ログ記録用に guild_id を保持

    async def callback(self, interaction: discord.Interaction):
        # interaction.guild.id は interaction が発生したギルドIDであり、ボタン設置ギルドIDと一致するはず
        # self.guild_id はボタン作成時に指定されたギルドID
        username = str(interaction.user)
        uid = str(interaction.user.id)
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            # 非同期関数のはずなので await する
            await self.log_func(self.guild_id, username, uid, timestamp)
            await interaction.response.send_message(
                "✅ Bonus claimed and logged! Your participation has been recorded.", ephemeral=True
            )
            # ボタンを無効化して重複クリックを防ぐ（任意）
            self.disabled = True
            self.label = "Claimed"
            await interaction.edit_original_response(view=self.view) # view=self.view で更新
            logger.info(f"Bonus claimed by {username} ({uid}) in guild {self.guild_id}")

        except Exception as e:
            logger.error(f"Error logging bonus claim for user {uid} in guild {self.guild_id}: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while logging your claim. Please try again or contact an admin.", ephemeral=True
            )

# 5. BonusView: BonusButtonを配置 (タイムアウトあり)
class BonusView(discord.ui.View):
    # log_func と guild_id を BonusButton に渡す
    def __init__(self, log_func: callable, guild_id: str, timeout: float):
        super().__init__(timeout=timeout)
        self.add_item(BonusButton(log_func, guild_id))
    # タイムアウト時の処理（任意）
    async def on_timeout(self):
         # タイムアウトしたらボタンを無効化
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        # メッセージを編集してタイムアウトしたことを示す（メッセージオブジェクトが必要）
        # このViewはメッセージ送信後に使われるので、メッセージへの参照を保持する必要がある
        # logger.info(f"Bonus view timed out for guild {self.message.guild.id if self.message else 'Unknown'}")
        # ここでメッセージを編集するのは難しいので、ボタンが無効になるだけとする


# --- 履歴表示用のページング UI (管理者向け) ---
class HistoryPagerView(discord.ui.View):
    def __init__(self, records: List[Dict[str, str]]):
        super().__init__(timeout=180) # 3分でタイムアウト
        self.records = records # 新しい順にソートされている想定
        self.current_page = 0
        self.per_page = 10
        self.total_pages = ceil(len(self.records) / self.per_page) if self.records else 1

        # ボタンの初期状態を設定
        self.prev_button = PrevPageButton(disabled=(self.current_page == 0))
        self.next_button = NextPageButton(disabled=(self.current_page >= self.total_pages - 1))
        self.add_item(self.prev_button)
        self.add_item(self.next_button)

    def update_buttons(self):
        """ページ番号に基づいてボタンの有効/無効を更新"""
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page >= self.total_pages - 1)

    def get_page_embed(self):
        """現在のページのEmbedを生成"""
        start_index = self.current_page * self.per_page
        end_index = start_index + self.per_page
        page_records = self.records[start_index:end_index]

        description_lines = [
            "Role assignment history for this server (most recent first).",
            "" # 空行
        ]
        if not page_records:
            description_lines.append("No assignments found on this page.")
        else:
            for i, record in enumerate(page_records, start=start_index + 1):
                # UIDからシングルクォートを除去してメンションを作成
                uid_raw = record.get("uid", "Unknown UID")
                uid_clean = uid_raw.lstrip("'")
                username = record.get("username", "Unknown User")
                time_str = record.get("time", "Unknown Time") # YYYY-MM-DD HH:MM:SS UTC 想定
                description_lines.append(f"{i}. <@{uid_clean}> ({username}) - {time_str}")

        embed = discord.Embed(
            title="Role Assignment History",
            description="\n".join(description_lines),
            color=EMBED_COLOR
        )
        embed.set_footer(text=f"Page {self.current_page + 1}/{self.total_pages} (Total {len(self.records)} assignments)")
        return embed

    async def update_message(self, interaction: discord.Interaction):
        """ボタン操作に応じてメッセージを更新"""
        self.update_buttons()
        embed = self.get_page_embed()
        await interaction.response.edit_message(embed=embed, view=self)

class PrevPageButton(discord.ui.Button):
    def __init__(self, disabled=False):
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.secondary, disabled=disabled, custom_id="history_prev_page")

    async def callback(self, interaction: discord.Interaction):
        view: HistoryPagerView = self.view # type: ignore
        if view.current_page > 0:
            view.current_page -= 1
            await view.update_message(interaction)
        else:
            # ボタンが無効なはずだが念のため応答だけ返す
            await interaction.response.defer()


class NextPageButton(discord.ui.Button):
    def __init__(self, disabled=False):
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary, disabled=disabled, custom_id="history_next_page")

    async def callback(self, interaction: discord.Interaction):
        view: HistoryPagerView = self.view # type: ignore
        if view.current_page < view.total_pages - 1:
            view.current_page += 1
            await view.update_message(interaction)
        else:
             # ボタンが無効なはずだが念のため応答だけ返す
            await interaction.response.defer()

# --- Bot イベント ---
@bot.event
async def on_ready():
    """ボット起動時の処理"""
    logger.info(f"Bot logged in as {bot.user.name} ({bot.user.id})")
    try:
        # Google Sheetsからデータをロード
        await data_manager.load_all_data()
    except Exception as e:
        # 起動時のデータロード失敗は致命的な可能性がある
        logger.critical(f"CRITICAL: Error loading initial data in on_ready: {e}", exc_info=True)
        # ここでボットを終了させるか、限定的な機能で起動し続けるか選択
        # exit(1)

    # 永続ビューをリスナーに追加
    # CombinedView は永続 (timeout=None)
    bot.add_view(CombinedView())
    logger.info("Persistent CombinedView added.")
    # HistoryPagerView はタイムアウトありなので on_ready で追加しない

    # スラッシュコマンドを同期
    try:
        # グローバルに同期する場合
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
        # 特定ギルドのみでテストする場合:
        # test_guild_id = 123456789012345678 # あなたのテストサーバーID
        # guild = discord.Object(id=test_guild_id)
        # bot.tree.copy_global_to(guild=guild)
        # synced = await bot.tree.sync(guild=guild)
        # logger.info(f"Synced {len(synced)} slash commands to guild {test_guild_id}.")
    except Exception as e:
        logger.error(f"Error syncing slash commands: {e}")


@bot.event
async def on_guild_join(guild: discord.Guild):
    """ボットが新しいサーバーに参加したときのログ"""
    logger.info(f"Joined new guild: {guild.name} (ID: {guild.id}, Owner: {guild.owner})")

@bot.event
async def on_guild_remove(guild: discord.Guild):
    """ボットがサーバーから退出したときの処理"""
    logger.info(f"Removed from guild: {guild.name} (ID: {guild.id})")
    guild_id_str = str(guild.id)
    config_changed = False
    # ギルド設定をメモリから削除
    if guild_id_str in data_manager.guild_config:
        del data_manager.guild_config[guild_id_str]
        logger.info(f"Removed configuration for guild {guild_id_str} from memory.")
        config_changed = True
    # ギルド履歴をメモリから削除
    if guild_id_str in data_manager.granted_history:
        del data_manager.granted_history[guild_id_str]
        logger.info(f"Removed history for guild {guild_id_str} from memory.")
        # 注意: granted_history シートからの削除は reset_history 同様、煩雑なのでここでは行わない
        # 必要であれば save_granted_history_sheet を呼び出す前に該当ギルド分を除外する処理を追加

    # 設定が変更された場合、シートに保存する (任意)
    if config_changed:
        try:
            # このギルドの設定を削除した状態でシートを保存
            await data_manager.save_guild_config_sheet()
        except Exception as e:
             logger.error(f"Failed to save guild config after removing guild {guild_id_str}: {e}")


@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """スラッシュコマンドのエラーハンドリング"""
    command_name = interaction.command.name if interaction.command else "Unknown Command"
    logger.error(f"Error in slash command '{command_name}': {error}", exc_info=True) # トレースバックも記録

    error_message = "An unexpected error occurred. Please try again later or contact an administrator."
    if isinstance(error, app_commands.CommandNotFound):
        # 通常発生しないはずだが念のため
        error_message = "Sorry, I don't recognize that command."
    elif isinstance(error, app_commands.MissingPermissions):
        perms = ", ".join(error.missing_permissions)
        error_message = f"You lack the required permissions to use this command: `{perms}`"
    elif isinstance(error, app_commands.BotMissingPermissions):
        perms = ", ".join(error.missing_permissions)
        error_message = f"I lack the required permissions to perform this action: `{perms}`. Please check my permissions in this server/channel."
    elif isinstance(error, app_commands.NoPrivateMessage):
         error_message = "This command cannot be used in Direct Messages."
    elif isinstance(error, app_commands.CheckFailure):
         # カスタムチェックや @app_commands.check デコレータでの失敗
         error_message = "You do not meet the requirements to use this command."
    elif isinstance(error, app_commands.CommandOnCooldown):
         error_message = f"This command is on cooldown. Please try again in {error.retry_after:.2f} seconds."
    elif isinstance(error, app_commands.TransformerError): # 引数の変換エラー
         error_message = f"Invalid input provided: {error}"
    # 必要に応じて他のエラータイプ (e.g., app_commands.ArgumentParsingError) もハンドル

    try:
        # response.send_message は最初の応答、followup.send は2回目以降の応答
        send_method = interaction.followup.send if interaction.response.is_done() else interaction.response.send_message
        await send_method(error_message, ephemeral=True)
    except discord.NotFound:
         logger.warning("Interaction was not found when trying to send error message (maybe it expired?).")
    except discord.HTTPException as e:
        logger.error(f"Failed to send error message for command '{command_name}': {e}")


# --- スラッシュコマンド ---

@bot.tree.command(name="setup", description="Post/Update the eligibility buttons and set the role.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    channel="Channel for the eligibility buttons.",
    role="Role to grant to eligible users."
)
async def setup_command(interaction: discord.Interaction, channel: discord.TextChannel, role: discord.Role):
    """セットアップコマンド: ボタンを投稿/更新し、設定を保存する"""
    if not interaction.guild:
        return await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    # --- 権限チェック ---
    if not interaction.app_permissions.manage_roles:
         return await interaction.response.send_message("I need the 'Manage Roles' permission.", ephemeral=True)
    if interaction.guild.me.top_role <= role:
         return await interaction.response.send_message(
            f"My highest role ('{interaction.guild.me.top_role.name}') isn't high enough to manage the '{role.name}' role. Please move my role higher.",
            ephemeral=True)
    if not channel.permissions_for(interaction.guild.me).send_messages or \
       not channel.permissions_for(interaction.guild.me).embed_links or \
       not channel.permissions_for(interaction.guild.me).read_message_history or \
       not channel.permissions_for(interaction.guild.me).manage_messages: # メッセージ編集/取得のため
         return await interaction.response.send_message(
            f"I need permissions to 'Send Messages', 'Embed Links', 'Read Message History', and 'Manage Messages' in {channel.mention}.", ephemeral=True)

    await interaction.response.defer(ephemeral=True) # 時間がかかる可能性

    embed = discord.Embed(
        title="Check Eligibility & C Image",
        description="Click the buttons below:\n"
                    "1. **Check Eligibility**: Grants the designated role if you are on the list and shows your C image.\n"
                    "2. **Check Your C**: Shows your C image without granting the role.",
        color=EMBED_COLOR
    )
    view = CombinedView() # 永続ビューを使用

    message_id_to_save = None
    message_link = "Not available"
    operation_type = "created" # "created" or "updated"

    # --- 既存メッセージの更新試行 ---
    old_config = data_manager.guild_config.get(guild_id_str)
    if old_config and old_config.get("message_id") and old_config.get("channel_id") == str(channel.id):
        old_msg_id_str = old_config["message_id"]
        if old_msg_id_str.isdigit():
            try:
                old_msg = await channel.fetch_message(int(old_msg_id_str))
                await old_msg.edit(embed=embed, view=view)
                message_id_to_save = old_msg.id
                message_link = old_msg.jump_url
                operation_type = "updated"
                logger.info(f"Updated existing eligibility message {message_id_to_save} in guild {guild_id_str}, channel {channel.id}.")
            except discord.NotFound:
                logger.warning(f"Old message (ID: {old_msg_id_str}) not found in channel {channel.id}. Creating a new message.")
            except discord.Forbidden:
                logger.error(f"Failed to edit old message (ID: {old_msg_id_str}) in channel {channel.id}. Insufficient permissions.")
                # 編集権限がない場合はフォローアップで通知し、新規作成は行わない方が混乱が少ないかも
                return await interaction.followup.send(f"Failed to update: I lack permission to edit the existing message in {channel.mention}. Please check my permissions or delete the old message manually.", ephemeral=True)
            except discord.HTTPException as e:
                logger.error(f"Failed to edit old message (ID: {old_msg_id_str}) due to HTTP error: {e}")
                return await interaction.followup.send(f"An error occurred while trying to update the message: {e}", ephemeral=True)

    # --- 新規メッセージ作成 ---
    if message_id_to_save is None:
        try:
            new_msg = await channel.send(embed=embed, view=view)
            message_id_to_save = new_msg.id
            message_link = new_msg.jump_url
            logger.info(f"Sent new eligibility message {message_id_to_save} to guild {guild_id_str}, channel {channel.id}.")
        except discord.Forbidden:
            logger.error(f"Failed to send message to channel {channel.id}. Insufficient permissions.")
            return await interaction.followup.send(f"Failed to send message: I lack permission to send messages in {channel.mention}.", ephemeral=True)
        except discord.HTTPException as e:
            logger.error(f"Failed to send message to channel {channel.id} due to HTTP error: {e}")
            return await interaction.followup.send(f"An error occurred while trying to send the message: {e}", ephemeral=True)

    # --- 設定の保存 ---
    if message_id_to_save:
        # 既存の設定に上書き、なければ新規作成
        current_config = data_manager.guild_config.get(guild_id_str, {})
        current_config.update({
            "server_name": interaction.guild.name,
            "channel_id": str(channel.id),
            "role_id": str(role.id),
            "message_id": str(message_id_to_save),
            # bonus_role_id はこのコマンドでは変更しないので、既存の値を維持
            # "bonus_role_id": current_config.get("bonus_role_id", "")
        })
        data_manager.guild_config[guild_id_str] = current_config # 更新/新規設定
        await data_manager.save_guild_config_sheet()

        await interaction.followup.send(
            f"Setup {operation_type} successfully! Buttons are active in {channel.mention} (<{message_link}>).\n"
            f"Eligible users will receive the {role.mention} role.",
            ephemeral=True
        )
    else:
        # メッセージ送信/編集に失敗した場合
        await interaction.followup.send("Setup failed. Could not post or update the buttons message.", ephemeral=True)


@bot.tree.command(name="reloadlist", description="Reload the eligible user list and images from the sheet.")
@app_commands.default_permissions(administrator=True)
async def reloadlist_command(interaction: discord.Interaction):
    """リロードコマンド: UID_List シートからUIDと画像URLを再読み込み"""
    await interaction.response.defer(ephemeral=True)
    try:
        await data_manager.load_uid_list_from_sheet()
        uid_count = len(data_manager.valid_uids)
        img_count = len(data_manager.user_image_map)
        await interaction.followup.send(
            f"Successfully reloaded data from the '{UID_LIST_SHEET}' sheet.\n"
            f"- UIDs loaded: {uid_count}\n"
            f"- Image URLs found: {img_count}",
            ephemeral=True
        )
    except Exception as e:
        logger.error(f"Error during /reloadlist: {e}", exc_info=True)
        await interaction.followup.send(f"An error occurred while reloading the list: {e}", ephemeral=True)


@bot.tree.command(name="history", description="Show the role assignment history for this server (paginated).")
@app_commands.default_permissions(administrator=True)
async def history_command(interaction: discord.Interaction):
    """履歴表示コマンド: このサーバーのロール付与履歴を表示"""
    if not interaction.guild:
        return await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    await interaction.response.defer(ephemeral=True)

    try:
        # 常に最新の履歴をシートから読み込む
        await data_manager.load_granted_history_sheet()
        records = data_manager.granted_history.get(guild_id_str, [])

        if not records:
            return await interaction.followup.send("No role assignment history found for this server.", ephemeral=True)

        # 履歴は新しいものが最後に追加されるので、表示のために逆順（新しい順）にする
        records_display_order = sorted(records, key=lambda x: x.get('time', ''), reverse=True)

        view = HistoryPagerView(records_display_order)
        embed = view.get_page_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    except Exception as e:
        logger.error(f"Error during /history: {e}", exc_info=True)
        await interaction.followup.send(f"An error occurred while fetching history: {e}", ephemeral=True)


@bot.tree.command(name="extractinfo", description="Show current setup info and recent role assignments.")
@app_commands.default_permissions(administrator=True)
async def extractinfo_command(interaction: discord.Interaction):
    """情報抽出コマンド: 現在の設定と最近のロール付与履歴を表示"""
    if not interaction.guild:
        return await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    await interaction.response.defer(ephemeral=True)

    try:
        # 最新の設定と履歴を読み込む
        await data_manager.load_guild_config_sheet()
        await data_manager.load_granted_history_sheet()

        config = data_manager.guild_config.get(guild_id_str)
        history = data_manager.granted_history.get(guild_id_str, [])

        if not config:
            return await interaction.followup.send("No setup information found for this server. Run `/setup` first.", ephemeral=True)

        # 設定情報を取得（文字列として）
        ch_id = config.get("channel_id", "N/A")
        role_id = config.get("role_id", "N/A")
        msg_id = config.get("message_id", "N/A")
        bonus_role_id = config.get("bonus_role_id", "") # bonus_role_id も取得

        channel_mention = f"<#{ch_id}>" if ch_id.isdigit() else "Invalid/Not set"
        role_mention = f"<@&{role_id}>" if role_id.isdigit() else "Invalid/Not set"
        bonus_role_mention = f"<@&{bonus_role_id}>" if bonus_role_id.isdigit() else "Not set"
        msg_link = "N/A"
        if ch_id.isdigit() and msg_id.isdigit():
            msg_link = f"https://discord.com/channels/{guild_id_str}/{ch_id}/{msg_id}"

        report_lines = [
            f"**⚙️ Server Configuration for {interaction.guild.name}**",
            f"- Server Name: {config.get('server_name', 'N/A')}",
            f"- Buttons Channel: {channel_mention} (ID: `{ch_id}`)",
            f"- Eligibility Role: {role_mention} (ID: `{role_id}`)",
            f"- Buttons Message: {msg_link} (ID: `{msg_id}`)",
            f"- Bonus Command Role: {bonus_role_mention} (ID: `{bonus_role_id if bonus_role_id else 'N/A'}`)",
            f"\n**📜 Recent Role Grants (last 10)** (Total: {len(history)})"
        ]

        # 履歴は新しい順に最大10件表示
        recent_history = sorted(history, key=lambda x: x.get('time', ''), reverse=True)[:10]

        if not recent_history:
            report_lines.append("- No recent assignments found.")
        else:
            for i, record in enumerate(recent_history, start=1):
                uid_clean = record.get('uid', 'Unknown').lstrip("'")
                username = record.get('username', 'Unknown User')
                time_str = record.get('time', 'Unknown Time')
                report_lines.append(f"{i}. <@{uid_clean}> (`{username}`) - {time_str}")

        report = "\n".join(report_lines)
        # メッセージ長チェック (Discord制限 2000文字)
        if len(report) > 2000:
            report = report[:1997] + "..."

        await interaction.followup.send(report, ephemeral=True)

    except Exception as e:
        logger.error(f"Error during /extractinfo: {e}", exc_info=True)
        await interaction.followup.send(f"An error occurred while extracting info: {e}", ephemeral=True)


@bot.tree.command(name="reset_history", description="⚠️ Reset the role assignment history for this server.")
@app_commands.default_permissions(administrator=True)
async def reset_history_command(interaction: discord.Interaction):
    """履歴リセットコマンド: このサーバーのロール付与履歴を消去"""
    if not interaction.guild:
        return await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    await interaction.response.defer(ephemeral=True)

    # メモリ上の履歴をクリア
    if guild_id_str in data_manager.granted_history:
        data_manager.granted_history[guild_id_str] = []
        logger.info(f"Cleared history for guild {guild_id_str} from memory.")
    else:
        logger.info(f"No history found in memory for guild {guild_id_str} to clear.")

    # Google Sheets 上の履歴もクリア (該当ギルドのみ削除)
    ws = await data_manager._get_or_create_worksheet(GRANTED_HISTORY_SHEET)
    if ws:
        def _clear_guild_history_from_sheet():
            try:
                all_records_with_headers = ws.get_all_values() # ヘッダー含む全行取得
                if not all_records_with_headers: return # 空なら何もしない

                header = all_records_with_headers[0]
                rows_to_keep = [header] # ヘッダーは保持
                deleted_count = 0

                # ヘッダーから guild_id の列インデックスを取得 (デフォルトは0)
                guild_id_col_index = 0
                try:
                    guild_id_col_index = header.index("guild_id")
                except ValueError:
                    logger.warning(f"'guild_id' column not found in header of '{GRANTED_HISTORY_SHEET}'. Assuming first column.")

                # ヘッダー以外の行をチェック
                for row in all_records_with_headers[1:]:
                    # 列数が足りない行はスキップ
                    if len(row) <= guild_id_col_index:
                        continue
                    # 該当ギルドIDでない行のみ保持
                    if row[guild_id_col_index] != guild_id_str:
                        rows_to_keep.append(row)
                    else:
                        deleted_count += 1

                # シートをクリアして保持する行だけ書き戻す
                ws.clear()
                ws.update('A1', rows_to_keep, value_input_option='USER_ENTERED')
                logger.info(f"Removed {deleted_count} history entries for guild {guild_id_str} from sheet '{GRANTED_HISTORY_SHEET}'.")
                return deleted_count
            except APIError as e:
                 logger.error(f"API error clearing history for guild {guild_id_str} in sheet: {e}")
                 raise # エラーを呼び出し元に伝える
            except Exception as e:
                 logger.error(f"Unexpected error clearing history for guild {guild_id_str} in sheet: {e}")
                 raise # エラーを呼び出し元に伝える

        try:
            deleted_count = await asyncio.to_thread(_clear_guild_history_from_sheet)
            await interaction.followup.send(
                f"Role assignment history for **{interaction.guild.name}** has been reset. {deleted_count} entries removed from the sheet.",
                ephemeral=True
            )
        except Exception: # シート操作でエラーが発生した場合
             await interaction.followup.send(
                 f"History in memory was cleared, but an error occurred while updating the Google Sheet. Please check the logs.",
                 ephemeral=True
             )
    else:
        await interaction.followup.send(
            f"Could not access the history sheet '{GRANTED_HISTORY_SHEET}'. History reset failed for the sheet.",
            ephemeral=True
        )

# --- Bonus Feature Commands ---

@bot.tree.command(name="bonus_setting", description="Set the role allowed to use the /bonus command.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(role="Role that can execute the /bonus command.")
async def bonus_setting_command(interaction: discord.Interaction, role: discord.Role):
    """ボーナス設定コマンド: /bonus コマンド実行許可ロールを設定"""
    if not interaction.guild:
        return await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    # ギルド設定を取得（なければ空辞書）
    conf = data_manager.guild_config.get(guild_id_str, {})
    # bonus_role_id を更新
    conf["bonus_role_id"] = str(role.id)
    # サーバー名も念のため更新（存在しない場合もあるので）
    conf["server_name"] = interaction.guild.name

    # 更新した設定を data_manager に反映
    data_manager.guild_config[guild_id_str] = conf

    try:
        await data_manager.save_guild_config_sheet()
        await interaction.response.send_message(
            f"✅ Success! Users with the {role.mention} role can now use the `/bonus` command in this server.",
            ephemeral=True
        )
    except Exception as e:
         logger.error(f"Failed to save bonus role setting for guild {guild_id_str}: {e}", exc_info=True)
         await interaction.response.send_message(
            f"❌ Failed to save the setting due to an error: {e}", ephemeral=True
        )

@bot.tree.command(name="bonus", description="Post a temporary button for users to claim a bonus.")
@app_commands.describe(
    channel="Channel where the bonus button will appear.",
    duration="Lifetime of the button (e.g., '15s', '30m', '1h', '2d'). Default: 15s."
)
async def bonus_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    duration: str = "15s"
):
    """ボーナスコマンド: 指定時間有効なボーナス請求ボタンを投稿"""
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("This command can only be used in a server by a server member.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    # --- 権限チェック ---
    # 1. コマンド実行者が必要なロールを持っているか、または管理者か
    conf = data_manager.guild_config.get(guild_id_str, {})
    bonus_role_id_str = conf.get("bonus_role_id")
    required_role_id = int(bonus_role_id_str) if bonus_role_id_str and bonus_role_id_str.isdigit() else None

    is_admin = interaction.user.guild_permissions.administrator
    has_bonus_role = required_role_id and any(r.id == required_role_id for r in interaction.user.roles)

    if not (is_admin or has_bonus_role):
        role_mention = f"<@&{required_role_id}>" if required_role_id else "the designated role"
        return await interaction.response.send_message(
            f"❌ You need Administrator permissions or the {role_mention} (set via `/bonus_setting`) to use this command.",
            ephemeral=True
        )

    # 2. ボットが指定チャンネルにメッセージを送信/削除できるか
    if not channel.permissions_for(interaction.guild.me).send_messages or \
       not channel.permissions_for(interaction.guild.me).manage_messages: # メッセージ削除のため
        return await interaction.response.send_message(
            f"❌ I need permissions to 'Send Messages' and 'Manage Messages' in {channel.mention} to post and manage the bonus button.",
            ephemeral=True
        )

    # --- ボタン表示期間のパース ---
    try:
        seconds = parse_duration_to_seconds(duration)
        if seconds <= 0:
            return await interaction.response.send_message("❌ Duration must be positive.", ephemeral=True)
    except ValueError:
        return await interaction.response.send_message("❌ Invalid duration format. Use e.g., '15s', '10m', '1h'.", ephemeral=True)

    # --- ボタン付きメッセージ送信 ---
    # BonusView に DataManager のログ関数と guild_id、タイムアウト秒数を渡す
    view = BonusView(data_manager.append_bonus_log_to_sheet, guild_id_str, timeout=float(seconds))
    try:
        msg = await channel.send(
            f"⏳ **Bonus Claim Available!** Press the button within **{duration}** to record your participation!",
            view=view
        )
        await interaction.response.send_message(
            f"✅ Bonus button posted to {channel.mention}. It will be active for **{duration}**.",
            ephemeral=True
        )
        logger.info(f"Bonus button posted in guild {guild_id_str}, channel {channel.id} by {interaction.user} for {duration}.")
    except discord.Forbidden:
        logger.error(f"Failed to send bonus button to channel {channel.id}. Insufficient permissions.")
        # defer() していないので response.send_message でエラーを返す
        return await interaction.response.send_message(f"❌ Failed to post button: I lack permission to send messages in {channel.mention}.", ephemeral=True)
    except discord.HTTPException as e:
        logger.error(f"Failed to send bonus button to channel {channel.id}: {e}")
        return await interaction.response.send_message(f"❌ Failed to post button due to an error: {e}", ephemeral=True)

    # --- メッセージ自動削除タスク ---
    # view.on_timeout でボタンは無効化されるが、メッセージ自体も削除する
    async def auto_delete_message():
        await asyncio.sleep(seconds)
        try:
            await msg.delete()
            logger.info(f"Auto-deleted bonus message {msg.id} in guild {guild_id_str}, channel {channel.id}.")
        except discord.NotFound:
            logger.warning(f"Bonus message {msg.id} not found for auto-deletion (already deleted?).")
        except discord.Forbidden:
             logger.error(f"Failed to auto-delete bonus message {msg.id}. Insufficient permissions.")
        except Exception as e:
            logger.error(f"Error auto-deleting bonus message {msg.id}: {e}")

    asyncio.create_task(auto_delete_message())


# --- Bot 実行 ---
if __name__ == "__main__":
    try:
        logger.info("Starting bot...")
        bot.run(TOKEN)
    except discord.LoginFailure:
        logger.critical("Failed to log in. Check your BOT_TOKEN.")
    except Exception as e:
        logger.critical(f"Fatal error running the bot: {e}", exc_info=True)

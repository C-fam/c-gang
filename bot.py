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
TOKEN = os.getenv("BOT_TOKEN")
if TOKEN is None:
    logger.critical("BOT_TOKEN not found in environment variables. Exiting.")
    exit(1)

GOOGLE_CREDENTIALS_STR = os.getenv("GOOGLE_CREDENTIALS")
if GOOGLE_CREDENTIALS_STR is None:
    logger.critical("GOOGLE_CREDENTIALS not found in environment variables. Exiting.")
    exit(1)

# --- Google Sheets 認証 ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SPREADSHEET_NAME = "C's Point Management Sheet" # ★★★ あなたのスプレッドシート名に変更 ★★★

# シート名定義 (定数化)
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
        # 基本的なパース試行
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        # タイムゾーン情報がない場合はUTCとみなす
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # UTCに変換してフォーマット
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        # 特殊なケース（ミリ秒や予期せぬオフセット）に対応しようと試みる
        try:
            # ミリ秒 (.xxx) を除去
            if '.' in iso_str:
                iso_str = iso_str.split('.')[0]
            # Zを+00:00に置換
            iso_str = iso_str.replace('Z', '+00:00')
            # 再度パース試行
            dt = datetime.fromisoformat(iso_str)
            if dt.tzinfo is None:
                 dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception: # それでもダメなら元の文字列を返す
             logger.warning(f"Could not parse date: {iso_str}. Returning original string.")
             return iso_str

def parse_duration_to_seconds(text: str) -> int:
    """'15s', '30m', '2h', '1d' のような文字列を秒数に変換"""
    match = re.fullmatch(r"(\d+)\s*([smhd])", text.lower().strip())
    if not match:
        logger.warning(f"Invalid duration format: '{text}'. Using default 15s.")
        return 15
    num, unit = int(match.group(1)), match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    seconds = num * multipliers[unit]
    # Discordのタイムアウト上限 (View: 15分*4 = 3600s * 4 = 14400s?) を考慮するかどうか
    # ここでは特に上限は設けない
    return seconds

# 定数：Embed の色
EMBED_COLOR = discord.Color(0x836EF9)

# --- DataManager クラス ---
class DataManager:
    def __init__(self):
        """ボット全体のデータ管理"""
        self.valid_uids: Set[str] = set()
        self.user_image_map: Dict[str, str] = {}
        # guild_config の型定義を明確化
        self.guild_config: Dict[str, Dict[str, Any]] = {}
        # 例: { "guild_id_str": {
        #          "server_name": "Server Name",
        #          "channel_id": "123...",
        #          "role_id": "456...",
        #          "message_id": "789...",
        #          "bonus_role_ids": ["111...", "222..."] # 文字列のリスト
        #      }
        #    }
        self.granted_history: Dict[str, List[Dict[str, str]]] = {}

    async def _get_or_create_worksheet(self, sheet_name: str, rows: str = "1000", cols: str = "10") -> Optional[gspread.Worksheet]:
        """ワークシート取得/作成 (非同期)"""
        def _sync_get_or_create():
            try:
                return SPREADSHEET.worksheet(sheet_name)
            except WorksheetNotFound:
                logger.info(f"Worksheet '{sheet_name}' not found, creating...")
                try:
                    return SPREADSHEET.add_worksheet(title=sheet_name, rows=int(rows), cols=int(cols))
                except APIError as e_create:
                    logger.error(f"API error creating worksheet '{sheet_name}': {e_create}")
                    return None
            except APIError as e_get:
                logger.error(f"API error getting worksheet '{sheet_name}': {e_get}")
                return None
            except Exception as e_other:
                logger.error(f"Unexpected error with worksheet '{sheet_name}': {e_other}")
                return None
        return await asyncio.to_thread(_sync_get_or_create)

    async def load_uid_list_from_sheet(self):
        """UID_Listシート読み込み"""
        ws = await self._get_or_create_worksheet(UID_LIST_SHEET, rows="1000", cols="3")
        if not ws: return

        def _fetch_data():
            try: return ws.get_all_records(head=1)
            except Exception as e:
                logger.error(f"Error fetching data from '{UID_LIST_SHEET}': {e}")
                return []
        rows = await asyncio.to_thread(_fetch_data)

        new_uids, new_image_map = set(), {}
        for row in rows:
            uid = str(row.get("UID", "")).strip()
            img_url = str(row.get("IMGURL", "")).strip()
            if uid:
                new_uids.add(uid)
                if img_url: new_image_map[uid] = img_url
        self.valid_uids, self.user_image_map = new_uids, new_image_map
        logger.info(f"Loaded {len(self.valid_uids)} UIDs, {len(self.user_image_map)} image URLs from '{UID_LIST_SHEET}'.")

    async def load_guild_config_sheet(self):
        """guild_configシート読み込み (bonus_role_ids対応)"""
        config = {}
        # bonus_role_ids 列を追加したので cols=6 に
        ws = await self._get_or_create_worksheet(GUILD_CONFIG_SHEET, rows="100", cols="6")
        if not ws: return

        def _load():
            loaded_config = {}
            try:
                records = ws.get_all_records(head=1)
                for row in records:
                    guild_id = str(row.get("guild_id", "")).strip()
                    if guild_id:
                        bonus_role_ids_str = str(row.get("bonus_role_ids", "")).strip()
                        # カンマ区切り文字列をリストに変換 (空文字列の場合は空リスト)
                        bonus_role_ids_list = [rid.strip() for rid in bonus_role_ids_str.split(',') if rid.strip().isdigit()]

                        loaded_config[guild_id] = {
                            "server_name": str(row.get("server_name", "")),
                            "channel_id": str(row.get("channel_id", "")).strip(),
                            "role_id": str(row.get("role_id", "")).strip(),
                            "message_id": str(row.get("message_id", "")).strip(),
                            "bonus_role_ids": bonus_role_ids_list # 文字列リストとして格納
                        }
            except APIError as e_api:
                 logger.error(f"API error loading '{GUILD_CONFIG_SHEET}': {e_api}")
            except Exception as e_other:
                logger.error(f"Error reading '{GUILD_CONFIG_SHEET}': {e_other}. Check sheet format.")
            return loaded_config

        self.guild_config = await asyncio.to_thread(_load)
        logger.info(f"Loaded {len(self.guild_config)} guild configurations from '{GUILD_CONFIG_SHEET}'.")

    async def save_guild_config_sheet(self):
        """guild_configシート保存 (bonus_role_ids対応)"""
        ws = await self._get_or_create_worksheet(GUILD_CONFIG_SHEET, rows="100", cols="6")
        if not ws: return

        # ヘッダー修正
        headers = ["guild_id", "server_name", "channel_id", "role_id", "message_id", "bonus_role_ids"]
        data_to_write = [headers]
        for gid, conf in self.guild_config.items():
            # bonus_role_idsリストをカンマ区切り文字列に変換
            bonus_ids_list = conf.get("bonus_role_ids", []) # list[str] を想定
            bonus_ids_str = ",".join(filter(None, bonus_ids_list)) # Noneや空文字を除外して結合

            row = [
                str(gid),
                str(conf.get("server_name", "")),
                str(conf.get("channel_id", "")),
                str(conf.get("role_id", "")),
                str(conf.get("message_id", "")),
                bonus_ids_str # カンマ区切り文字列として保存
            ]
            data_to_write.append(row)

        def _update():
            try:
                ws.clear()
                ws.update('A1', data_to_write, value_input_option='USER_ENTERED')
                logger.info(f"Guild config sheet '{GUILD_CONFIG_SHEET}' saved successfully.")
            except Exception as e:
                 logger.error(f"Error saving '{GUILD_CONFIG_SHEET}': {e}", exc_info=True)
                 raise # 保存失敗を呼び出し元に伝える

        try:
            await asyncio.to_thread(_update)
        except Exception: # _update内で発生した例外をキャッチ
             # 必要に応じてここで追加のフォールバック処理
             pass


    async def load_granted_history_sheet(self):
        """granted_historyシート読み込み"""
        history = {}
        ws = await self._get_or_create_worksheet(GRANTED_HISTORY_SHEET, rows="1000", cols="4")
        if not ws: return

        def _load():
            loaded_history = {}
            try:
                records = ws.get_all_records(head=1)
                for row in records:
                    guild_id = str(row.get("guild_id", "")).strip()
                    if guild_id:
                         uid = str(row.get("uid", "")).strip()
                         username = str(row.get("username", "")).strip()
                         time_str = str(row.get("time", "")).strip()
                         loaded_history.setdefault(guild_id, []).append({
                            "uid": uid, "username": username, "time": time_str })
            except Exception as e: logger.error(f"Error reading '{GRANTED_HISTORY_SHEET}': {e}")
            return loaded_history
        self.granted_history = await asyncio.to_thread(_load)
        logger.info(f"Loaded granted history for {len(self.granted_history)} guilds from '{GRANTED_HISTORY_SHEET}'.")

    async def save_granted_history_sheet(self):
        """granted_historyシート保存"""
        ws = await self._get_or_create_worksheet(GRANTED_HISTORY_SHEET, rows="1000", cols="4")
        if not ws: return

        headers = ["guild_id", "uid", "username", "time"]
        data_to_write = [headers]
        for gid, records in self.granted_history.items():
            for record in records:
                raw_uid = str(record.get("uid", ""))
                uid_str = f"'{raw_uid}" if raw_uid.isdigit() and not raw_uid.startswith("'") else raw_uid
                time_str = format_iso_time(record.get("time", ""))
                row = [str(gid), uid_str, str(record.get("username", "")), time_str]
                data_to_write.append(row)

        def _update():
            try:
                ws.clear()
                ws.update('A1', data_to_write, value_input_option='USER_ENTERED')
                logger.info(f"Granted history sheet '{GRANTED_HISTORY_SHEET}' saved.")
            except Exception as e: logger.error(f"Error saving '{GRANTED_HISTORY_SHEET}': {e}", exc_info=True)

        await asyncio.to_thread(_update)

    async def append_bonus_log_to_sheet(self, guild_id: str, username: str, uid: str, timestamp: str):
        """Bonus_Logシートへ追記"""
        ws = await self._get_or_create_worksheet(BONUS_LOG_SHEET, rows="1000", cols="4")
        if not ws: return

        uid_str = f"'{uid}" if uid.isdigit() and not uid.startswith("'") else uid
        time_str = format_iso_time(timestamp)

        def _ensure_header_and_append():
            try:
                header = ws.row_values(1)
                expected_header = ["guild_id", "username", "uid", "timestamp"]
                row_data = [str(guild_id), username, uid_str, time_str]
                if not header or header != expected_header:
                    # 既存データを保持しつつヘッダー挿入は複雑なので、クリアしてヘッダー＋データが安全かも
                    # ws.clear() # もしヘッダーがなければクリアする？運用による
                    ws.insert_row(expected_header, 1)
                    logger.info(f"Header written to '{BONUS_LOG_SHEET}'.")
                    ws.append_row(row_data, value_input_option='USER_ENTERED') # ヘッダー後に追加
                else:
                    ws.append_row(row_data, value_input_option='USER_ENTERED')
            except Exception as e: logger.error(f"Failed to append log to '{BONUS_LOG_SHEET}': {e}")

        await asyncio.to_thread(_ensure_header_and_append)

    async def load_all_data(self):
        """起動時に全データ読み込み"""
        logger.info("Loading all data from Google Sheets...")
        await self.load_uid_list_from_sheet()
        await self.load_guild_config_sheet()
        await self.load_granted_history_sheet()
        logger.info("Finished loading initial data.")

data_manager = DataManager()

# --- Discord Bot の準備 ---
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# --- 永続的な UI コンポーネント ---

class CheckEligibilityButton(discord.ui.Button):
    def __init__(self, custom_id="check_eligibility_button_v2"):
        super().__init__(custom_id=custom_id, label="Check Eligibility", style=discord.ButtonStyle.primary)

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
             return await interaction.response.send_message("Cannot perform this action here.", ephemeral=True)
        guild_id_str = str(interaction.guild.id)
        user_id_str = str(interaction.user.id)

        if user_id_str not in data_manager.valid_uids:
            return await interaction.response.send_message(f"Sorry, you are not eligible (UID: {user_id_str}).", ephemeral=True)

        guild_config = data_manager.guild_config.get(guild_id_str)
        if not guild_config:
            return await interaction.response.send_message("Bot setup needed. Contact admin.", ephemeral=True)

        role_id_str = guild_config.get("role_id")
        if not role_id_str or not role_id_str.isdigit():
            return await interaction.response.send_message("Config error: Role ID invalid.", ephemeral=True)
        role_id = int(role_id_str)
        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.response.send_message("Config error: Role not found.", ephemeral=True)

        if role in interaction.user.roles:
            return await interaction.response.send_message(f"You already have the {role.mention} role.", ephemeral=True)

        try:
            await interaction.user.add_roles(role, reason="Eligibility check passed")
            logger.info(f"Granted role '{role.name}' to {interaction.user} (ID: {user_id_str}) in guild {guild_id_str}.")
        except discord.Forbidden:
            logger.error(f"Failed to grant role to {user_id_str} in guild {guild_id_str}. Permissions missing.")
            return await interaction.response.send_message("Error: Could not grant role (permissions?).", ephemeral=True)
        except discord.HTTPException as e:
             logger.error(f"HTTP error granting role to {user_id_str}: {e}")
             return await interaction.response.send_message(f"Error granting role: {e}", ephemeral=True)

        response_text = f"You are **eligible** (UID: {user_id_str}). Role {role.mention} granted!"
        response_embed = None
        image_url = data_manager.user_image_map.get(user_id_str)
        if image_url:
            response_embed = discord.Embed(title="Eligibility Confirmed & Your C Image", description=f"Role {role.mention} granted!", color=EMBED_COLOR)
            response_embed.set_image(url=image_url)
            response_embed.set_footer(text=f"UID: {user_id_str}")

        await interaction.response.send_message(content=response_text if not response_embed else None, embed=response_embed, ephemeral=True)

        async def background_save_history():
            timestamp = datetime.now(timezone.utc).isoformat()
            history_entry = {"uid": user_id_str, "username": str(interaction.user), "time": timestamp}
            data_manager.granted_history.setdefault(guild_id_str, []).append(history_entry)
            try: await data_manager.save_granted_history_sheet()
            except Exception as e: logger.error(f"Error saving history for {user_id_str} in {guild_id_str}: {e}")
        asyncio.create_task(background_save_history())

class CheckYourCButton(discord.ui.Button):
    def __init__(self, custom_id="check_your_c_button_v2"):
        super().__init__(custom_id=custom_id, label="Check Your C", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        user_id_str = str(interaction.user.id)
        if user_id_str not in data_manager.valid_uids:
            return await interaction.response.send_message(f"Your UID ({user_id_str}) not found.", ephemeral=True)
        image_url = data_manager.user_image_map.get(user_id_str)
        if not image_url:
            return await interaction.response.send_message("Your UID is registered, but no image URL found.", ephemeral=True)
        embed = discord.Embed(title="Your C Image", description="Here is your C image.", color=EMBED_COLOR)
        embed.set_image(url=image_url)
        embed.set_footer(text=f"UID: {user_id_str}")
        await interaction.response.send_message(embed=embed, ephemeral=True)

class CombinedView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(CheckEligibilityButton())
        self.add_item(CheckYourCButton())

# --- Bonus UI ---
class BonusButton(discord.ui.Button):
    def __init__(self, log_func: callable, guild_id: str):
        # custom_id にタイムスタンプを含めて毎回一意にする（永続化しないので問題ない）
        super().__init__(label="Claim Bonus", style=discord.ButtonStyle.success, custom_id=f"bonus_claim_{guild_id}_{datetime.now(timezone.utc).timestamp()}")
        self.log_func = log_func
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        username = str(interaction.user)
        uid = str(interaction.user.id)
        timestamp = datetime.now(timezone.utc).isoformat()
        try:
            await self.log_func(self.guild_id, username, uid, timestamp) # 修正：guild_id を渡す
            await interaction.response.send_message("✅ Bonus claimed and logged!", ephemeral=True)
            self.disabled = True
            self.label = "Claimed"
            await interaction.edit_original_response(view=self.view)
            logger.info(f"Bonus claimed by {username} ({uid}) in guild {self.guild_id}")
        except Exception as e:
            logger.error(f"Error logging bonus claim for {uid} in guild {self.guild_id}: {e}")
            # 応答済みの場合があるので followup で送信試行
            try:
                await interaction.followup.send("❌ Error logging claim. Contact admin.", ephemeral=True)
            except discord.HTTPException: # 最初の応答も失敗していた場合
                 try: await interaction.response.send_message("❌ Error logging claim. Contact admin.", ephemeral=True)
                 except discord.HTTPException: pass # どうしようもない

class BonusView(discord.ui.View):
    def __init__(self, log_func: callable, guild_id: str, timeout: float):
        super().__init__(timeout=timeout)
        self.log_func = log_func
        self.guild_id = guild_id
        self.message: Optional[discord.Message] = None # メッセージ参照保持用
        self.add_item(BonusButton(log_func, guild_id))

    async def on_timeout(self):
        for item in self.children:
            if isinstance(item, discord.ui.Button): item.disabled = True
        if self.message:
            try:
                # タイムアウトしたらメッセージも編集してボタンを無効化表示
                await self.message.edit(content=f"~~{self.message.content}~~\nBonus claim period ended.", view=self)
                logger.info(f"Bonus view timed out and button disabled for message {self.message.id}")
            except discord.NotFound: pass # メッセージが既に消えている
            except discord.Forbidden: logger.error(f"Missing permissions to edit message {self.message.id} on timeout.")
            except Exception as e: logger.error(f"Error editing message on timeout: {e}")
        else:
            logger.warning("BonusView timed out but message reference was lost.")

# --- History Pager UI ---
class HistoryPagerView(discord.ui.View):
    def __init__(self, records: List[Dict[str, str]]):
        super().__init__(timeout=180)
        self.records = records
        self.current_page = 0
        self.per_page = 10
        self.total_pages = ceil(len(self.records) / self.per_page) if self.records else 1
        self.prev_button = PrevPageButton(disabled=(self.current_page == 0))
        self.next_button = NextPageButton(disabled=(self.current_page >= self.total_pages - 1))
        self.add_item(self.prev_button)
        self.add_item(self.next_button)

    def update_buttons(self):
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page >= self.total_pages - 1)

    def get_page_embed(self):
        start_index = self.current_page * self.per_page
        page_records = self.records[start_index : start_index + self.per_page]
        lines = ["Role assignment history (most recent first).\n"]
        if not page_records: lines.append("No assignments found.")
        else:
            for i, record in enumerate(page_records, start=start_index + 1):
                uid_clean = record.get("uid", "Unknown").lstrip("'")
                username = record.get("username", "Unknown")
                time_str = record.get("time", "Unknown")
                lines.append(f"{i}. <@{uid_clean}> (`{username}`) - {time_str}")
        embed = discord.Embed(title="Role Assignment History", description="\n".join(lines), color=EMBED_COLOR)
        embed.set_footer(text=f"Page {self.current_page + 1}/{self.total_pages} (Total {len(self.records)})")
        return embed

    async def update_message(self, interaction: discord.Interaction):
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_page_embed(), view=self)

class PrevPageButton(discord.ui.Button):
    def __init__(self, disabled=False):
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.secondary, disabled=disabled, custom_id="history_prev_page")
    async def callback(self, interaction: discord.Interaction):
        view: HistoryPagerView = self.view
        if view.current_page > 0:
            view.current_page -= 1
            await view.update_message(interaction)
        else: await interaction.response.defer()

class NextPageButton(discord.ui.Button):
    def __init__(self, disabled=False):
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary, disabled=disabled, custom_id="history_next_page")
    async def callback(self, interaction: discord.Interaction):
        view: HistoryPagerView = self.view
        if view.current_page < view.total_pages - 1:
            view.current_page += 1
            await view.update_message(interaction)
        else: await interaction.response.defer()

# --- Bot イベント ---
@bot.event
async def on_ready():
    logger.info(f"Bot logged in as {bot.user.name} ({bot.user.id})")
    try: await data_manager.load_all_data()
    except Exception as e: logger.critical(f"CRITICAL: Error loading initial data: {e}", exc_info=True)
    bot.add_view(CombinedView())
    logger.info("Persistent CombinedView added.")
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} global slash commands.")
    except Exception as e: logger.error(f"Error syncing slash commands: {e}")

@bot.event
async def on_guild_join(guild: discord.Guild):
    logger.info(f"Joined new guild: {guild.name} (ID: {guild.id})")

@bot.event
async def on_guild_remove(guild: discord.Guild):
    logger.info(f"Removed from guild: {guild.name} (ID: {guild.id})")
    guild_id_str = str(guild.id)
    config_changed = False
    if guild_id_str in data_manager.guild_config:
        del data_manager.guild_config[guild_id_str]
        logger.info(f"Removed config for guild {guild_id_str} from memory.")
        config_changed = True
    if guild_id_str in data_manager.granted_history:
        del data_manager.granted_history[guild_id_str]
        logger.info(f"Removed history for guild {guild_id_str} from memory.")
    if config_changed:
        try: await data_manager.save_guild_config_sheet()
        except Exception as e: logger.error(f"Failed to save config after removing guild {guild_id_str}: {e}")

@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    cmd_name = interaction.command.name if interaction.command else "Unknown"
    logger.error(f"Error in slash command '{cmd_name}': {error}", exc_info=True)
    error_map = {
        app_commands.MissingPermissions: f"You lack required permissions: `{', '.join(error.missing_permissions)}`",
        app_commands.BotMissingPermissions: f"I lack required permissions: `{', '.join(error.missing_permissions)}`",
        app_commands.NoPrivateMessage: "This command cannot be used in DMs.",
        app_commands.CheckFailure: "You don't meet requirements for this command.",
        app_commands.CommandOnCooldown: f"Command on cooldown. Try again in {error.retry_after:.2f}s.",
        app_commands.TransformerError: f"Invalid input: {error}",
    }
    msg = error_map.get(type(error), "An unexpected error occurred. Please contact admin.")
    try:
        send_method = interaction.followup.send if interaction.response.is_done() else interaction.response.send_message
        await send_method(f"❌ {msg}", ephemeral=True)
    except discord.NotFound: logger.warning(f"Interaction for '{cmd_name}' not found for error reporting.")
    except discord.HTTPException as e: logger.error(f"Failed to send error message for '{cmd_name}': {e}")

# --- スラッシュコマンド ---

@bot.tree.command(name="setup", description="Post/Update the eligibility buttons and set the main role.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(channel="Channel for the buttons.", role="Main role for eligible users.")
async def setup_command(interaction: discord.Interaction, channel: discord.TextChannel, role: discord.Role):
    if not interaction.guild: return await interaction.response.send_message("Server only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    # --- 権限チェック ---
    if not interaction.app_permissions.manage_roles:
        return await interaction.response.send_message(
            "I need the **Manage Roles** permission.", ephemeral=True)

    if interaction.guild.me.top_role <= role:
        return await interaction.response.send_message(
            f"My highest role **{interaction.guild.me.top_role.name}** is not high enough to manage **{role.name}**. "
            "Please move my role above the target role.", ephemeral=True)

    perms_me = channel.permissions_for(interaction.guild.me)
    if not (perms_me.send_messages and perms_me.embed_links and
            perms_me.read_message_history and perms_me.manage_messages):
        return await interaction.response.send_message(
            f"I need **Send Messages / Embed Links / Read Message History / Manage Messages** in {channel.mention}.",
            ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    embed = discord.Embed(title="Check Eligibility & C Image", description="...", color=EMBED_COLOR) # descriptionは前回のコード参照
    view = CombinedView()
    message_id_to_save, message_link, op_type = None, "N/A", "created"
    # ... (既存メッセージ更新 or 新規作成ロジックは省略、前回のコード参照) ...
    # --- 既存メッセージの更新を試行 ---
    old_conf = data_manager.guild_config.get(guild_id_str)
    if old_conf and old_conf.get("message_id") and old_conf.get("channel_id") == str(channel.id):
        old_msg_id = old_conf["message_id"]
        if old_msg_id.isdigit():
            try:
                old_msg = await channel.fetch_message(int(old_msg_id))
                await old_msg.edit(embed=embed, view=view)
                message_id_to_save = old_msg.id
                message_link = old_msg.jump_url
                op_type = "updated"
            except discord.NotFound:
                logger.warning(f"Old setup message ({old_msg_id}) not found, creating new one.")
            except discord.Forbidden:
                return await interaction.followup.send(
                    f"❌ I cannot edit the existing setup message in {channel.mention}. "
                    "Delete it manually or give me permission.", ephemeral=True)
            except discord.HTTPException as e:
                return await interaction.followup.send(f"❌ HTTP error while editing message: {e}", ephemeral=True)

    # --- 新規メッセージ送信 ---
    if message_id_to_save is None:
        try:
            new_msg = await channel.send(embed=embed, view=view)
            message_id_to_save = new_msg.id
            message_link = new_msg.jump_url
        except discord.Forbidden:
            return await interaction.followup.send(
                f"❌ I cannot post messages in {channel.mention}.", ephemeral=True)
        except discord.HTTPException as e:
            return await interaction.followup.send(f"❌ HTTP error while sending message: {e}", ephemeral=True)

    # --- 設定保存 ---
    if message_id_to_save:
        cfg = data_manager.guild_config.get(guild_id_str, {})
        cfg.update({
            "server_name": interaction.guild.name,
            "channel_id": str(channel.id),
            "role_id": str(role.id),
            "message_id": str(message_id_to_save),
            # bonus_role_ids は変更しない
            "bonus_role_ids": cfg.get("bonus_role_ids", [])
        })
        data_manager.guild_config[guild_id_str] = cfg
        await data_manager.save_guild_config_sheet()
        await interaction.followup.send(
            f"✅ Setup **{op_type}**! Buttons → {channel.mention} (<{message_link}>)\n"
            f"Eligible users get {role.mention}.", ephemeral=True)
    else:
        await interaction.followup.send("❌ Setup failed: message could not be posted.", ephemeral=True)

    try:
        # ここでメッセージ送信/編集処理 (前回のコードから流用)
        # 例: new_msg = await channel.send(embed=embed, view=view); message_id_to_save = new_msg.id
        # 例: old_msg = await channel.fetch_message(...); await old_msg.edit(...); message_id_to_save = old_msg.id
        # (ここでは省略)
        # ダミー処理: 実際には上記のコメント部分に送信/編集コードが必要
        if True: # 仮に成功したとする
             message_id_to_save = 12345 # ダミーID
             message_link = f"https://discord.com/channels/{guild_id_str}/{channel.id}/{message_id_to_save}" # ダミーリンク
        else:
             raise discord.DiscordException("Failed to send/edit message") # 失敗した場合

        if message_id_to_save:
            current_config = data_manager.guild_config.get(guild_id_str, {})
            current_config.update({
                "server_name": interaction.guild.name,
                "channel_id": str(channel.id),
                "role_id": str(role.id),
                "message_id": str(message_id_to_save),
                # bonus_role_ids はこのコマンドでは変更しない
                "bonus_role_ids": current_config.get("bonus_role_ids", []) # 既存の値維持
            })
            data_manager.guild_config[guild_id_str] = current_config
            await data_manager.save_guild_config_sheet()
            await interaction.followup.send(f"Setup {op_type} in {channel.mention} (<{message_link}>). Role: {role.mention}.", ephemeral=True)
        else: # メッセージ送信/編集に失敗した場合 (本来は上のtry-except内で処理される)
             await interaction.followup.send("Setup failed: Could not post/update message.", ephemeral=True)

    except Exception as e:
         logger.error(f"Error during /setup execution: {e}", exc_info=True)
         await interaction.followup.send(f"An error occurred during setup: {e}", ephemeral=True)


@bot.tree.command(name="reloadlist", description="Reload eligible users and images from the sheet.")
@app_commands.default_permissions(administrator=True)
async def reloadlist_command(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        await data_manager.load_uid_list_from_sheet()
        uid_count, img_count = len(data_manager.valid_uids), len(data_manager.user_image_map)
        await interaction.followup.send(f"Reloaded from '{UID_LIST_SHEET}'. UIDs: {uid_count}, Images: {img_count}.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error during /reloadlist: {e}", exc_info=True)
        await interaction.followup.send(f"Error reloading list: {e}", ephemeral=True)

@bot.tree.command(name="history", description="Show role assignment history (paginated).")
@app_commands.default_permissions(administrator=True)
async def history_command(interaction: discord.Interaction):
    if not interaction.guild: return await interaction.response.send_message("Server only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    await interaction.response.defer(ephemeral=True)
    try:
        await data_manager.load_granted_history_sheet()
        records = data_manager.granted_history.get(guild_id_str, [])
        if not records: return await interaction.followup.send("No history found.", ephemeral=True)
        records_display = sorted(records, key=lambda x: x.get('time', ''), reverse=True)
        view = HistoryPagerView(records_display)
        await interaction.followup.send(embed=view.get_page_embed(), view=view, ephemeral=True)
    except Exception as e:
        logger.error(f"Error during /history: {e}", exc_info=True)
        await interaction.followup.send(f"Error fetching history: {e}", ephemeral=True)

@bot.tree.command(name="extractinfo", description="Show current setup info and recent history.")
@app_commands.default_permissions(administrator=True)
async def extractinfo_command(interaction: discord.Interaction):
    if not interaction.guild: return await interaction.response.send_message("Server only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    await interaction.response.defer(ephemeral=True)
    try:
        await data_manager.load_guild_config_sheet()
        await data_manager.load_granted_history_sheet()
        config = data_manager.guild_config.get(guild_id_str)
        history = data_manager.granted_history.get(guild_id_str, [])
        if not config: return await interaction.followup.send("No setup info found.", ephemeral=True)

        # --- 情報取得 ---
        ch_id = config.get("channel_id", "N/A")
        role_id = config.get("role_id", "N/A")
        msg_id = config.get("message_id", "N/A")
        bonus_role_ids = config.get("bonus_role_ids", []) # リスト取得

        # --- 表示用文字列生成 ---
        channel_mention = f"<#{ch_id}>" if ch_id.isdigit() else "Invalid/Not set"
        role_mention = f"<@&{role_id}>" if role_id.isdigit() else "Invalid/Not set"
        bonus_roles_mentions = [f"<@&{r_id}>" for r_id in bonus_role_ids if r_id.isdigit()]
        bonus_roles_str = ", ".join(bonus_roles_mentions) if bonus_roles_mentions else "None set"
        msg_link = f"https://discord.com/channels/{guild_id_str}/{ch_id}/{msg_id}" if ch_id.isdigit() and msg_id.isdigit() else "N/A"

        # --- レポート作成 ---
        report_lines = [f"**⚙️ Config for {interaction.guild.name}**"]
        report_lines.append(f"- Buttons Channel: {channel_mention} (`{ch_id}`)")
        report_lines.append(f"- Eligibility Role: {role_mention} (`{role_id}`)")
        report_lines.append(f"- Buttons Message: {msg_link} (`{msg_id}`)")
        report_lines.append(f"- Bonus Command Roles: {bonus_roles_str} (`{','.join(bonus_role_ids) or 'N/A'}`)") # IDも表示
        report_lines.append(f"\n**📜 Recent Role Grants (last 10)** (Total: {len(history)})")
        recent_history = sorted(history, key=lambda x: x.get('time', ''), reverse=True)[:10]
        if not recent_history: report_lines.append("- No recent assignments.")
        else:
            for i, record in enumerate(recent_history, start=1):
                uid = record.get('uid', '').lstrip("'"); user = record.get('username', 'N/A'); time = record.get('time', 'N/A')
                report_lines.append(f"{i}. <@{uid}> (`{user}`) - {time}")

        report = "\n".join(report_lines)
        await interaction.followup.send(report[:2000], ephemeral=True) # 長さ制限

    except Exception as e:
        logger.error(f"Error during /extractinfo: {e}", exc_info=True)
        await interaction.followup.send(f"Error extracting info: {e}", ephemeral=True)

@bot.tree.command(name="reset_history", description="⚠️ Reset role assignment history for this server.")
@app_commands.default_permissions(administrator=True)
async def reset_history_command(interaction: discord.Interaction):
    if not interaction.guild: return await interaction.response.send_message("Server only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    await interaction.response.defer(ephemeral=True)
    # ... (履歴リセット処理は省略、前回のコード参照) ...
    # ↓↓↓ 実行後 ↓↓↓
    try:
        # メモリクリア
        if guild_id_str in data_manager.granted_history: data_manager.granted_history[guild_id_str] = []
        # シートクリア (前回のコードの _clear_guild_history_from_sheet を呼び出す)
        ws = await data_manager._get_or_create_worksheet(GRANTED_HISTORY_SHEET)
        if ws:
            def _clear_sheet():
                try:
                    all_rows = ws.get_all_values()          # ヘッダー＋データ
                    if not all_rows:
                        return 0
                    header = all_rows[0]
                    gid_col = header.index("guild_id") if "guild_id" in header else 0
                    keep_rows = [header]
                    deleted = 0
                    for row in all_rows[1:]:
                        if len(row) <= gid_col or row[gid_col] != guild_id_str:
                            keep_rows.append(row)
                        else:
                            deleted += 1
                    ws.clear()
                    ws.update('A1', keep_rows, value_input_option='USER_ENTERED')
                    return deleted
                except Exception as e:
                    logger.error(f"Sheet clear error: {e}")
                    raise

                 return 10 # 仮に10件削除したとする
            deleted_count = await asyncio.to_thread(_clear_sheet) # ダミー実行
            await interaction.followup.send(f"History reset. {deleted_count} sheet entries removed.", ephemeral=True)
        else: await interaction.followup.send("History reset in memory, but sheet access failed.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error during /reset_history: {e}", exc_info=True)
        await interaction.followup.send(f"Error resetting history: {e}", ephemeral=True)


# --- Bonus Feature Commands (Modified) ---

@bot.tree.command(name="add_bonus_role", description="Add a role allowed to use the /bonus command.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(role="Role to add for /bonus command permission.")
async def add_bonus_role_command(interaction: discord.Interaction, role: discord.Role):
    """ボーナスロール追加コマンド"""
    if not interaction.guild: return await interaction.response.send_message("Server only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    role_id_str = str(role.id)

    # 現在の設定を取得（なければ初期化）
    conf = data_manager.guild_config.get(guild_id_str, {})
    bonus_roles = conf.get("bonus_role_ids", []) # 現在のリスト取得

    if role_id_str not in bonus_roles:
        bonus_roles.append(role_id_str)
        conf["bonus_role_ids"] = bonus_roles # 更新したリストをセット
        conf["server_name"] = interaction.guild.name # サーバー名も更新
        data_manager.guild_config[guild_id_str] = conf # DataManagerに反映
        try:
            await data_manager.save_guild_config_sheet()
            await interaction.response.send_message(f"✅ Role {role.mention} **added** to the list of roles allowed to use `/bonus`.", ephemeral=True)
            logger.info(f"Added bonus role {role.id} for guild {guild_id_str}.")
        except Exception as e:
            # 保存失敗時はメモリ上の変更もロールバックした方が良いかもしれないが、ここではエラー通知のみ
            logger.error(f"Failed to save config after adding bonus role {role.id} for guild {guild_id_str}: {e}", exc_info=True)
            await interaction.response.send_message(f"❌ Failed to save setting: {e}", ephemeral=True)
    else:
        await interaction.response.send_message(f"ℹ️ Role {role.mention} is already in the list.", ephemeral=True)


@bot.tree.command(name="remove_bonus_role", description="Remove a role from the /bonus command permission list.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(role="Role to remove from /bonus command permission.")
async def remove_bonus_role_command(interaction: discord.Interaction, role: discord.Role):
    """ボーナスロール削除コマンド"""
    if not interaction.guild: return await interaction.response.send_message("Server only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    role_id_str = str(role.id)

    conf = data_manager.guild_config.get(guild_id_str, {})
    bonus_roles = conf.get("bonus_role_ids", [])

    if role_id_str in bonus_roles:
        bonus_roles.remove(role_id_str)
        conf["bonus_role_ids"] = bonus_roles
        conf["server_name"] = interaction.guild.name
        data_manager.guild_config[guild_id_str] = conf
        try:
            await data_manager.save_guild_config_sheet()
            await interaction.response.send_message(f"✅ Role {role.mention} **removed** from the list of roles allowed to use `/bonus`.", ephemeral=True)
            logger.info(f"Removed bonus role {role.id} for guild {guild_id_str}.")
        except Exception as e:
            logger.error(f"Failed to save config after removing bonus role {role.id} for guild {guild_id_str}: {e}", exc_info=True)
            await interaction.response.send_message(f"❌ Failed to save setting: {e}", ephemeral=True)
    else:
        await interaction.response.send_message(f"ℹ️ Role {role.mention} was not found in the allowed list.", ephemeral=True)


@bot.tree.command(name="bonus", description="Post a temporary button for users to claim a bonus.")
@app_commands.describe(channel="Channel for the bonus button.", duration="Button lifetime (e.g., '15s', '10m', '1h'). Default: 15s.")
async def bonus_command(interaction: discord.Interaction, channel: discord.TextChannel, duration: str = "15s"):
    """ボーナスコマンド (権限チェック修正)"""
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Server member only.", ephemeral=True)
    guild_id_str = str(interaction.guild.id)

    # --- 権限チェック (修正) ---
    conf = data_manager.guild_config.get(guild_id_str, {})
    bonus_role_ids = conf.get("bonus_role_ids", []) # ロールIDリスト取得
    required_role_ids_int = {int(rid) for rid in bonus_role_ids if rid.isdigit()} # intのSetに変換

    is_admin = interaction.user.guild_permissions.administrator
    # ユーザーが持つロールIDのSet
    user_role_ids = {role.id for role in interaction.user.roles}
    # ユーザーが許可されたロールのいずれかを持っているか
    has_bonus_role = not required_role_ids_int.isdisjoint(user_role_ids) # 積集合が空でないか

    if not (is_admin or has_bonus_role):
        role_mentions = [f"<@&{rid}>" for rid in bonus_role_ids if rid.isdigit()]
        roles_str = ", ".join(role_mentions) if role_mentions else "the designated roles"
        return await interaction.response.send_message(f"❌ You need Admin perms or one of the following roles: {roles_str}.", ephemeral=True)

    # ... (ボットのチャンネル権限チェックは省略) ...
    # ... (期間パース処理は省略) ...
    # ... (ボタン付きメッセージ送信は省略) ...
    # ↓↓↓ メッセージ送信後 ↓↓↓
    try:
        seconds = parse_duration_to_seconds(duration)
        if seconds <= 0: raise ValueError("Duration must be positive")

        # ボットの権限チェック (メッセージ送信・管理)
        if not channel.permissions_for(interaction.guild.me).send_messages or \
           not channel.permissions_for(interaction.guild.me).manage_messages:
             raise discord.Forbidden("Missing Send/Manage Messages permission in channel.")

        view = BonusView(data_manager.append_bonus_log_to_sheet, guild_id_str, timeout=float(seconds))
        msg = await channel.send(f"⏳ **Bonus Claim!** Press within **{duration}**!", view=view)
        view.message = msg # BonusViewにメッセージ参照を渡す (タイムアウト編集用)

        await interaction.response.send_message(f"✅ Bonus button posted to {channel.mention} for {duration}.", ephemeral=True)
        logger.info(f"Bonus button posted in {guild_id_str} by {interaction.user} for {duration}.")

        # 自動削除タスク (変更なし)
        async def auto_delete():
            await asyncio.sleep(seconds)
            try: await msg.delete()
            except Exception as e_del: logger.warning(f"Could not auto-delete bonus msg {msg.id}: {e_del}")
        asyncio.create_task(auto_delete())

    except ValueError as e_val: # duration パースエラーなど
        await interaction.response.send_message(f"❌ Invalid input: {e_val}", ephemeral=True)
    except discord.Forbidden as e_forbid:
        await interaction.response.send_message(f"❌ Permission Error: {e_forbid}", ephemeral=True)
    except Exception as e_other:
        logger.error(f"Error during /bonus command: {e_other}", exc_info=True)
        await interaction.response.send_message(f"❌ An unexpected error occurred: {e_other}", ephemeral=True)


# --- Bot 実行 ---
if __name__ == "__main__":
    try:
        logger.info("Starting bot...")
        bot.run(TOKEN)
    except discord.LoginFailure: logger.critical("Login failed. Check BOT_TOKEN.")
    except Exception as e: logger.critical(f"Bot run error: {e}", exc_info=True)

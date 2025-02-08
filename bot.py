import os
import json
import logging
import asyncio
from datetime import datetime
from math import ceil

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 環境変数の読み込み (.envファイルから BOT_TOKEN, GOOGLE_CREDENTIALS 等を取得)
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
if TOKEN is None:
    logger.error("BOT_TOKEN not found in environment variables.")
    exit(1)

# Google Sheets 認証
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_credentials_str = os.getenv("GOOGLE_CREDENTIALS")
if google_credentials_str is None:
    logger.error("GOOGLE_CREDENTIALS not found in environment variables.")
    exit(1)
try:
    creds_dict = json.loads(google_credentials_str)
except Exception as e:
    logger.error("Failed to parse GOOGLE_CREDENTIALS: %s", e)
    exit(1)

CREDS = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
try:
    GSPREAD_CLIENT = gspread.authorize(CREDS)
except Exception as e:
    logger.error("Failed to authorize Google Sheets client: %s", e)
    exit(1)

# スプレッドシート名 (あなたの場合: "C's Point Management Sheet")
SPREADSHEET_NAME = "C's Point Management Sheet"
try:
    SPREADSHEET = GSPREAD_CLIENT.open(SPREADSHEET_NAME)
except Exception as e:
    logger.error("Failed to open spreadsheet '%s': %s", SPREADSHEET_NAME, e)
    exit(1)

def format_time(iso_str: str) -> str:
    """ISO8601文字列を 'YYYY-MM-DD HH:MM:SS' 形式に変換"""
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_str


class DataManager:
    def __init__(self):
        # UID→画像URLのマッピング用
        self.valid_uids = set()
        self.user_image_map = {}

        # 各ギルド設定や履歴
        self.guild_config = {}     # {guild_id: {"server_name", "channel_id", "role_id", "message_id"}}
        self.granted_history = {}  # {guild_id: [ {"uid", "username", "time"}, ... ]}

    async def get_sheet(self, sheet_name: str, rows="1000", cols="10"):
        """
        指定したワークシートを取得し、存在しなければ作成して返す
        """
        def _get_sheet():
            try:
                return SPREADSHEET.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                return SPREADSHEET.add_worksheet(title=sheet_name, rows=rows, cols=cols)
        return await asyncio.to_thread(_get_sheet)

    # ▼ CSVではなく "UID_List" シートからUIDと画像URLを読み込む ▼
    async def load_uid_list_from_sheet(self):
        ws = await self.get_sheet("UID_List")  # シート名: UID_List
        # 例: A列=UID, B列=IMGURL
        def _fetch_data():
            return ws.get_all_records()
        rows = await asyncio.to_thread(_fetch_data)

        new_uids = set()
        new_image_map = {}
        for row in rows:
            uid = str(row.get("UID", "")).strip()
            img_url = str(row.get("IMGURL", "")).strip()
            if uid:
                new_uids.add(uid)
                if img_url:
                    new_image_map[uid] = img_url

        self.valid_uids = new_uids
        self.user_image_map = new_image_map
        logger.info("Loaded %d UIDs from UID_List sheet.", len(self.valid_uids))

    async def load_guild_config_sheet(self):
        def _load():
            config = {}
            try:
                ws = SPREADSHEET.worksheet("guild_config")
                records = ws.get_all_records()
                for row in records:
                    guild_id = str(row.get("guild_id", "")).strip()
                    if guild_id:
                        config[guild_id] = {
                            "server_name": row.get("server_name", ""),
                            "channel_id": int(row.get("channel_id") or 0),
                            "role_id": int(row.get("role_id") or 0),
                            "message_id": int(row.get("message_id") or 0)
                        }
            except Exception as e:
                logger.error("Error loading guild_config: %s", e)
            return config
        self.guild_config = await asyncio.to_thread(_load)

    async def save_guild_config_sheet(self):
        ws = await self.get_sheet("guild_config", rows="100", cols="10")
        headers = ["guild_id", "server_name", "channel_id", "role_id", "message_id"]
        data = [headers]
        for gid, conf in self.guild_config.items():
            row = [
                gid,
                conf.get("server_name", ""),
                int(conf.get("channel_id", 0)),
                int(conf.get("role_id", 0)),
                int(conf.get("message_id", 0))
            ]
            data.append(row)

        def _update():
            ws.clear()
            ws.update("A1", data)
        await asyncio.to_thread(_update)
        logger.info("Guild config sheet saved.")

    async def load_granted_history_sheet(self):
        def _load():
            history = {}
            try:
                ws = SPREADSHEET.worksheet("granted_history")
                records = ws.get_all_records()
                for row in records:
                    guild_id = str(row.get("guild_id", "")).strip()
                    if guild_id:
                        history.setdefault(guild_id, []).append({
                            "uid": row.get("uid", ""),
                            "username": row.get("username", ""),
                            "time": row.get("time", "")
                        })
            except Exception as e:
                logger.error("Error loading granted_history: %s", e)
            return history
        self.granted_history = await asyncio.to_thread(_load)

    async def save_granted_history_sheet(self):
        ws = await self.get_sheet("granted_history", rows="1000", cols="10")
        headers = ["guild_id", "uid", "username", "time"]
        data = [headers]
        for gid, records in self.granted_history.items():
            for record in records:
                # uidは文字列として保存（先頭にシングルクォート）
                uid_str = f"'{record.get('uid', '')}"
                time_str = format_time(record.get("time", ""))
                row = [gid, uid_str, record.get("username", ""), time_str]
                data.append(row)

        def _update():
            ws.clear()
            ws.update("A1", data)
        await asyncio.to_thread(_update)
        logger.info("Granted history sheet saved.")

    async def append_log_to_sheet(self, guild_id: str, uid: str, username: str, timestamp: str):
        ws = await self.get_sheet("Log")
        uid_str = f"'{uid}"
        time_str = format_time(timestamp)

        def _append():
            try:
                ws.append_row([guild_id, uid_str, username, time_str])
            except Exception as e:
                logger.error("Failed to append log to sheet: %s", e)

        await asyncio.to_thread(_append)

    async def load_all_data(self):
        """
        Bot起動時などに全データを読み込む。
        """
        await self.load_uid_list_from_sheet()
        await self.load_guild_config_sheet()
        await self.load_granted_history_sheet()

    async def save_all_data(self):
        """
        Bot終了時などに全データを保存（必要に応じて）。
        """
        await self.save_guild_config_sheet()
        await self.save_granted_history_sheet()


# DataManagerインスタンス
data_manager = DataManager()

# Discord Bot の準備
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# --- ボタン UI ---
class CheckEligibilityButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            custom_id="check_eligibility_button",
            label="Check Eligibility",
            style=discord.ButtonStyle.primary
        )

    async def callback(self, interaction: discord.Interaction):
        guild_id_str = str(interaction.guild_id)
        user_id_str = str(interaction.user.id)

        # UIDチェック
        if user_id_str not in data_manager.valid_uids:
            if not interaction.response.is_done():
                return await interaction.response.send_message(
                    f"You are not eligible (UID: {user_id_str}).",
                    ephemeral=True
                )
            else:
                return await interaction.followup.send(
                    f"You are not eligible (UID: {user_id_str}).",
                    ephemeral=True
                )

        # ギルド設定取得
        info = data_manager.guild_config.get(guild_id_str)
        if not info:
            if not interaction.response.is_done():
                return await interaction.response.send_message(
                    "No setup found. Please run /setup.",
                    ephemeral=True
                )
            else:
                return await interaction.followup.send(
                    "No setup found. Please run /setup.",
                    ephemeral=True
                )

        role = interaction.guild.get_role(info["role_id"])
        if not role:
            if not interaction.response.is_done():
                return await interaction.response.send_message(
                    "Configured role not found.",
                    ephemeral=True
                )
            else:
                return await interaction.followup.send(
                    "Configured role not found.",
                    ephemeral=True
                )

        # 既にロール所持
        if role in interaction.user.roles:
            if not interaction.response.is_done():
                return await interaction.response.send_message(
                    f"You already have {role.mention}.",
                    ephemeral=True
                )
            else:
                return await interaction.followup.send(
                    f"You already have {role.mention}.",
                    ephemeral=True
                )

        # ロール付与
        try:
            await interaction.user.add_roles(role)
        except discord.Forbidden:
            if not interaction.response.is_done():
                return await interaction.response.send_message(
                    "Failed to grant role. Check bot permissions.",
                    ephemeral=True
                )
            else:
                return await interaction.followup.send(
                    "Failed to grant role. Check bot permissions.",
                    ephemeral=True
                )

        # テキストはエフェメラル (本人のみ)
        response_text = f"You are **eligible** (UID: {user_id_str}). Role {role.mention} has been granted!"
        await interaction.response.send_message(response_text, ephemeral=True)

        # 画像URLを取得してパブリックにEmbed送信
        image_url = data_manager.user_image_map.get(user_id_str)
        if image_url:
            embed = discord.Embed(
                title="Your Special Image",
                description="Here's your image!",
                color=discord.Color.green()
            )
            embed.set_image(url=image_url)
            # ephemeral=False => 全員が見えるパブリックメッセージ
            await interaction.followup.send(embed=embed, ephemeral=False)

        # ログ書き込み
        async def background_tasks():
            timestamp = datetime.utcnow().isoformat()
            log_entry = {
                "uid": user_id_str,
                "username": str(interaction.user),
                "time": timestamp
            }
            data_manager.granted_history.setdefault(guild_id_str, []).append(log_entry)
            try:
                await data_manager.save_granted_history_sheet()
                await data_manager.append_log_to_sheet(guild_id_str, user_id_str, str(interaction.user), timestamp)
            except Exception as e:
                logger.error("Background tasks error: %s", e)

        asyncio.create_task(background_tasks())


class CheckEligibilityView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(CheckEligibilityButton())


# --- 履歴表示用のページングUI ---
class HistoryPagerView(discord.ui.View):
    def __init__(self, records):
        super().__init__(timeout=None)
        self.records = records
        self.page = 0
        self.per_page = 10
        self.prev_button = PrevButton()
        self.next_button = NextButton()
        self.add_item(self.prev_button)
        self.add_item(self.next_button)
        self.update_buttons()

    def max_page(self):
        return ceil(len(self.records) / self.per_page) if self.records else 1

    def update_buttons(self):
        if len(self.records) < self.per_page:
            self.prev_button.disabled = True
            self.next_button.disabled = True
        else:
            self.prev_button.disabled = (self.page == 0)
            self.next_button.disabled = (self.page >= self.max_page() - 1)

    def get_page_embed(self):
        start = self.page * self.per_page
        end = start + self.per_page
        chunk = self.records[start:end]
        lines = []
        for i, record in enumerate(chunk, start=1):
            uid_raw = record["uid"]
            uid_clean = uid_raw.lstrip("'")
            lines.append(f"{start + i}. <@{uid_clean}>")
        description = (
            "This list shows the server's role assignment history.\n"
            "Below are the recent assignments:\n\n" +
            "\n".join(lines) if lines else "No assignments on this page."
        )
        embed = discord.Embed(
            title="Role Assignment History",
            description=description,
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Page {self.page+1}/{self.max_page()} (Total {len(self.records)})")
        return embed


class PrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: HistoryPagerView = self.view  # type: ignore
        if view.page > 0:
            view.page -= 1
        view.update_buttons()
        await interaction.response.edit_message(embed=view.get_page_embed(), view=view)


class NextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: HistoryPagerView = self.view  # type: ignore
        if view.page < view.max_page() - 1:
            view.page += 1
        view.update_buttons()
        await interaction.response.edit_message(embed=view.get_page_embed(), view=view)


# --- Botイベント ---
@bot.event
async def on_ready():
    logger.info("Bot logged in as %s", bot.user)
    await data_manager.load_all_data()
    logger.info("UID loaded: %d", len(data_manager.valid_uids))
    try:
        await bot.tree.sync()
        logger.info("Slash commands synced.")
    except Exception as e:
        logger.error("Error syncing slash commands: %s", e)

    # 永続View登録
    bot.add_view(CheckEligibilityView())


@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    logger.error("App command error: %s", error)
    if interaction.response.is_done():
        await interaction.followup.send("An error occurred. Please try again or contact an admin.", ephemeral=True)
    else:
        await interaction.response.send_message("An error occurred. Please try again or contact an admin.", ephemeral=True)


# --- /setup コマンド ---
@bot.tree.command(name="setup", description="Set up or update the eligibility button and assigned role.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    channel="Channel for the check button",
    role="Role to grant if eligible"
)
async def setup_command(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    role: discord.Role
):
    guild_id_str = str(interaction.guild_id)
    old_info = data_manager.guild_config.get(guild_id_str, {})
    old_msg_id = old_info.get("message_id")
    old_ch_id = old_info.get("channel_id", 0)

    embed_text = "Click the button below to see if you're on the list."
    if old_msg_id and old_ch_id:
        old_ch = interaction.guild.get_channel(old_ch_id)
        if old_ch:
            try:
                old_msg = await old_ch.fetch_message(old_msg_id)
                embed = discord.Embed(
                    title="Check Eligibility",
                    description=embed_text,
                    color=discord.Color.blue()
                )
                view = CheckEligibilityView()
                await old_msg.edit(embed=embed, view=view)
                data_manager.guild_config[guild_id_str] = {
                    "server_name": interaction.guild.name,
                    "channel_id": channel.id,
                    "role_id": role.id,
                    "message_id": old_msg.id
                }
                await data_manager.save_guild_config_sheet()
                return await interaction.response.send_message(
                    f"Button message updated in {old_ch.mention}. Role set to {role.mention}.",
                    ephemeral=True
                )
            except Exception as e:
                logger.error("Error editing old message: %s", e)

    # 新規メッセージ
    embed = discord.Embed(
        title="Check Eligibility",
        description=embed_text,
        color=discord.Color.blue()
    )
    view = CheckEligibilityView()
    new_msg = await channel.send(embed=embed, view=view)
    data_manager.guild_config[guild_id_str] = {
        "server_name": interaction.guild.name,
        "channel_id": channel.id,
        "role_id": role.id,
        "message_id": new_msg.id
    }
    await data_manager.save_guild_config_sheet()
    await interaction.response.send_message(
        f"Setup complete in {channel.mention} with role {role.mention}.",
        ephemeral=True
    )


# --- /reloadlist コマンド ---
@bot.tree.command(name="reloadlist", description="Reload the user list from UID_List sheet.")
@app_commands.default_permissions(administrator=True)
async def reloadlist_command(interaction: discord.Interaction):
    await data_manager.load_uid_list_from_sheet()
    count = len(data_manager.valid_uids)
    await interaction.response.send_message(
        f"Reloaded user list from sheet. {count} UIDs found.",
        ephemeral=True
    )


# --- /history コマンド ---
@bot.tree.command(name="history", description="Show the role-grant history in pages of 10.")
@app_commands.default_permissions(administrator=True)
async def history_command(interaction: discord.Interaction):
    await data_manager.load_granted_history_sheet()
    guild_id_str = str(interaction.guild_id)
    records = data_manager.granted_history.get(guild_id_str, [])
    if not records:
        return await interaction.response.send_message("No history for this server.", ephemeral=True)
    view = HistoryPagerView(records)
    embed = view.get_page_embed()
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


# --- /extractinfo コマンド ---
@bot.tree.command(name="extractinfo", description="Extract server info and recent role assignments.")
@app_commands.default_permissions(administrator=True)
async def extractinfo_command(interaction: discord.Interaction):
    await data_manager.load_granted_history_sheet()
    guild_id_str = str(interaction.guild_id)
    info = data_manager.guild_config.get(guild_id_str)
    if not info:
        return await interaction.response.send_message("No setup info found for this server.", ephemeral=True)

    ch_id = info["channel_id"]
    role_id = info["role_id"]
    msg_id = info["message_id"]

    lines = [
        "**Server Info**",
        f"- Server Name: {info.get('server_name', '')}",
        f"- Channel ID: {ch_id}",
        f"- Role ID: {role_id}",
        f"- Setup Message ID: {msg_id}",
        "",
        f"**Recent Role Grants** (total {len(data_manager.granted_history.get(guild_id_str, []))})"
    ]
    recs = data_manager.granted_history.get(guild_id_str, [])
    for i, record in enumerate(recs[-10:], start=1):
        uid_clean = record['uid'].lstrip("'")
        lines.append(f"{i}. <@{uid_clean}>")
    report = "\n".join(lines)
    await interaction.response.send_message(report, ephemeral=True)


# --- /reset_history コマンド ---
@bot.tree.command(name="reset_history", description="Reset the role-grant history (admin only).")
@app_commands.default_permissions(administrator=True)
async def reset_history_command(interaction: discord.Interaction):
    guild_id_str = str(interaction.guild_id)
    data_manager.granted_history[guild_id_str] = []
    await data_manager.save_granted_history_sheet()
    await interaction.response.send_message("History has been reset for this server.", ephemeral=True)


if __name__ == "__main__":
    bot.run(TOKEN)

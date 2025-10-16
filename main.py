import discord
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import datetime
import aiohttp
import asyncpg
from aiohttp import web
from discord import app_commands
import asyncio
from urllib.parse import urlparse, urlencode

# === Configuration ===
load_dotenv()

def getenv_int(name: str, default: int | None = None) -> int | None:
    val = os.getenv(name)
    try:
        return int(val) if val else default
    except ValueError:
        return default

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Channels / roles (safe parsing)
ANNOUNCEMENT_CHANNEL_ID = getenv_int("ANNOUNCEMENT_CHANNEL_ID")
LOG_CHANNEL_ID = getenv_int("LOG_CHANNEL_ID")
COMMAND_LOG_CHANNEL_ID = getenv_int("COMMAND_LOG_CHANNEL_ID")
ACTIVITY_LOG_CHANNEL_ID = getenv_int("ACTIVITY_LOG_CHANNEL_ID")
ANNOUNCEMENT_ROLE_ID = getenv_int("ANNOUNCEMENT_ROLE_ID", 0)  # optional; 0 means disabled
MANAGEMENT_ROLE_ID = getenv_int("MANAGEMENT_ROLE_ID")

DEPARTMENT_ROLE_ID = getenv_int("DEPARTMENT_ROLE_ID", 0)  # E&L dept role
INTERN_ROLE_ID = getenv_int("INTERN_ROLE_ID", 0)          # “E&L Intern” role (triggers seminar window)

# DB / API
DATABASE_URL = os.getenv("DATABASE_URL")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")  # for /roblox webhook auth

def _normalize_base(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u.rstrip("/")
    return ("https://" + u).rstrip("/")

ROBLOX_SERVICE_BASE = _normalize_base(os.getenv("ROBLOX_SERVICE_BASE") or None)
ROBLOX_REMOVE_URL = os.getenv("ROBLOX_REMOVE_URL") or None
if ROBLOX_REMOVE_URL and not ROBLOX_REMOVE_URL.startswith("http"):
    ROBLOX_REMOVE_URL = "https://" + ROBLOX_REMOVE_URL
ROBLOX_REMOVE_SECRET = os.getenv("ROBLOX_REMOVE_SECRET") or None
ROBLOX_GROUP_ID = os.getenv("ROBLOX_GROUP_ID")  # used to filter ranks if your service supports it

# Rank manager role (can run /rank)
RANK_MANAGER_ROLE_ID = getenv_int("RANK_MANAGER_ROLE_ID", MANAGEMENT_ROLE_ID or 0)

# Weekly configs
WEEKLY_REQUIREMENT = int(os.getenv("WEEKLY_REQUIREMENT", "3"))
WEEKLY_TIME_REQUIREMENT = int(os.getenv("WEEKLY_TIME_REQUIREMENT", "45"))  # minutes

# === Bot Setup ===
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

# E&L task catalog (rename as you like)
TASK_TYPES = [
    "Generator Stress Test",
    "Generator Repair",
    "Window Repair",
    "Alarm Test",
    "Sewer Repair",
]

TASK_PLURALS = {
    "Generator Stress Test": "Generator Stress Tests",
    "Generator Repair": "Generator Repairs",
    "Window Repair": "Window Repairs",
    "Alarm Test": "Alarm Tests",
    "Sewer Repair": "Sewer Repairs",
}

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)

def human_remaining(delta: datetime.timedelta) -> str:
    if delta.total_seconds() <= 0:
        return "0d"
    days = delta.days
    hours = (delta.seconds // 3600)
    mins = (delta.seconds % 3600) // 60
    parts = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if mins and not days: parts.append(f"{mins}m")
    return " ".join(parts) if parts else "under 1m"

def week_key(dt: datetime.datetime | None = None) -> str:
    d = dt or utcnow()
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

# === Excuse helpers (NEW) ===
def _valid_week_key(s: str) -> bool:
    # Accepts YYYY-WWW (ISO week, zero-padded)
    try:
        if len(s) != 8 or s[4] != '-' or s[5] != 'W':
            return False
        year = int(s[:4])
        week = int(s[6:8])
        return 1 <= week <= 53 and 1900 <= year <= 3000
    except:
        return False

def _resolve_week_key(arg: str | None) -> str:
    """
    Convert a user-supplied week argument into a proper ISO week key.
    Accepts: None/'', 'current', 'prev', 'previous', 'last', 'next', or a literal 'YYYY-Www'.
    """
    if not arg:
        return week_key()
    val = arg.strip().lower()
    if val in ("current", "this", "now"):
        return week_key()
    base = utcnow()
    if val in ("prev", "previous", "last"):
        return week_key(base - datetime.timedelta(weeks=1))
    if val in ("next",):
        return week_key(base + datetime.timedelta(weeks=1))
    if _valid_week_key(arg):
        return arg
    # Fallback to current if format is wrong
    return week_key()

class EL_BOT(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)
        self.db_pool = None

    async def setup_hook(self):
        # DB pool
        try:
            self.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            print("Successfully connected to the database.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            return

        # Schema (shared with MD style)
        async with self.db_pool.acquire() as connection:
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS weekly_tasks (
                    member_id BIGINT PRIMARY KEY,
                    tasks_completed INT DEFAULT 0
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS task_logs (
                    log_id SERIAL PRIMARY KEY,
                    member_id BIGINT,
                    task TEXT,
                    task_type TEXT,
                    proof_url TEXT,
                    comments TEXT,
                    timestamp TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS weekly_task_logs (
                    log_id SERIAL PRIMARY KEY,
                    member_id BIGINT,
                    task TEXT,
                    task_type TEXT,
                    proof_url TEXT,
                    comments TEXT,
                    timestamp TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_verification (
                    discord_id BIGINT PRIMARY KEY,
                    roblox_id BIGINT UNIQUE
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_time (
                    member_id BIGINT PRIMARY KEY,
                    time_spent INT DEFAULT 0
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS roblox_sessions (
                    roblox_id BIGINT PRIMARY KEY,
                    start_time TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS internship_seminars (
                    discord_id BIGINT PRIMARY KEY,
                    assigned_at TIMESTAMPTZ,
                    deadline TIMESTAMPTZ,
                    passed BOOLEAN DEFAULT FALSE,
                    passed_at TIMESTAMPTZ,
                    warned_5d BOOLEAN DEFAULT FALSE,
                    expired_handled BOOLEAN DEFAULT FALSE
                );
            ''')
            await connection.execute("CREATE TABLE IF NOT EXISTS member_ranks (discord_id BIGINT PRIMARY KEY, rank TEXT, set_by BIGINT, set_at TIMESTAMPTZ);")
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS strikes (
                    strike_id SERIAL PRIMARY KEY,
                    member_id BIGINT NOT NULL,
                    reason TEXT,
                    issued_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    set_by BIGINT,
                    auto BOOLEAN DEFAULT FALSE
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS activity_excuses (
                    week_key TEXT PRIMARY KEY,
                    reason TEXT,
                    set_by BIGINT,
                    set_at TIMESTAMPTZ
                );
            ''')
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS member_activity_excuses (
                    member_id BIGINT PRIMARY KEY,
                    reason TEXT,
                    expires_at TIMESTAMPTZ,
                    set_by BIGINT,
                    set_at TIMESTAMPTZ
                );
            ''')

        # Sync slash commands
        try:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} command(s)")
        except Exception as e:
            print(f"Failed to sync commands: {e}")

        # Web server for Roblox integration (time tracking)
        app = web.Application()
        app.router.add_post('/roblox', self.roblox_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        print("Web server for Roblox integration is running on :8080.")

    # --- Roblox webhook: now with activity embeds ---
    async def roblox_handler(self, request):
        if request.headers.get("X-Secret-Key") != API_SECRET_KEY:
            return web.Response(status=401)
        data = await request.json()
        roblox_id = data.get("robloxId")
        status = data.get("status")

        async with self.db_pool.acquire() as connection:
            discord_id = await connection.fetchval(
                "SELECT discord_id FROM roblox_verification WHERE roblox_id = $1", roblox_id
            )

        if discord_id:
            if status == "joined":
                async with self.db_pool.acquire() as connection:
                    await connection.execute(
                        "INSERT INTO roblox_sessions (roblox_id, start_time) VALUES ($1, $2) "
                        "ON CONFLICT (roblox_id) DO UPDATE SET start_time = $2",
                        roblox_id, utcnow()
                    )
                # Activity embed
                member = find_member(int(discord_id))
                name = member.display_name if member else f"User {discord_id}"
                await send_activity_embed(
                    "🟢 Joined Site",
                    f"**{name}** started a session.",
                    discord.Color.green()
                )

            elif status == "left":
                session_start = None
                async with self.db_pool.acquire() as connection:
                    session_start = await connection.fetchval(
                        "SELECT start_time FROM roblox_sessions WHERE roblox_id = $1", roblox_id
                    )
                    if session_start:
                        await connection.execute("DELETE FROM roblox_sessions WHERE roblox_id = $1", roblox_id)
                        duration = (utcnow() - session_start).total_seconds()
                        await connection.execute(
                            "INSERT INTO roblox_time (member_id, time_spent) VALUES ($1, $2) "
                            "ON CONFLICT (member_id) DO UPDATE SET time_spent = roblox_time.time_spent + $2",
                            discord_id, int(duration)
                        )
                # Activity embed with minutes
                mins = int((utcnow() - session_start).total_seconds() // 60) if session_start else 0
                # Also fetch their weekly total to display
                weekly_minutes = 0
                async with self.db_pool.acquire() as connection:
                    total_seconds = await connection.fetchval(
                        "SELECT time_spent FROM roblox_time WHERE member_id=$1", discord_id
                    ) or 0
                weekly_minutes = total_seconds // 60
                member = find_member(int(discord_id))
                name = member.display_name if member else f"User {discord_id}"
                await send_activity_embed(
                    "🔴 Left Site",
                    f"**{name}** ended their session. Time this session: **{mins} min**.\nThis week: **{weekly_minutes}/{WEEKLY_TIME_REQUIREMENT} min**",
                    discord.Color.red()
                )

        return web.Response(status=200)

bot = EL_BOT()

# === Helpers ===
def smart_chunk(text, size=4000):
    chunks = []
    while len(text) > size:
        split_index = text.rfind('\n', 0, size)
        if split_index == -1:
            split_index = text.rfind(' ', 0, size)
        if split_index == -1:
            split_index = size
        chunks.append(text[:split_index])
        text = text[split_index:].lstrip()
    chunks.append(text)
    return chunks

async def send_long_embed(target, title, description, color, footer_text, author_name=None, author_icon_url=None, image_url=None):
    chunks = smart_chunk(description)
    embed = discord.Embed(title=title, description=chunks[0], color=color, timestamp=utcnow())
    if footer_text: embed.set_footer(text=footer_text)
    if author_name: embed.set_author(name=author_name, icon_url=author_icon_url)
    if image_url: embed.set_image(url=image_url)
    await target.send(embed=embed)
    for i, chunk in enumerate(chunks[1:], start=2):
        follow_up = discord.Embed(description=chunk, color=color)
        follow_up.set_footer(text=f"Part {i}/{len(chunks)}")
        await target.send(embed=follow_up)

async def log_action(title: str, description: str):
    if not COMMAND_LOG_CHANNEL_ID:
        return
    ch = bot.get_channel(COMMAND_LOG_CHANNEL_ID)
    if not ch:
        return
    embed = discord.Embed(title=title, description=description, color=discord.Color.dark_gray(), timestamp=utcnow())
    await ch.send(embed=embed)

def channel_or_fallback():
    ch = bot.get_channel(ACTIVITY_LOG_CHANNEL_ID) if ACTIVITY_LOG_CHANNEL_ID else None
    if not ch:
        ch = bot.get_channel(COMMAND_LOG_CHANNEL_ID) if COMMAND_LOG_CHANNEL_ID else None
    return ch

async def send_activity_embed(title: str, desc: str, color: discord.Color):
    ch = channel_or_fallback()
    if not ch:
        return
    embed = discord.Embed(title=title, description=desc, color=color, timestamp=utcnow())
    await ch.send(embed=embed)

def find_member(discord_id: int) -> discord.Member | None:
    for g in bot.guilds:
        m = g.get_member(discord_id)
        if m:
            return m
    return None

# Internship Seminar helpers
async def ensure_intern_record(member: discord.Member):
    """Ensure the intern has a 2-week seminar window if they hold the INTERN_ROLE_ID."""
    if not INTERN_ROLE_ID:
        return
    if not any(r.id == INTERN_ROLE_ID for r in member.roles):
        return
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT discord_id FROM internship_seminars WHERE discord_id=$1", member.id)
        if row:
            return
        assigned = utcnow()
        deadline = assigned + datetime.timedelta(days=14)
        await conn.execute(
            "INSERT INTO internship_seminars (discord_id, assigned_at, deadline, passed, passed_at, warned_5d, expired_handled) "
            "VALUES ($1, $2, $3, FALSE, FALSE, FALSE)",
            member.id, assigned, deadline
        )

# Retry helper for HTTP
async def _retry(coro_factory, attempts=3, delay=0.8):
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_factory()
        except Exception as e:
            last_exc = e
            if i < attempts - 1:
                await asyncio.sleep(delay)
    raise last_exc

# Roblox svc helpers
async def try_remove_from_roblox(discord_id: int) -> bool:
    if not ROBLOX_REMOVE_URL or not ROBLOX_REMOVE_SECRET:
        return False
    try:
        async with bot.db_pool.acquire() as conn:
            roblox_id = await conn.fetchval("SELECT roblox_id FROM roblox_verification WHERE discord_id = $1", discord_id)
        if not roblox_id:
            print(f"try_remove_from_roblox: no roblox_id for {discord_id}")
            return False
        async def do_post():
            async with aiohttp.ClientSession() as session:
                headers = {"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"}
                payload = {"robloxId": int(roblox_id)}
                async with session.post(ROBLOX_REMOVE_URL, headers=headers, json=payload, timeout=20) as resp:
                    if not (200 <= resp.status < 300):
                        text = await resp.text()
                        raise RuntimeError(f"Roblox removal failed {resp.status}: {text}")
                    return True
        return await _retry(do_post)
    except Exception as e:
        print(f"Roblox removal call failed: {e}")
        return False

async def fetch_group_ranks():
    """Fetch Roblox group ranks; passes ?groupId= if provided."""
    if not ROBLOX_SERVICE_BASE or not ROBLOX_REMOVE_SECRET:
        return []
    base = ROBLOX_SERVICE_BASE.rstrip('/') + '/ranks'
    q = {}
    if ROBLOX_GROUP_ID:
        q["groupId"] = ROBLOX_GROUP_ID  # if your service reads it
    url = base + (("?" + urlencode(q)) if q else "")
    try:
        async def do_get():
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"X-Secret-Key": ROBLOX_REMOVE_SECRET}, timeout=20) as resp:
                    if not (200 <= resp.status < 300):
                        text = await resp.text()
                        raise RuntimeError(f"/ranks HTTP {resp.status}: {text}")
                    data = await resp.json()
                    return data.get('roles', [])
        return await _retry(do_get)
    except Exception as e:
        print(f"fetch_group_ranks error: {e}")
        return []

async def set_group_rank(roblox_id: int, role_id: int = None, rank_number: int = None) -> bool:
    if not ROBLOX_SERVICE_BASE or not ROBLOX_REMOVE_SECRET:
        return False
    url = ROBLOX_SERVICE_BASE.rstrip('/') + '/set-rank'
    body = {"robloxId": int(roblox_id)}
    if role_id is not None:
        body["roleId"] = int(role_id)
    if rank_number is not None:
        body["rankNumber"] = int(rank_number)
    if ROBLOX_GROUP_ID:
        try:
            body["groupId"] = int(ROBLOX_GROUP_ID)
        except:
            pass
    try:
        async def do_post():
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body, headers={"X-Secret-Key": ROBLOX_REMOVE_SECRET, "Content-Type": "application/json"}, timeout=20) as resp:
                    if not (200 <= resp.status < 300):
                        text = await resp.text()
                        raise RuntimeError(f"/set-rank HTTP {resp.status}: {text}")
                    return True
        return await _retry(do_post)
    except Exception as e:
        print(f"set_group_rank error: {e}")
        return False

# === Events ===
@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    check_weekly_tasks.start()
    internship_seminar_loop.start()

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    # If member gains INTERN_ROLE_ID, open their 2-week “Internship Seminar” window.
    if INTERN_ROLE_ID and (INTERN_ROLE_ID not in {r.id for r in before.roles}) and (INTERN_ROLE_ID in {r.id for r in after.roles}):
        await ensure_intern_record(after)
        await log_action("Internship Seminar Assigned",
                         f"Member: {after.mention} • Deadline: {(utcnow()+datetime.timedelta(days=14)).strftime('%Y-%m-%d %H:%M UTC')}")

# Global slash error
@bot.tree.error
async def global_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        await log_action("Slash Command Error", f"Command: **/{getattr(interaction.command, 'name', 'unknown')}**\nError: `{error}`")
    finally:
        if not interaction.response.is_done():
            try:
                await interaction.response.send_message("Sorry, something went wrong running that command.", ephemeral=True)
            except:
                pass

# === Slash Commands ===

# /verify
@bot.tree.command(name="verify", description="Link your Roblox account to the bot.")
async def verify(interaction: discord.Interaction, roblox_username: str):
    payload = {"usernames": [roblox_username], "excludeBannedUsers": True}
    async with aiohttp.ClientSession() as session:
        async with session.post("https://users.roblox.com/v1/usernames/users", json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data["data"]:
                    user_data = data["data"][0]
                    roblox_id = user_data["id"]
                    roblox_name = user_data["name"]
                    async with bot.db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO roblox_verification (discord_id, roblox_id) VALUES ($1, $2) "
                            "ON CONFLICT (discord_id) DO UPDATE SET roblox_id = EXCLUDED.roblox_id",
                            interaction.user.id, roblox_id
                        )
                    await log_action("Verification Linked", f"User: {interaction.user.mention}\nRoblox: **{roblox_name}** (`{roblox_id}`)")
                    await interaction.response.send_message(f"Successfully verified as {roblox_name}!", ephemeral=True)
                else:
                    await interaction.response.send_message("Could not find that Roblox user.", ephemeral=True)
            else:
                await interaction.response.send_message("There was an error looking up the Roblox user.", ephemeral=True)

# /announce
@bot.tree.command(name="announce", description="Open a form to send an announcement.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID if (ANNOUNCEMENT_ROLE_ID or 0) == 0 else ANNOUNCEMENT_ROLE_ID)
@app_commands.choices(color=[
    app_commands.Choice(name="Blue", value="blue"),
    app_commands.Choice(name="Green", value="green"),
    app_commands.Choice(name="Red", value="red"),
    app_commands.Choice(name="Yellow", value="yellow"),
    app_commands.Choice(name="Purple", value="purple"),
    app_commands.Choice(name="Orange", value="orange"),
    app_commands.Choice(name="Gold", value="gold"),
])
async def announce(interaction: discord.Interaction, color: str = "blue"):
    class AnnouncementForm(discord.ui.Modal, title='Send Announcement'):
        def __init__(self, color_obj: discord.Color):
            super().__init__()
            self.color_obj = color_obj
        ann_title = discord.ui.TextInput(label='Title', placeholder='Announcement title', style=discord.TextStyle.short, required=True, max_length=200)
        ann_message = discord.ui.TextInput(label='Message', placeholder='Write your announcement here…', style=discord.TextStyle.paragraph, required=True, max_length=4000)
        async def on_submit(self, interaction2: discord.Interaction):
            ch = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID) if ANNOUNCEMENT_CHANNEL_ID else None
            if not ch:
                await interaction2.response.send_message("Announcement channel not found or not set.", ephemeral=True)
                return
            await send_long_embed(
                target=ch,
                title=f"📢 {self.ann_title.value}",
                description=self.ann_message.value,
                color=self.color_obj,
                footer_text=f"Announcement by {interaction2.user.display_name}"
            )
            await log_action("Announcement Sent", f"User: {interaction2.user.mention}\nTitle: **{self.ann_title.value}**")
            await interaction2.response.send_message("Announcement sent successfully!", ephemeral=True)

    color_obj = getattr(discord.Color, color, discord.Color.blue)()
    await interaction.response.send_modal(AnnouncementForm(color_obj=color_obj))

# /log — task log with proof
@bot.tree.command(name="log", description="Log a completed E&L task with proof.")
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def log_task(interaction: discord.Interaction, task_type: str, proof: discord.Attachment):
    class LogTaskForm(discord.ui.Modal, title='Add Comments (optional)'):
        def __init__(self, proof: discord.Attachment, task_type: str):
            super().__init__()
            self.proof = proof
            self.task_type = task_type
        comments = discord.ui.TextInput(label='Comments', placeholder='Any additional comments?', style=discord.TextStyle.paragraph, required=False, max_length=1000)
        async def on_submit(self, interaction2: discord.Interaction):
            log_channel = bot.get_channel(LOG_CHANNEL_ID) if LOG_CHANNEL_ID else None
            if not log_channel:
                await interaction2.response.send_message("Log channel not found or not set.", ephemeral=True)
                return
            member_id = interaction2.user.id
            comments_str = self.comments.value or "No comments"
            async with bot.db_pool.acquire() as conn:
                # permanent
                await conn.execute(
                    "INSERT INTO task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    member_id, self.task_type, self.task_type, self.proof.url, comments_str, utcnow()
                )
                # weekly
                await conn.execute(
                    "INSERT INTO weekly_task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    member_id, self.task_type, self.task_type, self.proof.url, comments_str, utcnow()
                )
                # counter
                await conn.execute(
                    "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, 1) "
                    "ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + 1",
                    member_id
                )
                tasks_completed = await conn.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)

            full_description = f"**Task Type:** {self.task_type}\n\n**Comments:**\n{comments_str}"
            await send_long_embed(
                target=log_channel,
                title="✅ Task Logged",
                description=full_description,
                color=discord.Color.green(),
                footer_text=f"Member ID: {member_id}",
                author_name=interaction2.user.display_name,
                author_icon_url=interaction2.user.avatar.url if interaction2.user.avatar else None,
                image_url=self.proof.url
            )
            await log_action("Task Logged", f"User: {interaction2.user.mention}\nType: **{self.task_type}**")
            await interaction2.response.send_message(
                f"Your task has been logged! You have completed {tasks_completed} task(s) this week.",
                ephemeral=True
            )

    await interaction.response.send_modal(LogTaskForm(proof=proof, task_type=task_type))

# /mytasks
@bot.tree.command(name="mytasks", description="Check your weekly tasks and time.")
async def mytasks(interaction: discord.Interaction):
    member_id = interaction.user.id
    async with bot.db_pool.acquire() as conn:
        tasks_completed = await conn.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id) or 0
        time_spent_seconds = await conn.fetchval("SELECT time_spent FROM roblox_time WHERE member_id = $1", member_id) or 0
        active_strikes = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member_id, utcnow())
    time_spent_minutes = time_spent_seconds // 60
    await interaction.response.send_message(
        f"You have **{tasks_completed}/{WEEKLY_REQUIREMENT}** tasks and **{time_spent_minutes}/{WEEKLY_TIME_REQUIREMENT}** mins. "
        f"Active strikes: **{active_strikes}/3**.",
        ephemeral=True
    )

# /viewtasks
@bot.tree.command(name="viewtasks", description="Show a member's E&L task totals by type (all-time).")
async def viewtasks(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT COALESCE(NULLIF(task_type, ''), task) AS ttype, COUNT(*) AS cnt "
            "FROM task_logs WHERE member_id = $1 GROUP BY ttype ORDER BY cnt DESC, ttype ASC",
            target.id,
        )
        total = await conn.fetchval("SELECT COUNT(*) FROM task_logs WHERE member_id = $1", target.id)
    if not rows:
        await interaction.response.send_message(f"No tasks found for {target.display_name}.", ephemeral=True)
        return
    lines = []
    for r in rows:
        base = r['ttype'] or "Uncategorized"
        label = TASK_PLURALS.get(base, base + ("s" if not base.endswith("s") else ""))
        lines.append(f"**{label}** — {r['cnt']}")
    embed = discord.Embed(
        title=f"🗂️ Task Totals for {target.display_name}",
        description="\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=utcnow()
    )
    embed.set_footer(text=f"Total tasks: {total}")
    await log_action("Viewed Tasks", f"Requester: {interaction.user.mention}\nTarget: {target.mention if target != interaction.user else 'self'}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# /addtask (mgmt)
@bot.tree.command(name="addtask", description="(Mgmt) Add tasks to a member's history and weekly totals.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.choices(task_type=[app_commands.Choice(name=t, value=t) for t in TASK_TYPES])
async def addtask(
    interaction: discord.Interaction,
    member: discord.Member,
    task_type: str,
    count: app_commands.Range[int, 1, 100] = 1,
    comments: str | None = None,
    proof: discord.Attachment | None = None,
):
    now = utcnow()
    proof_url = proof.url if proof else None
    comments_val = comments or "Added by management"

    async with bot.db_pool.acquire() as conn:
        async with conn.transaction():
            batch_rows = [(member.id, task_type, task_type, proof_url, comments_val, now)] * count
            await conn.executemany(
                "INSERT INTO task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                batch_rows
            )
            await conn.executemany(
                "INSERT INTO weekly_task_logs (member_id, task, task_type, proof_url, comments, timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                batch_rows
            )
            await conn.execute(
                "INSERT INTO weekly_tasks (member_id, tasks_completed) VALUES ($1, $2) "
                "ON CONFLICT (member_id) DO UPDATE SET tasks_completed = weekly_tasks.tasks_completed + $2",
                member.id, count
            )

        rows = await conn.fetch(
            "SELECT COALESCE(NULLIF(task_type, ''), task) AS ttype, COUNT(*) AS cnt "
            "FROM task_logs WHERE member_id = $1 GROUP BY ttype ORDER BY cnt DESC, ttype ASC",
            member.id,
        )

    lines = []
    for r in rows:
        base = r['ttype'] or "Uncategorized"
        label = TASK_PLURALS.get(base, base + ("s" if not base.endswith("s") else ""))
        lines.append(f"{label} — {r['cnt']}")

    desc = f"Added **{count}× {task_type}** to {member.mention}.\n\n**Now totals:**\n" + "\n".join(lines)
    embed = discord.Embed(title="✅ Tasks Added", description=desc, color=discord.Color.green(), timestamp=utcnow())
    if proof_url:
        embed.set_image(url=proof_url)

    await log_action("Tasks Added", f"By: {interaction.user.mention}\nMember: {member.mention}\nType: **{task_type}** × {count}")
    await interaction.response.send_message(embed=embed, ephemeral=True)

# /leaderboard
@bot.tree.command(name="leaderboard", description="Displays the weekly leaderboard (tasks + on-site minutes).")
async def leaderboard(interaction: discord.Interaction):
    async with bot.db_pool.acquire() as conn:
        task_rows = await conn.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
        time_rows = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")

    task_map = {r['member_id']: r['tasks_completed'] for r in task_rows}
    time_map = {r['member_id']: r['time_spent'] for r in time_rows}

    member_ids = set(task_map.keys()) | set(time_map.keys())
    if not member_ids:
        await interaction.response.send_message("No activity logged this week.", ephemeral=True)
        return

    records = []
    for mid in member_ids:
        member = interaction.guild.get_member(mid)
        name = member.display_name if member else f"Unknown ({mid})"
        tasks_done = task_map.get(mid, 0)
        minutes_done = (time_map.get(mid, 0) // 60)
        records.append((name, tasks_done, minutes_done, mid))

    records.sort(key=lambda x: (-x[1], -x[2], x[0].lower()))

    embed = discord.Embed(title="🏆 E&L Weekly Leaderboard", color=discord.Color.gold(), timestamp=utcnow())
    lines = []
    rank_emoji = ["🥇", "🥈", "🥉"]
    for i, (name, tasks_done, minutes_done, _) in enumerate(records[:10]):
        prefix = rank_emoji[i] if i < 3 else f"**{i+1}.**"
        lines.append(f"{prefix} **{name}** — {tasks_done} tasks, {minutes_done} mins")
    embed.description = "\n".join(lines)
    await log_action("Viewed Leaderboard", f"Requester: {interaction.user.mention}")
    await interaction.response.send_message(embed=embed)

# Remove last weekly log (mgmt)
@bot.tree.command(name="removelastlog", description="(Mgmt) Removes the last logged weekly task for a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def removelastlog(interaction: discord.Interaction, member: discord.Member):
    member_id = member.id
    async with bot.db_pool.acquire() as conn:
        async with conn.transaction():
            last_log = await conn.fetchrow(
                "SELECT log_id, task FROM weekly_task_logs WHERE member_id = $1 ORDER BY timestamp DESC LIMIT 1",
                member_id
            )
            if not last_log:
                await interaction.response.send_message(f"{member.display_name} has no weekly tasks logged.", ephemeral=True)
                return
            await conn.execute("DELETE FROM weekly_task_logs WHERE log_id = $1", last_log['log_id'])
            await conn.execute(
                "UPDATE weekly_tasks SET tasks_completed = GREATEST(tasks_completed - 1, 0) WHERE member_id = $1",
                member_id
            )
            new_count = await conn.fetchval("SELECT tasks_completed FROM weekly_tasks WHERE member_id = $1", member_id)

    await log_action("Removed Last Weekly Task", f"By: {interaction.user.mention}\nMember: {member.mention}\nRemoved: **{last_log['task']}**")
    await interaction.response.send_message(
        f"Removed last weekly task for {member.mention}: '{last_log['task']}'. They now have {new_count} tasks.",
        ephemeral=True
    )

# /welcome — compact
@bot.tree.command(name="welcome", description="Sends the official E&L welcome message.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def welcome(interaction: discord.Interaction):
    class WelcomeLinks(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=None)
            self.add_item(
                discord.ui.Button(
                    label="E&L Guidelines",
                    url="https://trello.com/b/N1nrWeG4/el-information-hub"
                )
            )

    embed = discord.Embed(
        title="Welcome to Engineering & Logistics!",
        description="We keep the facility running — maintenance, logistics, and systems.",
        color=discord.Color.green(),
        timestamp=utcnow()
    )

    embed.add_field(
        name="1) Start here",
        value="Review the **E&L Guidelines** (button below).",
        inline=False
    )
    embed.add_field(
        name="2) Verify & tracking",
        value="Run **/verify <roblox_username>** so your on-site time and tasks start tracking to your account.",
        inline=False
    )
    embed.add_field(
        name="3) Intern Seminar",
        value="If you hold the **E&L Intern** role, you have **2 weeks** to complete your **Internship Seminar**. Contact management to schedule.",
        inline=False
    )

    embed.set_footer(text="Questions? Ping E&L Management • Stay safe, stay efficient.")

    await interaction.channel.send(embed=embed, view=WelcomeLinks())
    await log_action("Welcome Sent", f"By: {interaction.user.mention} • Channel: {interaction.channel.mention}")
    await interaction.response.send_message("Welcome message sent!", ephemeral=True)

# /dm
@bot.tree.command(name="dm", description="(Mgmt) Send a direct message to a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def dm(interaction: discord.Interaction, member: discord.Member, title: str, message: str):
    if member.bot:
        await interaction.response.send_message("You can't send messages to bots!", ephemeral=True)
        return
    description = message.replace('\\n', '\n')
    try:
        await send_long_embed(
            target=member,
            title=f"💌 {title}",
            description=description,
            color=discord.Color.magenta(),
            footer_text=f"A message from {interaction.guild.name}"
        )
        await log_action("DM Sent", f"From: {interaction.user.mention}\nTo: {member.mention}\nTitle: **{title}**")
        await interaction.response.send_message(f"Your message has been sent to {member.mention}!", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(f"I couldn't message {member.mention}. They might have DMs disabled.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)
        print(f"DM command error: {e}")

# Internship Seminar commands
@bot.tree.command(name="seminar_passed", description="(Mgmt) Mark a member as having passed their Internship Seminar.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def seminar_passed(interaction: discord.Interaction, member: discord.Member):
    assigned = utcnow()
    deadline = assigned + datetime.timedelta(days=14)
    passed_at = assigned
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO internship_seminars (discord_id, assigned_at, deadline, passed, passed_at, warned_5d, expired_handled)
            VALUES ($1, $2, $3, TRUE, $4, TRUE, TRUE)
            ON CONFLICT (discord_id)
            DO UPDATE SET
                passed = TRUE,
                passed_at = EXCLUDED.passed_at,
                warned_5d = TRUE,
                expired_handled = TRUE
            """,
            member.id, assigned, deadline, passed_at
        )
    await log_action("Internship Seminar Passed", f"Member: {member.mention}\nBy: {interaction.user.mention}")
    await interaction.response.send_message(f"Marked {member.mention} as **passed their Internship Seminar**.", ephemeral=True)

@bot.tree.command(name="seminar_view", description="View a member's Internship Seminar status.")
async def seminar_view(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    await ensure_intern_record(target)
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT assigned_at, deadline, passed, passed_at FROM internship_seminars WHERE discord_id = $1",
            target.id
        )
    if not row:
        await interaction.response.send_message(f"No seminar record for {target.display_name}.", ephemeral=True)
        return
    if row["passed"]:
        when = row["passed_at"].strftime("%Y-%m-%d %H:%M UTC") if row["passed_at"] else "unknown time"
        msg = f"**{target.display_name}**: ✅ Seminar passed (at {when})."
    else:
        remaining = row["deadline"] - utcnow()
        pretty = human_remaining(remaining)
        msg = (
            f"**{target.display_name}**: ❌ Not passed.\n"
            f"Deadline: **{row['deadline'].strftime('%Y-%m-%d %H:%M UTC')}** "
            f"(**{pretty}** remaining)"
        )
    await log_action("Seminar Viewed", f"Requester: {interaction.user.mention}\nTarget: {target.mention if target != interaction.user else 'self'}")
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="seminar_extend", description="(Mgmt) Extend a member's seminar deadline by N days.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def seminar_extend(interaction: discord.Interaction, member: discord.Member, days: app_commands.Range[int, 1, 60], reason: str | None = None):
    await ensure_intern_record(member)
    async with bot.db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT deadline, passed FROM internship_seminars WHERE discord_id = $1", member.id)
        if not row:
            await interaction.response.send_message(f"No seminar record for {member.display_name} (are they an Intern?).", ephemeral=True)
            return
        if row["passed"]:
            await interaction.response.send_message(f"{member.display_name} already passed their seminar.", ephemeral=True)
            return
        new_deadline = (row["deadline"] or utcnow()) + datetime.timedelta(days=days)
        await conn.execute("UPDATE internship_seminars SET deadline = $1 WHERE discord_id = $2", new_deadline, member.id)

    await log_action("Seminar Deadline Extended",
                     f"Member: {member.mention}\nAdded: **{days}** day(s)\nNew deadline: **{new_deadline.strftime('%Y-%m-%d %H:%M UTC')}**\nReason: {reason or '—'}")
    await interaction.response.send_message(
        f"Extended {member.mention}'s seminar by **{days}** day(s). New deadline: **{new_deadline.strftime('%Y-%m-%d %H:%M UTC')}**.",
        ephemeral=True
    )

# Strike helpers/commands
async def issue_strike(member: discord.Member, reason: str, *, set_by: int | None, auto: bool) -> int:
    now = utcnow()
    expires = now + datetime.timedelta(days=90)
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO strikes (member_id, reason, issued_at, expires_at, set_by, auto) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            member.id, reason, now, expires, set_by, auto
        )
        active = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member.id, now)
    try:
        await member.send(
            f"You've received a strike for failing to meet weekly requirements. "
            f"This will expire on **{expires.strftime('%Y-%m-%d')}**. "
            f"(**{active}/3 strikes**)"
        )
    except:
        pass
    await log_action("Strike Issued", f"Member: {member.mention}\nReason: {reason}\nAuto: {auto}\nActive now: **{active}/3**")
    return active

async def enforce_three_strikes(member: discord.Member):
    try:
        await member.send("You've been automatically removed from **Engineering & Logistics** for reaching **3/3 strikes**.")
    except:
        pass
    roblox_removed = await try_remove_from_roblox(member.id)
    kicked = False
    try:
        await member.kick(reason="Reached 3/3 strikes — automatic removal.")
        kicked = True
    except Exception as e:
        print(f"Kick failed for {member.id}: {e}")
    await log_action("Three-Strike Removal",
                     f"Member: {member.mention}\nRoblox removal: {'✅' if roblox_removed else '❌/N/A'}\nDiscord kick: {'✅' if kicked else '❌'}")

@bot.tree.command(name="strikes_add", description="(Mgmt) Add a strike to a member.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def strikes_add(interaction: discord.Interaction, member: discord.Member, reason: str):
    active_after = await issue_strike(member, reason, set_by=interaction.user.id, auto=False)
    if active_after >= 3:
        await enforce_three_strikes(member)
    await interaction.response.send_message(f"Strike added to {member.mention}. Active strikes: **{active_after}/3**.", ephemeral=True)

@bot.tree.command(name="strikes_remove", description="(Mgmt) Remove N active strikes from a member (earliest expiring first).")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def strikes_remove(interaction: discord.Interaction, member: discord.Member, count: app_commands.Range[int, 1, 10] = 1):
    now = utcnow()
    async with bot.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT strike_id FROM strikes WHERE member_id=$1 AND expires_at > $2 ORDER BY expires_at ASC LIMIT $3",
            member.id, now, count
        )
        if not rows:
            await interaction.response.send_message(f"{member.display_name} has no active strikes.", ephemeral=True)
            return
        ids = [r['strike_id'] for r in rows]
        await conn.execute("DELETE FROM strikes WHERE strike_id = ANY($1::int[])", ids)
        remaining = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1 AND expires_at > $2", member.id, now)
    await log_action("Strikes Removed", f"Member: {member.mention}\nRemoved: **{len(ids)}**\nActive remaining: **{remaining}/3**")
    await interaction.response.send_message(f"Removed **{len(ids)}** strike(s) from {member.mention}. Active remaining: **{remaining}/3**.", ephemeral=True)

@bot.tree.command(name="strikes_view", description="View a member's active and total strikes.")
async def strikes_view(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    now = utcnow()
    async with bot.db_pool.acquire() as conn:
        active_rows = await conn.fetch("SELECT reason, expires_at, issued_at, auto FROM strikes WHERE member_id=$1 AND expires_at > $2 ORDER BY expires_at ASC", target.id, now)
        total = await conn.fetchval("SELECT COUNT(*) FROM strikes WHERE member_id=$1", target.id)
    if not active_rows:
        desc = f"**Active strikes:** 0/3\n**Total strikes ever:** {total}"
    else:
        lines = [f"• {r['reason']} — expires **{r['expires_at'].strftime('%Y-%m-%d')}** ({'auto' if r['auto'] else 'manual'})" for r in active_rows]
        desc = f"**Active strikes:** {len(active_rows)}/3\n" + "\n".join(lines) + f"\n\n**Total strikes ever:** {total}"
    embed = discord.Embed(title=f"Strikes for {target.display_name}", description=desc, color=discord.Color.orange(), timestamp=utcnow())
    await interaction.response.send_message(embed=embed, ephemeral=True)

# === Activity Excuse Commands ===
@bot.tree.command(name="excuse", description="(Mgmt) Excuse a member from weekly requirements for a number of days.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
async def excuse_member(
    interaction: discord.Interaction,
    member: discord.Member,
    days: app_commands.Range[int, 1, 60],
    reason: str
):
    now = utcnow()
    expires_at = now + datetime.timedelta(days=days)
    clean_reason = reason.strip() or "No reason provided"
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO member_activity_excuses (member_id, reason, expires_at, set_by, set_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (member_id) DO UPDATE
            SET reason = EXCLUDED.reason,
                expires_at = EXCLUDED.expires_at,
                set_by = EXCLUDED.set_by,
                set_at = EXCLUDED.set_at
            """,
            member.id, clean_reason, expires_at, interaction.user.id, now
        )

    await log_action(
        "Activity Excuse Granted",
        f"Member: {member.mention}\nUntil: {expires_at.strftime('%Y-%m-%d %H:%M UTC')}\nReason: {clean_reason}"
    )
    await interaction.response.send_message(
        f"Excused {member.mention} until **{expires_at.strftime('%Y-%m-%d %H:%M UTC')}**. Reason: {clean_reason}",
        ephemeral=True
    )

# === Weekly check + reset (Sundays UTC) ===
@tasks.loop(time=datetime.time(hour=4, minute=0, tzinfo=datetime.timezone.utc))
async def check_weekly_tasks():
    if utcnow().weekday() != 6:
        return
    wk = week_key()
    async with bot.db_pool.acquire() as conn:
        now = utcnow()
        await conn.execute("DELETE FROM member_activity_excuses WHERE expires_at <= $1", now)
        is_excused_row = await conn.fetchrow("SELECT week_key, reason FROM activity_excuses WHERE week_key=$1", wk)
        member_excuse_rows = await conn.fetch(
            "SELECT member_id, reason, expires_at FROM member_activity_excuses WHERE expires_at > $1",
            now
        )
    excused_reason = is_excused_row["reason"] if is_excused_row else None
    member_excuses = {row["member_id"]: row for row in member_excuse_rows}

    announcement_channel = bot.get_channel(ANNOUNCEMENT_CHANNEL_ID) if ANNOUNCEMENT_CHANNEL_ID else None
    if not announcement_channel:
        print("Weekly check warning: Announcement channel not configured.")

    guild = announcement_channel.guild if announcement_channel else (bot.guilds[0] if bot.guilds else None)
    if not guild:
        print("Weekly check aborted: no guild context.")
        return

    dept_role = guild.get_role(DEPARTMENT_ROLE_ID) if DEPARTMENT_ROLE_ID else None
    dept_member_ids = {m.id for m in (dept_role.members if dept_role else guild.members) if not m.bot}

    async with bot.db_pool.acquire() as conn:
        all_tasks = await conn.fetch("SELECT member_id, tasks_completed FROM weekly_tasks")
        all_time = await conn.fetch("SELECT member_id, time_spent FROM roblox_time")
        strike_counts = {
            r['member_id']: r['cnt'] for r in await conn.fetch(
                "SELECT member_id, COUNT(*) as cnt FROM strikes WHERE expires_at > $1 GROUP BY member_id",
                utcnow()
            )
        }

    tasks_map = {r['member_id']: r['tasks_completed'] for r in all_tasks if r['member_id'] in dept_member_ids}
    time_map = {r['member_id']: r['time_spent'] for r in all_time if r['member_id'] in dept_member_ids}

    met, not_met, zero, excused_members = [], [], [], []
    considered_ids = set(tasks_map.keys()) | set(time_map.keys())

    for member_id in considered_ids:
        member = guild.get_member(member_id)
        if not member:
            continue
        if member_id in member_excuses:
            row = member_excuses[member_id]
            sc = strike_counts.get(member_id, 0)
            excused_members.append((member, row["reason"], row["expires_at"], sc))
            continue
        tasks_done = tasks_map.get(member_id, 0)
        time_done_minutes = (time_map.get(member_id, 0)) // 60
        sc = strike_counts.get(member_id, 0)
        if tasks_done >= WEEKLY_REQUIREMENT and time_done_minutes >= WEEKLY_TIME_REQUIREMENT:
            met.append((member, sc))
        else:
            not_met.append((member, tasks_done, time_done_minutes, sc))

    zero_ids = dept_member_ids - considered_ids
    for mid in zero_ids:
        member = guild.get_member(mid)
        if member:
            if mid in member_excuses:
                row = member_excuses[mid]
                sc = strike_counts.get(mid, 0)
                excused_members.append((member, row["reason"], row["expires_at"], sc))
                continue
            sc = strike_counts.get(mid, 0)
            zero.append((member, sc))

    def fmt_met(lst):
        return ", ".join(f"{m.mention} (strikes: {sc})" for m, sc in lst) if lst else "—"

    def fmt_not_met(lst):
        return "\n".join(f"{m.mention} — {t}/{WEEKLY_REQUIREMENT} tasks, {mins}/{WEEKLY_TIME_REQUIREMENT} mins (strikes: {sc})" for m, t, mins, sc in lst) if lst else "—"

    def fmt_zero(lst):
        return ", ".join(f"{m.mention} (strikes: {sc})" for m, sc in lst) if lst else "—"

    def fmt_excused(lst):
        return "\n".join(
            f"{m.mention} — until **{exp.strftime('%Y-%m-%d')}** (strikes: {sc})\nReason: {rsn}"
            for m, rsn, exp, sc in lst
        ) if lst else "—"

    title = "Weekly Task Summary"
    summary = f"--- Weekly Task Report (**{wk}**){' — EXCUSED' if excused_reason else ''} ---\n\n"
    if excused_reason:
        summary += f"**Excuse Reason:** {excused_reason}\n\n"
    summary += f"**✅ Met Requirement ({len(met)}):**\n{fmt_met(met)}\n\n"
    summary += f"**❌ Below Quota ({len(not_met)}):**\n{fmt_not_met(not_met)}\n\n"
    summary += f"**🚫 0 Activity ({len(zero)}):**\n{fmt_zero(zero)}\n\n"
    summary += f"**🛌 Excused Members ({len(excused_members)}):**\n{fmt_excused(excused_members)}\n\n"
    summary += "Weekly counts will now be reset."

    if announcement_channel:
        await send_long_embed(
            target=announcement_channel,
            title=title,
            description=summary,
            color=discord.Color.gold(),
            footer_text=None
        )
    else:
        await log_action(title, summary)

    # Issue strikes for not-met (if NOT excused)
    if not excused_reason:
        for m, t, mins, _sc in not_met + [(m, 0, 0, sc) for m, sc in zero]:
            try:
                if not m:
                    continue
                active_after = await issue_strike(m, "Missed weekly quota", set_by=None, auto=True)
                if active_after >= 3:
                    await enforce_three_strikes(m)
            except Exception as e:
                print(f"Strike flow error for {getattr(m, 'id', 'unknown')}: {e}")

    # Reset weekly tables
    async with bot.db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE weekly_tasks, weekly_task_logs, roblox_time, roblox_sessions")
    print("Weekly tasks and time checked and reset.")

# Internship Seminar 5-day warning + overdue enforcement
@tasks.loop(minutes=30)
async def internship_seminar_loop():
    try:
        async with bot.db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT discord_id, deadline, warned_5d, passed, expired_handled "
                "FROM internship_seminars WHERE passed = FALSE"
            )
        if not rows:
            return

        now = utcnow()
        for r in rows:
            discord_id = r["discord_id"]
            deadline = r["deadline"]
            warned_5d = r["warned_5d"]
            expired_handled = r["expired_handled"]
            if not deadline:
                continue

            remaining = deadline - now

            # 5-day warning (one-time)
            if (not warned_5d) and datetime.timedelta(days=4, hours=23) <= remaining <= datetime.timedelta(days=5, hours=1):
                ch = bot.get_channel(COMMAND_LOG_CHANNEL_ID) or (bot.get_channel(ANNOUNCEMENT_CHANNEL_ID) if ANNOUNCEMENT_CHANNEL_ID else None)
                if ch:
                    member = find_member(discord_id)
                    mention = member.mention if member else f"<@{discord_id}>"
                    pretty = human_remaining(remaining)
                    await ch.send(
                        f"{mention} hasn’t completed their **Internship Seminar** yet and has **{pretty}** remaining. Please check in with them."
                    )
                    async with bot.db_pool.acquire() as conn2:
                        await conn2.execute("UPDATE internship_seminars SET warned_5d = TRUE WHERE discord_id = $1", discord_id)

            # Overdue enforcement (only once)
            if remaining <= datetime.timedelta(seconds=0) and not expired_handled:
                member = find_member(discord_id)
                if member:
                    try:
                        await member.send(
                            "Hi — automatic notice from **Engineering & Logistics**.\n\n"
                            "Your **2-week Internship Seminar** deadline has passed and you have been **removed** due to not completing it in time.\n"
                            "If this is a mistake, please contact E&L Management."
                        )
                    except:
                        pass

                    roblox_removed = await try_remove_from_roblox(discord_id)

                    kicked = False
                    try:
                        await member.kick(reason="Internship Seminar deadline expired — automatic removal.")
                        kicked = True
                    except Exception as e:
                        print(f"Kick failed for {member.id}: {e}")

                    await log_action(
                        "Internship Seminar Expiry Enforced",
                        f"Member: {member.mention}\nRoblox removal: {'✅' if roblox_removed else 'Skipped/Failed ❌'}\nDiscord kick: {'✅' if kicked else '❌'}"
                    )

                    async with bot.db_pool.acquire() as conn3:
                        await conn3.execute("UPDATE internship_seminars SET expired_handled = TRUE WHERE discord_id = $1", discord_id)
    except Exception as e:
        print(f"internship_seminar_loop error: {e}")

@internship_seminar_loop.before_loop
async def before_internship_loop():
    await bot.wait_until_ready()

# --- Autocompletes ---
async def group_role_autocomplete(interaction: discord.Interaction, current: str):
    current_lower = (current or "").lower()
    roles = await fetch_group_ranks()
    if not roles:
        return []
    out = []
    for r in roles:
        name = r.get('name', '')
        if not current_lower or name.lower().startswith(current_lower):
            out.append(app_commands.Choice(name=name, value=name))
        if len(out) >= 25:
            break
    return out

# NEW: Week autocomplete for excuse commands
async def week_autocomplete(_: discord.Interaction, current: str):
    now = utcnow()
    choices = []
    labels = []

    wk_prev = week_key(now - datetime.timedelta(weeks=1))
    wk_curr = week_key(now)
    wk_next = week_key(now + datetime.timedelta(weeks=1))
    labels.extend([("Previous (" + wk_prev + ")", wk_prev),
                   ("Current (" + wk_curr + ")", wk_curr),
                   ("Next (" + wk_next + ")", wk_next)])

    for i in range(2, 8):
        w = week_key(now - datetime.timedelta(weeks=i))
        labels.append((w, w))

    cur = (current or "").lower()
    for name, value in labels:
        if not cur or cur in name.lower() or cur in value.lower():
            choices.append(app_commands.Choice(name=name, value=value))
        if len(choices) >= 25:
            break
    return choices

# === Excuse slash commands (NEW) ===
@bot.tree.command(name="excuse_set", description="(Mgmt) Mark an ISO week as excused from weekly quota & strikes.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.autocomplete(week=week_autocomplete)
async def excuse_set(
    interaction: discord.Interaction,
    reason: str,
    week: str | None = None
):
    wk = _resolve_week_key(week)
    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO activity_excuses (week_key, reason, set_by, set_at) "
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (week_key) DO UPDATE SET reason = EXCLUDED.reason, set_by = EXCLUDED.set_by, set_at = EXCLUDED.set_at",
            wk, reason, interaction.user.id, utcnow()
        )
    await log_action("Week Excused", f"By: {interaction.user.mention}\nWeek: **{wk}**\nReason: {reason}")
    await interaction.response.send_message(
        f"✅ Marked **{wk}** as **EXCUSED**. Reason: *{reason}*",
        ephemeral=True
    )

@bot.tree.command(name="excuse_clear", description="(Mgmt) Clear the excuse for an ISO week.")
@app_commands.checks.has_role(MANAGEMENT_ROLE_ID)
@app_commands.autocomplete(week=week_autocomplete)
async def excuse_clear(
    interaction: discord.Interaction,
    week: str | None = None
):
    wk = _resolve_week_key(week)
    async with bot.db_pool.acquire() as conn:
        rec = await conn.fetchrow("SELECT reason FROM activity_excuses WHERE week_key=$1", wk)
        if not rec:
            await interaction.response.send_message(f"No excuse set for **{wk}**.", ephemeral=True)
            return
        await conn.execute("DELETE FROM activity_excuses WHERE week_key=$1", wk)
    await log_action("Week Excuse Cleared", f"By: {interaction.user.mention}\nWeek: **{wk}**")
    await interaction.response.send_message(f"🧹 Cleared excuse for **{wk}**.", ephemeral=True)

@bot.tree.command(name="excuse_view", description="View the excuse (if any) for an ISO week.")
@app_commands.autocomplete(week=week_autocomplete)
async def excuse_view(
    interaction: discord.Interaction,
    week: str | None = None
):
    wk = _resolve_week_key(week)
    async with bot.db_pool.acquire() as conn:
        rec = await conn.fetchrow(
            "SELECT reason, set_by, set_at FROM activity_excuses WHERE week_key=$1",
            wk
        )
    if not rec:
        await interaction.response.send_message(f"**{wk}** is not excused.", ephemeral=True)
        return

    setter = find_member(int(rec["set_by"])) if rec["set_by"] else None
    setter_name = setter.mention if setter else (f"<@{rec['set_by']}>" if rec["set_by"] else "Unknown")
    when = rec["set_at"].strftime("%Y-%m-%d %H:%M UTC") if rec["set_at"] else "unknown"
    embed = discord.Embed(
        title=f"📅 Excuse for {wk}",
        description=f"**Reason:** {rec['reason']}\n**Set by:** {setter_name}\n**Set at:** {when}",
        color=discord.Color.light_grey(),
        timestamp=utcnow()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# /rank (autocomplete from Roblox service)
@bot.tree.command(name="rank", description="(Rank Manager) Set a member's Roblox/Discord rank to a group role.")
@app_commands.checks.has_role(RANK_MANAGER_ROLE_ID)
@app_commands.autocomplete(group_role=group_role_autocomplete)
async def rank(interaction: discord.Interaction, member: discord.Member, group_role: str):
    async with bot.db_pool.acquire() as conn:
        roblox_id = await conn.fetchval("SELECT roblox_id FROM roblox_verification WHERE discord_id = $1", member.id)
    if not roblox_id:
        await interaction.response.send_message(f"{member.display_name} hasn’t linked a Roblox account with `/verify` yet.", ephemeral=True)
        return

    ranks = await fetch_group_ranks()
    if not ranks:
        await interaction.response.send_message("Couldn’t fetch Roblox group ranks. Check ROBLOX_SERVICE_BASE & secret.", ephemeral=True)
        return

    target = next((r for r in ranks if r.get('name','').lower() == group_role.lower()), None)
    if not target:
        await interaction.response.send_message("That rank wasn’t found. Try typing to see suggestions.", ephemeral=True)
        return

    # Remove previous stored Discord rank role if it exists
    try:
        prev_rank = None
        async with bot.db_pool.acquire() as conn:
            prev_rank = await conn.fetchval("SELECT rank FROM member_ranks WHERE discord_id=$1", member.id)
        if prev_rank:
            for role in interaction.guild.roles:
                if role.name.lower() == prev_rank.lower():
                    await member.remove_roles(role, reason=f"Replacing rank via /rank by {interaction.user}")
                    break
    except Exception as e:
        print(f"/rank remove old role error: {e}")

    ok = await set_group_rank(int(roblox_id), role_id=int(target['id']))
    if not ok:
        await interaction.response.send_message("Failed to set Roblox rank (service error).", ephemeral=True)
        return

    async with bot.db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO member_ranks (discord_id, rank, set_by, set_at) VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (discord_id) DO UPDATE SET rank = EXCLUDED.rank, set_by = EXCLUDED.set_by, set_at = EXCLUDED.set_at",
            member.id, target['name'], interaction.user.id, utcnow()
        )

    assigned_role = None
    try:
        for role in interaction.guild.roles:
            if role.name.lower() == target['name'].lower():
                await member.add_roles(role, reason=f"Rank set via /rank by {interaction.user}")
                assigned_role = role
                break
    except Exception as e:
        print(f"/rank role assign error: {e}")

    msg = f"Set **Roblox rank** for {member.mention} to **{target['name']}**."
    if assigned_role:
        msg += f" Also assigned Discord role **{assigned_role.name}**."
    await log_action("Rank Set", f"By: {interaction.user.mention}\nMember: {member.mention}\nNew Rank: **{target['name']}**")
    await interaction.response.send_message(msg, ephemeral=True)

# === Run ===
if __name__ == "__main__":
    if ROBLOX_SERVICE_BASE:
        try:
            parsed = urlparse(ROBLOX_SERVICE_BASE)
            if not parsed.scheme or not parsed.netloc:
                print(f"[WARN] ROBLOX_SERVICE_BASE looks odd: {ROBLOX_SERVICE_BASE}")
        except Exception:
            print(f"[WARN] Could not parse ROBLOX_SERVICE_BASE: {ROBLOX_SERVICE_BASE}")
    else:
        print("[INFO] ROBLOX_SERVICE_BASE not set; /rank autocomplete + set-rank will be unavailable.")

    bot.run(BOT_TOKEN)

#!/usr/bin/env python3
# ==============================================
#  Ruijie Voucher Scanner Bot  — GitHub Safe Version
# ==============================================
import telebot
import asyncio
import aiohttp
import json
import base64
import random
import re
import os
import string
import time
import uuid
import threading
import logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import ddddocr
import numpy as np
import cv2
from datetime import datetime, timedelta, timezone
import sqlite3
from contextlib import contextmanager
# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# ======================================================
#  ⚙️ CONFIGURATION — Read from Environment Variables
#  GitHub Secrets မှ ဖတ်သည် — တိုက်ရိုက်မရေးပါနှင့်
# ======================================================
BOT_TOKEN = os.getenv("9270543827:AAHuqeAK_Q7j_2daPHwcYm2Nty8yR7WEpbQ")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN မတွေ့ပါ။ Environment Variable သို့မဟုတ် GitHub Secrets တွင် သတ်မှတ်ပါ")

ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [x.strip() for x in ADMIN_IDS_STR.split(",") if x.strip()]
ADMIN_NAME = os.getenv("ADMIN_NAME", "Admin")
FORWARD_CHANNEL = os.getenv("FORWARD_CHANNEL", "")

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "200"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
CONNECTION_LIMIT = int(os.getenv("CONNECTION_LIMIT", "1000"))
CONNECTION_PER_HOST = int(os.getenv("CONNECTION_PER_HOST", "500"))
TIMEOUT = int(os.getenv("TIMEOUT", "25"))
DB_PATH = "bot_data.db"

PROXY_LIST_STR = os.getenv("PROXY_LIST", "")
PROXY_LIST = [x.strip() for x in PROXY_LIST_STR.split(",") if x.strip()]
random.shuffle(PROXY_LIST)
# ======================================================
# --- Database ---
_db_conn = None
_db_lock = threading.Lock()
def get_db_connection():
    global _db_conn
    with _db_lock:
        if _db_conn is None:
            _db_conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
            _db_conn.execute("PRAGMA journal_mode=WAL")
            _db_conn.execute("PRAGMA synchronous=NORMAL")
            _db_conn.execute("PRAGMA cache_size=10000")
            _db_conn.row_factory = sqlite3.Row
        return _db_conn
def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS keys (key TEXT PRIMARY KEY, user_id TEXT, plan TEXT, expires_at TEXT, code_limit INTEGER DEFAULT 1000, used_codes INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS results (user_id TEXT PRIMARY KEY, codes TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, key TEXT, registered_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings (user_id TEXT PRIMARY KEY, proxy_enabled INTEGER DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS scan_checkpoints (user_id TEXT PRIMARY KEY, mode TEXT, last_code TEXT, start_digit TEXT, timestamp REAL)''')
    conn.commit()
    logger.info("Database initialized")
init_db()
# --- In-memory caches ---
user_data = {}
approve = {}
scan_tasks = {}
success_messages = {}
success_texts = {}
limited_messages = {}
limited_texts = {}
captcha_state = {}
paid_users = {}
_voucher_sem = None
_start_time = time.monotonic()
_connector = None
# --- Proxy helpers ---
_proxy_index = 0
def get_next_proxy():
    if not PROXY_LIST:
        return None
    global _proxy_index
    proxy = PROXY_LIST[_proxy_index % len(PROXY_LIST)]
    _proxy_index += 1
    if not proxy.startswith(('http://', 'https://')):
        proxy = f"http://{proxy}"
    return proxy
# --- DB Helpers ---
@contextmanager
def get_db_cursor():
    conn = get_db_connection()
    try:
        yield conn.cursor()
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB error: {e}")
        raise
def db_get_key(key):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM keys WHERE key = ?", (key,))
        r = c.fetchone()
        return dict(r) if r else None
def db_get_user(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return dict(r) if r else None
def db_get_user_by_key(key):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM users WHERE key = ?", (key,))
        r = c.fetchone()
        return dict(r) if r else None
def db_get_results(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT codes FROM results WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return json.loads(r[0]) if r else []
def db_save_results(user_id, codes):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO results (user_id, codes) VALUES (?, ?)", (user_id, json.dumps(codes)))
def db_add_user(user_id, key):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO users (user_id, key, registered_at) VALUES (?, ?, ?)",
                  (user_id, key, datetime.now(timezone.utc).isoformat()))
def db_add_key(key, user_id, plan, expires_at, code_limit):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO keys (key, user_id, plan, expires_at, code_limit, used_codes) VALUES (?, ?, ?, ?, ?, ?)",
                  (key, user_id, plan, expires_at, code_limit, 0))
def db_delete_key(key):
    with get_db_cursor() as c:
        c.execute("DELETE FROM keys WHERE key = ?", (key,))
def db_update_used_codes(key, used_codes):
    with get_db_cursor() as c:
        c.execute("UPDATE keys SET used_codes = ? WHERE key = ?", (used_codes, key))
def db_get_all_keys():
    with get_db_cursor() as c:
        c.execute("SELECT * FROM keys")
        return {r[0]: dict(r) for r in c.fetchall()}
def db_get_all_users():
    with get_db_cursor() as c:
        c.execute("SELECT user_id FROM users")
        return [r[0] for r in c.fetchall()]
def db_get_proxy_setting(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT proxy_enabled FROM user_settings WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return bool(r[0]) if r else True
def db_set_proxy_setting(user_id, enabled):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO user_settings (user_id, proxy_enabled) VALUES (?, ?)",
                  (user_id, 1 if enabled else 0))
def db_get_checkpoint(user_id):
    with get_db_cursor() as c:
        c.execute("SELECT * FROM scan_checkpoints WHERE user_id = ?", (user_id,))
        r = c.fetchone()
        return dict(r) if r else None
def db_save_checkpoint(user_id, mode, last_code, start_digit=None):
    with get_db_cursor() as c:
        c.execute("INSERT OR REPLACE INTO scan_checkpoints (user_id, mode, last_code, start_digit, timestamp) VALUES (?, ?, ?, ?, ?)",
                  (user_id, mode, last_code, start_digit, time.time()))
def db_clear_checkpoint(user_id):
    with get_db_cursor() as c:
        c.execute("DELETE FROM scan_checkpoints WHERE user_id = ?", (user_id,))
def check_key_expiration(expires_at):
    if not expires_at:
        return False
    try:
        if expires_at == "9999-12-31T23:59:59Z":
            return True
        exp_time = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp_time
    except:
        return False
def generate_expiry(plan):
    now = datetime.now(timezone.utc)
    plans = {"30m": timedelta(minutes=30), "1h": timedelta(hours=1), "1d": timedelta(days=1),
             "7d": timedelta(days=7), "1m": timedelta(days=30), "1y": timedelta(days=365), "unlimited": None}
    if plan not in plans:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + plans[plan]).isoformat()
def generate_random_key(length=12):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))
def is_admin(user_id):
    return str(user_id) in ADMIN_IDS
async def forward_to_channel(message_text, parse_mode=None):
    if not FORWARD_CHANNEL:
        return
    try:
        await bot.send_message(FORWARD_CHANNEL, message_text, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f"Forward error: {e}")
# ==================== KEYBOARDS ====================
def get_main_keyboard(user_id=None):
    keyboard = InlineKeyboardMarkup(row_width=2)
    if user_id:
        proxy_enabled = db_get_proxy_setting(user_id)
        proxy_text = "🔴 Proxy OFF" if not proxy_enabled else "🟢 Proxy ON"
        proxy_callback = "menu_proxy_off" if proxy_enabled else "menu_proxy_on"
    else:
        proxy_text = "🟢 Proxy ON"
        proxy_callback = "menu_proxy_off"
    keyboard.add(
        InlineKeyboardButton("💸 PAID USER", callback_data="menu_paid"),
        InlineKeyboardButton("🔗 STAR LINK Portal URL ထည့်ရန်", callback_data="menu_free_trial"),
        InlineKeyboardButton(proxy_text, callback_data=proxy_callback),
        InlineKeyboardButton("💻🔓 Success Codes ကြည့်မည်", callback_data="menu_result"),
        InlineKeyboardButton("🔄 Recheck ပြန်လုပ်စစ်မည်", callback_data="menu_recheck"),
        InlineKeyboardButton("🛑 Scan ရပ်မည်", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard
def get_voucher_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔢 DIGIT 6 (0-9)", callback_data="scan_6"),
        InlineKeyboardButton("🔢 DIGIT 7 (0-9)", callback_data="scan_7"),
        InlineKeyboardButton("🔢 DIGIT 8 (0-9)", callback_data="scan_8"),
        InlineKeyboardButton("🔤 ENG small 6 (a-z)", callback_data="scan_engsmall6"),
        InlineKeyboardButton("🔤 ENG small 7 (a-z)", callback_data="scan_engsmall7"),
        InlineKeyboardButton("🔤 ENG small 8 (a-z)", callback_data="scan_engsmall8"),
        InlineKeyboardButton("🔤+🔢 MIXED 6 (a-z0-9)", callback_data="scan_engmixed6"),
        InlineKeyboardButton("🔤+🔢 MIXED 7 (a-z0-9)", callback_data="scan_engmixed7"),
        InlineKeyboardButton("🔤+🔢 MIXED 8 (a-z0-9)", callback_data="scan_engmixed8"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard
def get_digit_keyboard(mode):
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("🎲 Random ဖြစ်ရှာရန်", callback_data=f"digit_{mode}_random"))
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard
def get_start_scam_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🚀 START SCAN", callback_data="menu_start_scam"), InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard
def get_paid_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("✅ KEY ထည့်ရန်", callback_data="menu_enter_key"), InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard
def get_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard
def get_scam_button_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🛑 STOP SCAN", callback_data="menu_stop"), InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard
def get_admin_main_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔑 Generate Key", callback_data="admin_genkey"),
        InlineKeyboardButton("🗑️ Delete Key", callback_data="admin_delkey"),
        InlineKeyboardButton("📋 List Keys", callback_data="admin_listkeys"),
        InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats"),
        InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
        InlineKeyboardButton("👾 Users List", callback_data="admin_users"),
        InlineKeyboardButton("🔙 Back to Menu", callback_data="admin_back")
    )
    return keyboard
def get_admin_genkey_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⏱️ 30m", callback_data="admin_gen_30m"),
        InlineKeyboardButton("⏱️ 1h", callback_data="admin_gen_1h"),
        InlineKeyboardButton("📅 1d", callback_data="admin_gen_1d"),
        InlineKeyboardButton("📅 7d", callback_data="admin_gen_7d"),
        InlineKeyboardButton("📅 1m", callback_data="admin_gen_1m"),
        InlineKeyboardButton("📅 1y", callback_data="admin_gen_1y"),
        InlineKeyboardButton("♾️ Unlimited", callback_data="admin_gen_unlimited"),
        InlineKeyboardButton("🔙 Back", callback_data="admin_back")
    )
    return keyboard
def get_admin_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_back"))
    return keyboard
# ==================== BOT INSTANCE ====================
bot = AsyncTeleBot(BOT_TOKEN)
# ==================== HELP COMMAND ====================
@bot.message_handler(commands=['help'])
async def help_command(message):
    user_id = str(message.chat.id)
    help_text = f"""📖 **Help Guide — {ADMIN_NAME} Bot**
👋 Welcome to STAR LINK CODE HACK Bot!
━━━━━━━━━━━━━━━━━━━━
🔹 **Commands for Users:**
/start → Main Menu
/key [your_key] → Register as PAID USER
/portal [portal_url] → Set your Portal URL
/scan [mode] → Start scan (e.g. /scan 6)
/stop → Stop current scan
/result → View your saved success codes
/recheck → Recheck your saved codes
/proxy → Toggle proxy ON/OFF
/help → Show this help
━━━━━━━━━━━━━━━━━━━━
🔸 **Admin Commands:**
/admin → Admin Panel
/genkey [plan] [limit] [user_id] → Generate key for user
/delkey [key] → Delete a key
/sendall [message] → Broadcast to all users
/status → Bot statistics
━━━━━━━━━━━━━━━━━━━━
📌 **Scan Modes:**
6, 7, 8 → Digit only (0-9)
engsmall6, engsmall7, engsmall8 →

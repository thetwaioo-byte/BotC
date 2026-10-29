#!/usr/bin/env python3
# ==============================================
#  Ruijie Voucher Scanner Bot  — Ready-to-Use
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
import cv2
import numpy as np
import ddddocr
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta, timezone
import sqlite3
from contextlib import contextmanager

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ======================================================
#  ✅ CONFIGURATION — သင့်အချက်အလက်များ ထည့်ပြီးသား
# ======================================================
BOT_TOKEN = "9270543827:AAHuqeAK_Q7j_2daPHwcYm2Nty8yR7WEpbQ"
ADMIN_IDS = ["6130147402"]
ADMIN_NAME = "Myogyi10384"
FORWARD_CHANNEL = "@Zawgyi1296"

MAX_CONCURRENT = 200
BATCH_SIZE = 200
CONNECTION_LIMIT = 1000
CONNECTION_PER_HOST = 500
TIMEOUT = 25
DB_PATH = "bot_data.db"

PROXY_LIST = [
    "gzsvv1pggl7k:3g9xpulazhkz2c2@65.111.5.6:3129",
    "y2g26w7t3tv4:p5ouenejkn07fvy@209.50.179.187:3129",
    "gdllkdvi6mhq:04l2fmxbv72tzkl@65.111.2.10:3129"
]
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
    logger.info("✅ Database ready")
init_db()

user_data = {}
approve = {}
scan_tasks = {}
success_messages = {}
success_texts = {}
paid_users = {}
_start_time = time.monotonic()
_connector = None

# --- Proxy ---
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
        InlineKeyboardButton("🔗 Portal URL ထည့်ရန်", callback_data="menu_free_trial"),
        InlineKeyboardButton(proxy_text, callback_data=proxy_callback),
        InlineKeyboardButton("💻 Success Codes ကြည့်မည်", callback_data="menu_result"),
        InlineKeyboardButton("🔄 Recheck ပြန်စစ်မည်", callback_data="menu_recheck"),
        InlineKeyboardButton("🛑 Scan ရပ်မည်", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_voucher_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔢 6 လုံး", callback_data="scan_6"),
        InlineKeyboardButton("🔢 7 လုံး", callback_data="scan_7"),
        InlineKeyboardButton("🔢 8 လုံး", callback_data="scan_8"),
        InlineKeyboardButton("🔤 အင်္ဂလိပ် 6", callback_data="scan_engsmall6"),
        InlineKeyboardButton("🔤 အင်္ဂလိပ် 7", callback_data="scan_engsmall7"),
        InlineKeyboardButton("🔤 အင်္ဂလိပ် 8", callback_data="scan_engsmall8"),
        InlineKeyboardButton("🔤+🔢 ရောစပ် 6", callback_data="scan_engmixed6"),
        InlineKeyboardButton("🔤+🔢 ရောစပ် 7", callback_data="scan_engmixed7"),
        InlineKeyboardButton("🔤+🔢 ရောစပ် 8", callback_data="scan_engmixed8"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back")
    )
    return keyboard

def get_digit_keyboard(mode):
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_random"))
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_start_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🚀 SCAN စတင်မည်", callback_data="menu_start_scam"), InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_paid_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("✅ KEY ထည့်ရန်", callback_data="menu_enter_key"), InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_stop_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🛑 ရပ်မည်", callback_data="menu_stop"), InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_admin_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔑 Key ထုတ်မည်", callback_data="admin_genkey"),
        InlineKeyboardButton("🗑️ Key ဖျက်မည်", callback_data="admin_delkey"),
        InlineKeyboardButton("📋 Key များကြည့်မည်", callback_data="admin_listkeys"),
        InlineKeyboardButton("📊 အချက်အလက်", callback_data="admin_stats"),
        InlineKeyboardButton("📢 အကုန်ပို့မည်", callback_data="admin_broadcast"),
        InlineKeyboardButton("👾 အသုံးပြုသူများ", callback_data="admin_users"),
        InlineKeyboardButton("🔙 Back", callback_data="admin_back")
    )
    return keyboard

def get_admin_plan_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⏱️ ၃၀မိနစ်", callback_data="admin_gen_30m"),
        InlineKeyboardButton("⏱️ ၁နာရီ", callback_data="admin_gen_1h"),
        InlineKeyboardButton("📅 ၁ရက်", callback_data="admin_gen_1d"),
        InlineKeyboardButton("📅 ၇ရက်", callback_data="admin_gen_7d"),
        InlineKeyboardButton("📅 ၁လ", callback_data="admin_gen_1m"),
        InlineKeyboardButton("📅 ၁နှစ်", callback_data="admin_gen_1y"),
        InlineKeyboardButton("♾️ ထာဝရ", callback_data="admin_gen_unlimited"),
        InlineKeyboardButton("🔙 Back", callback_data="admin_back")
    )
    return keyboard

# ==================== BOT ====================
bot = AsyncTeleBot(BOT_TOKEN)

# --- Commands ---
@bot.message_handler(commands=['start'])
async def start(message):
    user_id = str(message.chat.id)
    user_name = message.from_user.first_name or "User"
    
    if is_admin(user_id):
        approve[message.chat.id] = True
        paid_users[user_id] = True
        valid = True
        admin_msg = "✅ သင်သည် Admin ဖြစ်ပါသည်။ KEY မလိုအပ်ပါ။"
    else:
        user_info = db_get_user(user_id)
        valid = False
        admin_msg = ""
        if user_info:
            key_info = db_get_key(user_info["key"])
            if key_info and check_key_expiration(key_info["expires_at"]):
                valid = True
                approve[message.chat.id] = True
                paid_users[user_id] = True
    
    proxy_status = "ON" if db_get_proxy_setting(user_id) else "OFF"
    
    if valid:
        text = f"""✨ VOUCHER SCAN BOT ✨
👹 အမည်: {user_name}
🆔 ID: {user_id}
✅ PAID USER အဖြစ်အသုံးပြုနိုင်ပါသည်။
{admin_msg}
🔄 Proxy: {proxy_status}
အောက်က Menu မှရွေးချယ်ပါ။"""
    else:
        text = f"""✨ VOUCHER SCAN BOT ✨
👹 အမည်: {user_name}
🆔 ID: {user_id}
⚠️ အကောင့်မှတ်ပုံတင်မရသေးပါ။
PAID USER ဖြစ်ရန် Admin သို့ဆက်သွယ်ပါ။"""
    
    await bot.send_message(message.chat.id, text, reply_markup=get_main_keyboard(user_id))
    await forward_to_channel(f"🆕 အသုံးပြုသူအသစ်: {user_name} | ID: {user_id}")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    text = """📖 အသုံးပြုနည်း

/start - မီနူး
/key [သင့်ကီး] - အကောင့်ဝင်ရန်
/portal [URL] - Portal URL ထည့်ရန်
/scan [မုဒ်] - စတင်စစ်ဆေးရန်
/stop - ရပ်တန့်ရန်
/result - ရလဒ်ကြည့်ရန်
/recheck - ပြန်စစ်ဆေးရန်
/proxy - Proxy ဖွင့်/ပိတ်

🔢 မုဒ်များ- 6,7,8 | engsmall6-8 | engmixed6-8
"""
    await bot.reply_to(message, text)

@bot.message_handler(commands=['key'])
async def key_cmd(message):
    user_id = str(message.chat.id)
    if is_admin(user_id):
        await bot.reply_to(message, "✅ Admin ဖြစ်သောကြောင့် KEY မလိုအပ်ပါ။")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "🔑 /key [သင့်ကီး]")
        return
    key = args[1].strip()
    key_info = db_get_key(key)
    if not key_info:
        await bot.reply_to(message, "❌ ကီးမှားနေပါသည်။")
        return
    if not check_key_expiration(key_info["expires_at"]):
        await bot.reply_to(message, "❌ ကီးသက်တမ်းကုန်သွားပါပြီ။")
        return
    db_add_user(user_id, key)
    approve[message.chat.id] = True
    paid_users[user_id] = True
    await bot.reply_to(message, f"✅ အောင်မြင်စွာအသုံးပြုနိုင်ပါပြီ။\nPlan: {key_info['plan']}")
    await forward_to_channel(f"✅ အသုံးပြုသူဝင်ရောက်: {user_id} | Key: {key}")

@bot.message_handler(commands=['portal'])
async def portal_cmd(message):
    user_id = str(message.chat.id)
    if user_id not in paid_users and not is_admin(user_id):
        await bot.reply_to(message, "❌ အရင်ဆုံး အကောင့်ဝင်ပါ။")
        return
    args = message.text.split(maxsplit=

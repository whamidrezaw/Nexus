"""
NewsRadar v7.2 - Enterprise Edition
Features: Zero-Copy Media (Instant), Smart Queue, Auto-Cleaning
"""

import os
import asyncio
import logging
import re
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import motor.motor_asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# وب‌سرور برای زنده نگه داشتن در Render
try:
    from web_server import keep_alive
except ImportError:
    def keep_alive(): pass

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
@dataclass(frozen=True)
class Config:
    API_ID: int
    API_HASH: str
    STRING_SESSION: str
    TARGET_CHANNEL: str
    MONGO_URI: str
    
    # تنظیمات هوشمند
    MAX_QUEUE_SIZE: int = 200        # افزایش ظرفیت صف
    DUPLICATE_TTL: int = 86400 * 3   # حافظه تکراری‌ها (3 روز)
    
    NEWS_CHANNELS: tuple = (
        "BBCPersian", "RadioFarda", "Tasnimnews", 
        "deutsch_news1", "khabarfuri", "KHABAREROOZ_IR"
    )
    
    PROXY_CHANNELS: tuple = (
        "iProxyem", "Proxymelimon", "famoushaji", 
        "V2rrayVPN", "napsternetv", "v2rayng_vpn"
    )

    BLACKLIST: tuple = (
        "@deutsch_news1", "deutsch_news1", "Deutsch_News1",
        "@radiofarda_official", "radiofarda_official", "RadioFarda",
        "@BBCPersian", "BBCPersian", "bbcpersian", "BBC",
        "Tasnimnews", "@TasnimNews", "خبرگزاری تسنیم",
        "@KhabarFuri", "KhabarFuri", "khabarfuri", "خبر فوری",
        "KHABAREROOZ_IR", "@KHABAREROOZ_IR", "khabarerooz_ir",
        "عضو شوید", "لینک عضویت", "join", "Join",
        "تبلیغ", "vpn", "VPN", "proxy", "فیلترشکن",
        "اینستاگرام", "youtube", "twitter", "http", "www.",
        "@", "🆔", "👇", "👉", "pv", "PV"


          "@", "🆔", "سایت تسنیم را در آدرس زیر ببینید :", "👉", "pv", "سایت تسنیم را در آدرس زیر ببینید:"
    )
    
    SIG_NEWS = "\n\n📡 <b>رادار اخبار</b>\n🆔 @NewsRadar_hub"
    SIG_PROXY = "\n\n🔐 <b>کانفیگ اختصاصی</b>\n🆔 @NewsRadar_hub"

    @classmethod
    def from_env(cls):
        return cls(
            API_ID=int(os.getenv("TELEGRAM_API_ID", "0")),
            API_HASH=os.getenv("TELEGRAM_API_HASH", ""),
            STRING_SESSION=os.getenv("STRING_SESSION", ""),
            TARGET_CHANNEL=os.getenv("TARGET_CHANNEL", ""),
            MONGO_URI=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        )

# ============================================================================
# 2. LOGGING
# ============================================================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("NewsRadar-v7.2")

# ============================================================================
# 3. CONTENT ENGINE
# ============================================================================
class ContentEngine:
    # رجکس بهبود یافته برای تشخیص دقیق پروتکل‌ها
    PROXY_PATTERN = re.compile(r'(vmess|vless|trojan|ss|tuic|hysteria2?)://[a-zA-Z0-9\-_@:/?=&%.#]+')
    MENTION_CLEANER = re.compile(r'@[a-zA-Z0-9_]+')

    @staticmethod
    def get_content_hash(text: str) -> str:
        if not text: return "empty"
        normalized = re.sub(r'\s+', '', text.lower().strip())
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

    @classmethod
    def extract_proxies(cls, text: str) -> list:
        if not text: return []
        # جستجوی تمام کانفیگ‌ها
        configs = cls.PROXY_PATTERN.findall(text)
        # فیلتر کردن موارد ناقص
        valid_configs = [c.strip() for c in configs if len(c) > 20]
        return list(set(valid_configs))

    @classmethod
    def clean_news(cls, text: str, blacklist: tuple) -> str:
        if not text: return None
        
        # 1. حذف عبارات بلک‌لیست
        for bad in blacklist:
            if bad in text:
                text = text.replace(bad, "")

        # 2. حذف منشن‌ها
        text = cls.MENTION_CLEANER.sub('', text)
        
        # 3. نرمال‌سازی خطوط
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        
        if len(text) < 25: return None
        return text

    @staticmethod
    def get_emoji(text: str) -> str:
        t = text.lower()
        if any(x in t for x in ['فوری', 'urgent']): return '🔴'
        if any(x in t for x in ['اقتصاد', 'دلار', 'طلا']): return '💰'
        if any(x in t for x in ['جنگ', 'حمله', 'war']): return '⚔️'
        if any(x in t for x in ['ورزش', 'فوتبال']): return '⚽️'
        return '📰'

# ============================================================================
# 4. DATABASE
# ============================================================================
class Database:
    def __init__(self, uri: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self.client.newsradar_v7
        self.history = self.db.history

    async def initialize(self):
        await self.history.create_index("created_at", expireAfterSeconds=Config.DUPLICATE_TTL)
        await self.history.create_index("content_hash", unique=True)

    async def is_duplicate(self, content_hash: str) -> bool:
        return await self.history.find_one({"content_hash": content_hash}) is not None

    async def save(self, content_hash: str, source: str):
        try:
            await self.history.insert_one({
                "content_hash": content_hash,
                "source": source,
                "created_at": datetime.now(timezone.utc)
            })
        except: pass

# ============================================================================
# 5. QUEUE WORKER (The Publisher)
# ============================================================================
class QueueWorker:
    def __init__(self, client: TelegramClient, config: Config):
        self.client = client
        self.config = config
        self.queue = asyncio.Queue(maxsize=config.MAX_QUEUE_SIZE)

    async def add_news(self, msg_obj, clean_text, source):
        # ما فقط آبجکت پیام را ذخیره میکنیم، نه فایل را (صرفه جویی در رم)
        await self.queue.put({
            'type': 'news',
            'msg_obj': msg_obj,
            'text': clean_text,
            'source': source
        })

    async def add_proxy(self, config_text, source):
        await self.queue.put({
            'type': 'proxy',
            'config': config_text,
            'source': source
        })

    async def start(self):
        logger.info("👷 Worker Started & Ready...")
        while True:
            item = await self.queue.get()
            try:
                if item['type'] == 'news':
                    await self._publish_news(item)
                elif item['type'] == 'proxy':
                    await self._publish_proxy(item)
                
                # جلوگیری از FloodWait
                await asyncio.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"Publish Error: {e}")
            finally:
                self.queue.task_done()

    async def _publish_news(self, item):
        text = item['text']
        source = item['source']
        msg_obj = item['msg_obj'] # پیام اصلی تلگرام
        
        emoji = ContentEngine.get_emoji(text)
        header = text.split('\n')[0]
        body = '\n'.join(text.split('\n')[1:])
        caption = f"<b>{emoji} {header}</b>\n\n{body}{self.config.SIG_NEWS}"

        # نکته طلایی: استفاده از msg_obj.media برای کپی مستقیم بدون دانلود
        if msg_obj.media:
            await self.client.send_message(
                self.config.TARGET_CHANNEL,
                message=caption,
                file=msg_obj.media, # تلگرام خودش مدیا را کپی می‌کند
                parse_mode='html'
            )
        else:
            await self.client.send_message(
                self.config.TARGET_CHANNEL,
                caption,
                parse_mode='html',
                link_preview=False
            )
        logger.info(f"✅ News Sent (Src: {source})")

    async def _publish_proxy(self, item):
        conf = item['config']
        txt = f"🔑 <b>Connect to Freedom</b>\n\n<code>{conf}</code>{self.config.SIG_PROXY}"
        await self.client.send_message(
            self.config.TARGET_CHANNEL,
            txt,
            parse_mode='html',
            link_preview=False
        )
        logger.info(f"✅ Proxy Sent (Src: {item['source']})")

# ============================================================================
# 6. MAIN LOGIC
# ============================================================================
async def process_message(message, source, db: Database, worker: QueueWorker, config: Config):
    """تابع مرکزی پردازش پیام (هم برای Backfill هم Realtime)"""
    text = message.text or ""
    
    # 1. پردازش پروکسی
    if source in config.PROXY_CHANNELS:
        proxies = ContentEngine.extract_proxies(text)
        for conf in proxies:
            h = ContentEngine.get_content_hash(conf)
            if not await db.is_duplicate(h):
                await db.save(h, source)
                await worker.add_proxy(conf, source)

    # 2. پردازش خبر
    elif source in config.NEWS_CHANNELS:
        clean_text = ContentEngine.clean_news(text, config.BLACKLIST)
        if clean_text:
            h = ContentEngine.get_content_hash(clean_text)
            if not await db.is_duplicate(h):
                await db.save(h, source)
                # کل آبجکت پیام را به ورکر میدهیم
                await worker.add_news(message, clean_text, source)

async def main():
    config = Config.from_env()
    db = Database(config.MONGO_URI)
    await db.initialize()
    
    client = TelegramClient(StringSession(config.STRING_SESSION), config.API_ID, config.API_HASH)
    worker = QueueWorker(client, config)
    
    await client.start()
    
    # ⚡️ 1. اجرای Worker قبل از هر کاری
    asyncio.create_task(worker.start())

    # ⏳ 2. بخش Backfill (یک ساعت گذشته)
    logger.info("⏳ Starting Backfill...")
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    all_channels = config.NEWS_CHANNELS + config.PROXY_CHANNELS
    
    for channel in all_channels:
        try:
            async for msg in client.iter_messages(channel, offset_date=one_hour_ago, reverse=True):
                await process_message(msg, channel, db, worker, config)
            await asyncio.sleep(1) # استراحت بین کانال‌ها
        except Exception as e:
            logger.error(f"Backfill error on {channel}: {e}")
            
    logger.info("✅ Backfill Done. Listening for new messages...")

    # 📡 3. بخش Real-time
    @client.on(events.NewMessage(chats=all_channels))
    async def handler(event):
        try:
            chat = await event.get_chat()
            source = chat.username or chat.title
            await process_message(event.message, source, db, worker, config)
        except Exception as e:
            logger.error(f"Handler Error: {e}")

    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt: pass
    except Exception as e: logger.critical(f"Fatal: {e}")

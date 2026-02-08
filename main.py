"""
NewsRadar v7.0 - Hybrid Free Edition
Powered by Asyncio Queues & Smart Deduplication
"""

import os
import sys
import time
import asyncio
import logging
import re
import html
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Set

import motor.motor_asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon import errors

# برای زنده نگه داشتن سرور (اگر وب‌سرور دارید)
try:
    from web_server import keep_alive
except ImportError:
    def keep_alive(): pass

# ============================================================================
# 1. CONFIGURATION (تنظیمات)
# ============================================================================
@dataclass(frozen=True)
class Config:
    API_ID: int
    API_HASH: str
    STRING_SESSION: str
    TARGET_CHANNEL: str
    MONGO_URI: str
    
    # تنظیمات هوشمند
    MAX_QUEUE_SIZE: int = 100       # ظرفیت صف داخلی
    DUPLICATE_TTL: int = 86400 * 3  # حافظه تکراری‌ها (3 روز)
    
    # کانال‌ها
    NEWS_CHANNELS: tuple = (
        "BBCPersian", "RadioFarda", "Tasnimnews", 
        "deutsch_news1", "khabarfuri", "KHABAREROOZ_IR", "euronewspe"
    )
    
    PROXY_CHANNELS: tuple = (
        "iProxyem", "Proxymelimon", "famoushaji", 
        "V2rrayVPN", "napsternetv", "v2rayng_vpn", "v2rayng_org"
    )

    BLACKLIST: tuple = (
        "عضو شوید", "join", "تبلیغ", "رزرو", "bet", "سایت", "کلیک کنید",
        "https://t.me", "@", "insta", "youtube"
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
# 2. ADVANCED LOGGING (لاگ‌گیری حرفه‌ای)
# ============================================================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("NewsRadar-v7")

# ============================================================================
# 3. SMART LOGIC (مغز متفکر)
# ============================================================================
class ContentEngine:
    """موتور پردازش محتوا با الگوریتم‌های نسخه Enterprise"""
    
    # رجکس‌های پیشرفته برای استخراج دقیق
    PROXY_PATTERN = re.compile(r'(vmess|vless|trojan|ss)://[a-zA-Z0-9\-_@:/?=&%.]+')
    URL_CLEANER = re.compile(r'https?://\S+')
    MENTION_CLEANER = re.compile(r'@[a-zA-Z0-9_]+')

    @staticmethod
    def get_content_hash(text: str) -> str:
        """ساخت اثر انگشت یکتا برای محتوا (جلوگیری از تکرار هوشمند)"""
        # نرمال‌سازی: حذف فاصله‌های اضافه و کوچک کردن حروف
        normalized = re.sub(r'\s+', '', text.lower().strip())
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

    @classmethod
    def process_proxy(cls, text: str) -> List[str]:
        """استخراج کانفیگ‌های سالم"""
        if not text: return []
        configs = cls.PROXY_PATTERN.findall(text)
        # حذف کانفیگ‌های ناقص یا خیلی کوتاه
        valid_configs = [c for c in configs if len(c) > 50]
        # حذف تکراری‌ها در یک پیام
        return list(set(valid_configs))

    @classmethod
    def process_news(cls, text: str, blacklist: tuple) -> Optional[str]:
        """تمیزکاری متن خبر"""
        if not text: return None
        
        # حذف کلمات بلک‌لیست
        for bad in blacklist:
            if bad in text:
                text = text.replace(bad, "")

        # حذف لینک‌ها و منشن‌ها
        text = cls.MENTION_CLEANER.sub('', text)
        
        # تمیزکاری نهایی
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        
        if len(text) < 30: return None  # خبرهای خیلی کوتاه ارزش ندارند
        return text

    @staticmethod
    def detect_topic(text: str) -> str:
        """تشخیص موضوع برای اموجی هوشمند"""
        t = text.lower()
        if any(x in t for x in ['فوری', 'breaking', 'urgent']): return '🔴'
        if any(x in t for x in ['اقتصاد', 'دلار', 'طلا']): return '💰'
        if any(x in t for x in ['جنگ', 'حمله', 'war']): return '⚔️'
        if any(x in t for x in ['تکنولوژی', 'ai', 'tech']): return '🤖'
        return '📰'

# ============================================================================
# 4. DATABASE & MEMORY (حافظه بلند مدت)
# ============================================================================
class Database:
    def __init__(self, uri: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self.client.newsradar_v7
        self.history = self.db.history

    async def initialize(self):
        # ساخت ایندکس برای حذف خودکار رکوردهای قدیمی (TTL)
        await self.history.create_index("created_at", expireAfterSeconds=Config.DUPLICATE_TTL)
        await self.history.create_index("content_hash", unique=True)

    async def is_duplicate(self, content_hash: str) -> bool:
        """بررسی سریع در دیتابیس"""
        found = await self.history.find_one({"content_hash": content_hash})
        return found is not None

    async def save_hash(self, content_hash: str, source: str):
        """ذخیره هش برای آینده"""
        try:
            await self.history.insert_one({
                "content_hash": content_hash,
                "source": source,
                "created_at": datetime.now(timezone.utc)
            })
        except Exception:
            pass  # اگر تکراری بود و همزمان ثبت شد، مشکلی نیست

# ============================================================================
# 5. WORKER SYSTEM (سیستم صف و انتشار)
# ============================================================================
class QueueWorker:
    def __init__(self, client: TelegramClient, config: Config, db: Database):
        self.client = client
        self.config = config
        self.db = db
        self.queue = asyncio.Queue(maxsize=config.MAX_QUEUE_SIZE)
        
    async def add_task(self, task_type: str, data: dict):
        """اضافه کردن به صف (بدون مسدود کردن برنامه)"""
        try:
            self.queue.put_nowait((task_type, data))
        except asyncio.QueueFull:
            logger.warning("Queue is full! Dropping oldest item.")
            try:
                self.queue.get_nowait()
                self.queue.put_nowait((task_type, data))
            except: pass

    async def start_consumer(self):
        """مصرف‌کننده صف (Publisher)"""
        logger.info("👷 Worker started processing queue...")
        
        while True:
            # دریافت از صف
            task_type, data = await self.queue.get()
            
            try:
                if task_type == 'proxy':
                    await self._publish_proxy(data)
                elif task_type == 'news':
                    await self._publish_news(data)
                
                # استراحت هوشمند (جلوگیری از FloodWait)
                await asyncio.sleep(random.uniform(2.0, 4.0))
                
            except Exception as e:
                logger.error(f"Publish Error: {e}")
            finally:
                self.queue.task_done()

    async def _publish_proxy(self, data):
        config = data['config']
        # فرمت شیک برای کپی کردن
        msg = f"🔑 <b>Connect to Freedom</b>\n\n<code>{config}</code>{self.config.SIG_PROXY}"
        await self.client.send_message(
            self.config.TARGET_CHANNEL, 
            msg, 
            parse_mode='html', 
            link_preview=False
        )
        logger.info(f"✅ Proxy Published (Source: {data['source']})")

    async def _publish_news(self, data):
        text = data['text']
        media = data.get('media')
        emoji = ContentEngine.detect_topic(text)
        
        # فرمت خبر
        header = text.split('\n')[0]
        body = '\n'.join(text.split('\n')[1:])
        formatted_text = f"<b>{emoji} {header}</b>\n\n{body}{self.config.SIG_NEWS}"
        
        if media:
            await self.client.send_file(
                self.config.TARGET_CHANNEL,
                media,
                caption=formatted_text,
                parse_mode='html'
            )
        else:
            await self.client.send_message(
                self.config.TARGET_CHANNEL,
                formatted_text,
                parse_mode='html',
                link_preview=False
            )
        logger.info(f"📰 News Published (Source: {data['source']})")

# ============================================================================
# 6. MAIN CONTROLLER (کنترل‌کننده اصلی)
# ============================================================================
async def main():
    config = Config.from_env()
    
    # اتصال به دیتابیس
    db = Database(config.MONGO_URI)
    await db.initialize()
    
    # راه اندازی کلاینت تلگرام
    client = TelegramClient(
        StringSession(config.STRING_SESSION),
        config.API_ID,
        config.API_HASH
    )
    
    # راه اندازی ورکر
    worker = QueueWorker(client, config, db)

    @client.on(events.NewMessage(chats=config.NEWS_CHANNELS + config.PROXY_CHANNELS))
    async def handler(event):
        try:
            chat = await event.get_chat()
            channel_name = chat.username or chat.title
            text = event.message.text or ""
            
            # --- حالت پروکسی ---
            if channel_name in config.PROXY_CHANNELS:
                configs = ContentEngine.process_proxy(text)
                for conf in configs:
                    # تولید هش از خود کانفیگ (جلوگیری از کانفیگ تکراری)
                    conf_hash = ContentEngine.get_content_hash(conf)
                    
                    if not await db.is_duplicate(conf_hash):
                        await db.save_hash(conf_hash, channel_name)
                        await worker.add_task('proxy', {'config': conf, 'source': channel_name})
            
            # --- حالت خبر ---
            elif channel_name in config.NEWS_CHANNELS:
                clean_text = ContentEngine.process_news(text, config.BLACKLIST)
                if clean_text:
                    # تولید هش از متن تمیز شده (اگر دو کانال یک خبر را بگذارند، دومی حذف می‌شود)
                    news_hash = ContentEngine.get_content_hash(clean_text)
                    
                    if not await db.is_duplicate(news_hash):
                        await db.save_hash(news_hash, channel_name)
                        
                        media = None
                        if event.message.media:
                            # دانلود مدیا فقط اگر تکراری نبود
                            media = await event.message.download_media(file=bytes)
                        
                        await worker.add_task('news', {
                            'text': clean_text, 
                            'media': media, 
                            'source': channel_name
                        })

        except Exception as e:
            logger.error(f"Handler Error: {e}")

    # شروع برنامه
    await client.start()
    logger.info("🚀 NewsRadar v7.0 (Hybrid) Started!")
    
    # اجرای همزمان مصرف‌کننده صف
    asyncio.create_task(worker.start_consumer())
    
    # اجرای مداوم
    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f"Fatal Error: {e}")

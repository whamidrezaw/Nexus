"""
NewsRadar v6.6 - Smart Mixer
Randomized Execution • Smart Filtering • Dual Signature
"""

import os
import sys
import time
import asyncio
import random
import logging
import signal
import re
import html
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Deque
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import motor.motor_asyncio
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

# اضافه کردن وب‌سرور برای زنده نگه داشتن
from web_server import keep_alive


# ============================================================================
# CONFIGURATION
# ============================================================================
@dataclass(frozen=True)
class Config:
    # --- تنظیمات اصلی ---
    API_ID: int
    API_HASH: str
    STRING_SESSION: str
    TARGET_CHANNEL: str
    MONGO_URI: str
    
    # --- تنظیمات فنی ---
    CYCLE_MIN: int = 120   # حداقل صبر بین چرخه (ثانیه)
    CYCLE_MAX: int = 300   # حداکثر صبر بین چرخه
    MAX_CACHE: int = 5000
    MAX_MEDIA_MB: int = 20
    
    # --- لیست کانال‌های خبری (News) ---
    NEWS_CHANNELS: tuple = (
        "BBCPersian", "RadioFarda", "Tasnimnews", 
        "deutsch_news1", "khabarfuri", "KHABAREROOZ_IR"
    )

    # --- لیست کانال‌های پروکسی (Proxy) ---
    PROXY_CHANNELS: tuple = (
        "iProxyem", "Proxymelimon", "famoushaji", 
        "V2rrayVPN", "napsternetv"
    )

    # --- لیست سیاه (کلمات حذفی) ---
    BLACKLIST: tuple = (
        "@deutsch_news1", "deutsch_news1", "آخرین اخبارفوری آلمان",
        "@radiofarda_official", "radiofarda_official", "RadioFarda", "@RadioFarda",
        "@BBCPersian", "BBCPersian",
        "Tasnimnews", "@TasnimNews",
        "@KhabarFuri", "KhabarFuri", "KhabarFuri | اخبار",
        "🔴@KHABAREROOZ_IR", "@KHABAREROOZ_IR", "KHABAREROOZ_IR",
        "https://www.TasnimNews.ir", "www.TasnimNews.ir",
        "سایت تسنیم را در آدرس زیر ببینید:", "▪️سایت تسنیم را در آدرس زیر ببینید:",
        "#درعمق" , "درعمق", 
        "عضو شوید", "join", "لینک عضویت", "کلیک کنید",
        "📷", "▪️", "@"  # علامت @ را حذف میکند تا تبلیغ نشود
    )

    # --- امضاها (دقیقاً طبق خواسته شما) ---
    SIG_NEWS = "\n\n📡 <b>رادار هوشمند اخبار جهان</b>\n🆔 @NewsRadar_hub"
    SIG_PROXY = "\n\n🔐 <b>کانفیگ اختصاصی | اتصال امن</b>\n🆔 @NewsRadar_hub"
    
    @classmethod
    def from_env(cls):
        api_id = os.getenv("TELEGRAM_API_ID")
        if not api_id or not api_id.isdigit():
            raise ValueError("TELEGRAM_API_ID must be numeric")
        
        return cls(
            API_ID=int(api_id),
            API_HASH=os.getenv("TELEGRAM_API_HASH", ""),
            STRING_SESSION=os.getenv("STRING_SESSION", ""),
            TARGET_CHANNEL=os.getenv("TARGET_CHANNEL", ""),
            MONGO_URI=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        )


# ============================================================================
# LOGGER
# ============================================================================
def setup_logger():
    logger = logging.getLogger("newsradar")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    return logger

logger = setup_logger()


# ============================================================================
# RATE LIMITER
# ============================================================================
class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def consume(self, tokens: float = 1.0) -> float:
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            else:
                deficit = tokens - self.tokens
                self.tokens = 0.0
                return deficit / self.rate


# ============================================================================
# MEMORY MANAGER
# ============================================================================
class MemoryManager:
    def __init__(self, mongo_uri: str, max_size: int):
        self.max_size = max_size
        self.cache: Dict[str, float] = {}
        self.lru: Deque[str] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()
        
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            mongo_uri,
            serverSelectionTimeoutMS=3000,
            maxPoolSize=10
        )
        self.db = self.client.newsradar.posts
    
    async def setup(self):
        await self.db.create_index("id", unique=True)
        await self.db.create_index("created_at", expireAfterSeconds=30 * 24 * 3600)
        
        cursor = self.db.find({}, {"id": 1}).sort("created_at", -1).limit(self.max_size)
        async for doc in cursor:
            await self._add_to_cache(doc["id"])
        
        logger.info(f"Memory ready: {len(self.cache)} items")
    
    async def _add_to_cache(self, item_id: str):
        async with self._lock:
            if item_id in self.cache:
                self.lru.remove(item_id)
                self.lru.append(item_id)
            else:
                if len(self.cache) >= self.max_size:
                    oldest = self.lru.popleft()
                    if oldest in self.cache: del self.cache[oldest]
                self.cache[item_id] = time.time()
                self.lru.append(item_id)
    
    async def seen(self, item_id: str) -> bool:
        async with self._lock:
            if item_id in self.cache:
                self.lru.remove(item_id)
                self.lru.append(item_id)
                return True
        
        exists = await self.db.find_one({"id": item_id}) is not None
        if exists:
            await self._add_to_cache(item_id)
        return exists
    
    async def mark_seen(self, item_id: str, metadata: dict = None):
        await self._add_to_cache(item_id)
        try:
            await self.db.update_one(
                {"id": item_id},
                {"$set": {
                    "id": item_id,
                    "created_at": datetime.now(timezone.utc),
                    "metadata": metadata or {}
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Persist failed: {e}")

    async def close(self):
        self.client.close()


# ============================================================================
# CONTENT PROCESSOR (Blacklist + Formatter)
# ============================================================================
class ContentProcessor:
    PATTERNS = {
        'url': re.compile(r'https?://[^\s]+|www\.[^\s]+'),
        'mention': re.compile(r'@[a-zA-Z0-9_]+'),
        'whitespace': re.compile(r'\s+'),
    }

    @classmethod
    def clean(cls, text: str, blacklist: tuple, is_proxy: bool) -> Optional[str]:
        if not text: return None
        
        # 1. حذف کلمات لیست سیاه (با دقت بالا)
        for bad_word in blacklist:
            if bad_word in text:
                text = text.replace(bad_word, "")
            
        # 2. تمیزکاری عمومی
        if not is_proxy:
            # برای اخبار: منشن‌های باقیمانده را پاک کن
            text = cls.PATTERNS['mention'].sub(' ', text)
        else:
            # برای پروکسی: مطمئن شو لینک‌های Vmess/Vless خراب نمیشن
            pass

        # 3. نرمال‌سازی فاصله‌ها
        text = cls.PATTERNS['whitespace'].sub(' ', text).strip()
        
        # 4. بررسی طول محتوا
        min_len = 10 if is_proxy else 25
        if len(text) < min_len:
            return None
            
        return text

    @classmethod
    def format(cls, text: str, signature: str, is_proxy: bool) -> str:
        text = html.escape(text)
        
        if not is_proxy:
            # فرمت اخبار
            lines = text.split('\n')
            if lines and lines[0]:
                emoji = cls._emoji(text)
                lines[0] = f"<b>{emoji} {lines[0]}</b>"
            text = '\n'.join(lines)
        else:
            # فرمت پروکسی
            text = f"🔑 <b>Connect to Freedom</b>\n\n<code>{text}</code>"

        return f"{text}{signature}"

    @staticmethod
    def _emoji(text: str) -> str:
        t = text.lower()
        if any(w in t for w in ['جنگ', 'حمله', 'war']): return '⚔️'
        if any(w in t for w in ['انفجار', 'بمب']): return '💣'
        if any(w in t for w in ['آمریکا', 'usa']): return '🇺🇸'
        if any(w in t for w in ['ایران']): return '🇮🇷'
        if any(w in t for w in ['دلار', 'طلا', 'سکه']): return '💵'
        if any(w in t for w in ['فوری', 'عاجل', 'breaking']): return '🔴'
        return '📰'


# ============================================================================
# MEDIA HANDLER
# ============================================================================
class SafeMediaHandler:
    SUPPORTED = {'.jpg', '.jpeg', '.png', '.webp', '.gif', '.mp4', '.mov'}
    MAX_SIZE = 20 * 1024 * 1024
    
    def __init__(self, temp_dir: str = "/tmp/newsradar"):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
    
    @asynccontextmanager
    async def download(self, client, message):
        file_path = None
        try:
            if not message.media:
                yield None
                return
            
            if hasattr(message.media, 'size') and message.media.size > self.MAX_SIZE:
                yield None
                return

            ts = int(time.time() * 1000)
            file_path = self.temp_dir / f"m_{ts}_{random.randint(100,999)}"
            
            downloaded = await asyncio.wait_for(
                client.download_media(message, file=str(file_path)),
                timeout=50.0
            )
            
            if not downloaded or not Path(downloaded).exists():
                yield None
                return
                
            path = Path(downloaded)
            if path.suffix.lower() not in self.SUPPORTED:
                yield None
                return
                
            yield str(path)
            
        except Exception as e:
            logger.error(f"DL Err: {e}")
            yield None
        finally:
            if file_path and file_path.exists():
                try: file_path.unlink()
                except: pass


# ============================================================================
# MAIN BOT LOGIC (MIXER MODE)
# ============================================================================
class NewsRadarBot:
    def __init__(self, config: Config):
        self.config = config
        self.memory = MemoryManager(config.MONGO_URI, config.MAX_CACHE)
        self.processor = ContentProcessor()
        self.media = SafeMediaHandler()
        self.limiter = TokenBucket(rate=0.5, capacity=2.0)
        self.running = False
        self.stats = {'posted': 0, 'errors': 0}

    async def _handle(self, client, channel: str, message, is_proxy: bool) -> bool:
        msg_id = f"{channel}_{message.id}"
        
        if await self.memory.seen(msg_id):
            return False
        
        raw_text = message.text or ""
        
        # فیلتر و تمیزکاری
        cleaned = self.processor.clean(raw_text, self.config.BLACKLIST, is_proxy)
        
        # اگر متن بعد از تمیزکاری خالی شد و عکس هم نداشت، ولش کن
        if not cleaned and not message.media:
            return False
            
        # انتخاب امضا
        sig = self.config.SIG_PROXY if is_proxy else self.config.SIG_NEWS
        formatted = self.processor.format(cleaned or "", sig, is_proxy)
        
        # لیمیتر (جلوگیری از رگباری فرستادن)
        wait = await self.limiter.consume(1.0)
        if wait > 0: await asyncio.sleep(wait)
        
        try:
            if message.media:
                async with self.media.download(client, message) as path:
                    if path:
                        await client.send_file(
                            self.config.TARGET_CHANNEL,
                            path,
                            caption=formatted,
                            parse_mode='html'
                        )
                    else:
                        if cleaned:
                            await client.send_message(
                                self.config.TARGET_CHANNEL,
                                formatted,
                                parse_mode='html',
                                link_preview=False
                            )
            else:
                await client.send_message(
                    self.config.TARGET_CHANNEL,
                    formatted,
                    parse_mode='html',
                    link_preview=False
                )
            
            await self.memory.mark_seen(msg_id, {'type': 'proxy' if is_proxy else 'news'})
            self.stats['posted'] += 1
            logger.info(f"✅ Posted from {channel} [{'PROXY' if is_proxy else 'NEWS'}]")
            return True

        except Exception as e:
            logger.error(f"Send Error: {e}")
            self.stats['errors'] += 1
            return False

    async def run(self):
        self.running = True
        await self.memory.setup()
        
        # --- میکسر کانال‌ها ---
        # همه کانال‌ها را در یک لیست واحد می‌ریزیم
        # ساختار: (اسم_کانال, آیا_پروکسی_است؟)
        
        all_targets = []
        
        # اضافه کردن اخبار
        for ch in self.config.NEWS_CHANNELS:
            all_targets.append((ch, False))
            
        # اضافه کردن پروکسی‌ها
        for ch in self.config.PROXY_CHANNELS:
            all_targets.append((ch, True))
            
        logger.info(f"Target Pool: {len(all_targets)} sources")

        async with TelegramClient(
            StringSession(self.config.STRING_SESSION),
            self.config.API_ID,
            self.config.API_HASH
        ) as client:
            
            logger.info("Bot Online & Connected 🚀")
            
            while self.running:
                # 🎲 شافل کردن (مخلوط کردن) لیست در ابتدای هر دور
                # این خط جادویی است که باعث می‌شود نظم به هم بریزد
                random.shuffle(all_targets)
                
                for channel_name, is_proxy in all_targets:
                    if not self.running: break
                    
                    try:
                        # از هر کانال فقط 2 پیام آخر را چک کن
                        # این باعث می‌شود سریع بین کانال‌ها جابجا شود (میکسر)
                        async for msg in client.iter_messages(channel_name, limit=2):
                            if not self.running: break
                            
                            processed = await self._handle(client, channel_name, msg, is_proxy)
                            if processed:
                                # اگر پستی ارسال شد، کمی صبر کن تا طبیعی به نظر برسد
                                await asyncio.sleep(random.uniform(2, 4))
                            
                    except Exception as e:
                        logger.error(f"Error reading {channel_name}: {e}")
                    
                    # مکث کوتاه بین سوئیچ کردن کانال‌ها
                    await asyncio.sleep(random.uniform(3, 6))

                # پایان یک دور کامل
                logger.info(f"Cycle finished. Total Posted: {self.stats['posted']}")
                # استراحت طولانی قبل از شروع دور بعدی
                await asyncio.sleep(random.randint(self.config.CYCLE_MIN, self.config.CYCLE_MAX))


# ============================================================================
# ENTRY POINT
# ============================================================================
async def main():
    try:
        config = Config.from_env()
    except Exception as e:
        logger.error(f"Config Error: {e}")
        return
        
    bot = NewsRadarBot(config)
    try:
        await bot.run()
    except Exception as e:
        logger.critical(f"Fatal: {e}")

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

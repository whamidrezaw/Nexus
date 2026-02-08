"""
NewsRadar v6.7 - Surgical Proxy Extractor
News + Pure Configs • Zero Junk • Protocol Enforcer
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
from typing import Optional, Dict, Deque, List
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import motor.motor_asyncio
from telethon import TelegramClient, errors
from telethon.sessions import StringSession

# اضافه کردن وب‌سرور
from web_server import keep_alive


# ============================================================================
# CONFIGURATION
# ============================================================================
@dataclass(frozen=True)
class Config:
    API_ID: int
    API_HASH: str
    STRING_SESSION: str
    TARGET_CHANNEL: str
    MONGO_URI: str
    
    CYCLE_MIN: int = 60    # بررسی سریع‌تر
    CYCLE_MAX: int = 180
    MAX_CACHE: int = 5000
    MAX_MEDIA_MB: int = 20
    
    # --- لیست کانال‌های خبری ---
    NEWS_CHANNELS: tuple = (
        "BBCPersian", "RadioFarda", "Tasnimnews", 
        "deutsch_news1", "khabarfuri", "KHABAREROOZ_IR"
    )

    # --- لیست کانال‌های پروکسی ---
    PROXY_CHANNELS: tuple = (
        "iProxyem", "Proxymelimon", "famoushaji", 
        "V2rrayVPN", "napsternetv", "v2rayng_vpn"
    )

    # --- لیست سیاه (فقط برای اخبار) ---
    BLACKLIST: tuple = (
        "@deutsch_news1", "deutsch_news1", 
        "@radiofarda_official", "radiofarda_official", "RadioFarda",
        "@BBCPersian", "BBCPersian",
        "Tasnimnews", "@TasnimNews",
        "@KhabarFuri", "KhabarFuri",
        "🔴@KHABAREROOZ_IR", "@KHABAREROOZ_IR",
        "https://www.TasnimNews.ir",
        "عضو شوید", "join", "لینک عضویت", "کلیک کنید", "▪️", "@"
    )

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
# LOGGER & LIMITER
# ============================================================================
def setup_logger():
    logger = logging.getLogger("newsradar")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    return logger

logger = setup_logger()

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
# MEMORY
# ============================================================================
class MemoryManager:
    def __init__(self, mongo_uri: str, max_size: int):
        self.max_size = max_size
        self.cache: Dict[str, float] = {}
        self.lru: Deque[str] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri, serverSelectionTimeoutMS=3000)
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
        if exists: await self._add_to_cache(item_id)
        return exists
    
    async def mark_seen(self, item_id: str, metadata: dict = None):
        await self._add_to_cache(item_id)
        try:
            await self.db.update_one(
                {"id": item_id},
                {"$set": {"id": item_id, "created_at": datetime.now(timezone.utc), "metadata": metadata or {}}},
                upsert=True
            )
        except Exception: pass

    async def close(self):
        self.client.close()


# ============================================================================
# CONTENT PROCESSOR (The Extractor Engine)
# ============================================================================
class ContentProcessor:
    # 1. رجکس دقیق برای پیدا کردن کانفیگ‌ها
    # این الگو می‌گوید: شروع با یکی از پروتکل‌ها و ادامه تا رسیدن به فضای خالی (Whitespace)
    PROXY_PATTERN = re.compile(r'(?:vmess|vless|trojan|ss)://\S+')
    
    PATTERNS = {
        'url': re.compile(r'https?://[^\s]+|www\.[^\s]+'),
        'mention': re.compile(r'@[a-zA-Z0-9_]+'),
        'whitespace': re.compile(r'\s+'),
    }

    @classmethod
    def extract_configs(cls, text: str) -> List[str]:
        """فقط و فقط کانفیگ‌ها را از متن بیرون می‌کشد"""
        if not text:
            return []
        # پیدا کردن تمام کانفیگ‌های موجود در متن
        return cls.PROXY_PATTERN.findall(text)

    @classmethod
    def clean_news(cls, text: str, blacklist: tuple) -> Optional[str]:
        """تمیزکاری متن خبر"""
        if not text: return None
        
        for bad_word in blacklist:
            if bad_word in text:
                text = text.replace(bad_word, "")
            
        text = cls.PATTERNS['mention'].sub(' ', text)
        text = cls.PATTERNS['whitespace'].sub(' ', text).strip()
        
        if len(text) < 25: return None
        return text

    @classmethod
    def format_news(cls, text: str, signature: str) -> str:
        text = html.escape(text)
        lines = text.split('\n')
        if lines and lines[0]:
            emoji = cls._emoji(text)
            lines[0] = f"<b>{emoji} {lines[0]}</b>"
        return f"{'\n'.join(lines)}{signature}"

    @classmethod
    def format_proxy(cls, config: str, signature: str) -> str:
        # کانفیگ را داخل تگ Code می‌گذاریم تا با یک کلیک کپی شود
        # هیچ متن اضافه‌ای اینجا نیست
        return f"🔑 <b>Connect to Freedom</b>\n\n<code>{config}</code>{signature}"

    @staticmethod
    def _emoji(text: str) -> str:
        t = text.lower()
        if any(w in t for w in ['جنگ', 'حمله', 'war']): return '⚔️'
        if any(w in t for w in ['انفجار', 'بمب']): return '💣'
        if any(w in t for w in ['فوری', 'عاجل']): return '🔴'
        return '📰'


# ============================================================================
# MEDIA HANDLER
# ============================================================================
class SafeMediaHandler:
    SUPPORTED = {'.jpg', '.jpeg', '.png', '.webp', '.mp4'}
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
            
            ts = int(time.time() * 1000)
            file_path = self.temp_dir / f"m_{ts}_{random.randint(100,999)}"
            
            downloaded = await asyncio.wait_for(
                client.download_media(message, file=str(file_path)),
                timeout=40.0
            )
            
            if not downloaded: yield None; return
            path = Path(downloaded)
            if path.suffix.lower() not in self.SUPPORTED: yield None; return
            yield str(path)
            
        except Exception: yield None
        finally:
            if file_path and file_path.exists():
                try: file_path.unlink()
                except: pass


# ============================================================================
# MAIN BOT LOGIC
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
        
        # 1. بررسی تکراری بودن
        if await self.memory.seen(msg_id):
            return False
        
        raw_text = message.text or ""

        # ==========================
        # 2. منطق اختصاصی پروکسی
        # ==========================
        if is_proxy:
            # فقط و فقط کانفیگ‌ها را بکش بیرون
            configs = self.processor.extract_configs(raw_text)
            
            # اگر هیچ کانفیگی پیدا نکردی، پیام را کاملاً نادیده بگیر (حتی اگر عکس داشت)
            if not configs:
                return False
            
            # اگر کانفیگ پیدا شد، به تعداد کانفیگ‌ها پست بگذار
            for conf in configs:
                formatted = self.processor.format_proxy(conf, self.config.SIG_PROXY)
                
                # کنترل سرعت
                wait = await self.limiter.consume(1.0)
                if wait > 0: await asyncio.sleep(wait)
                
                try:
                    await client.send_message(
                        self.config.TARGET_CHANNEL,
                        formatted,
                        parse_mode='html',
                        link_preview=False
                    )
                    self.stats['posted'] += 1
                    logger.info(f"✅ Config Posted from {channel}")
                except Exception as e:
                    logger.error(f"Proxy Send Error: {e}")
            
            # برای پروکسی‌ها نیازی به مارک کردن مدیا نیست، چون مدیا دانلود نمیکنیم
            await self.memory.mark_seen(msg_id, {'type': 'proxy', 'count': len(configs)})
            return True

        # ==========================
        # 3. منطق اختصاصی اخبار
        # ==========================
        else:
            cleaned = self.processor.clean_news(raw_text, self.config.BLACKLIST)
            
            # اگر متن خبری بعد از تمیزکاری خالی شد و عکس هم نداشت، ولش کن
            if not cleaned and not message.media:
                return False
            
            formatted = self.processor.format_news(cleaned or "", self.config.SIG_NEWS)
            
            wait = await self.limiter.consume(1.0)
            if wait > 0: await asyncio.sleep(wait)
            
            try:
                # برای اخبار، مدیا دانلود می‌شود
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
                            # اگر مدیا فیل شد ولی متن دارد
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
                
                await self.memory.mark_seen(msg_id, {'type': 'news'})
                self.stats['posted'] += 1
                logger.info(f"📰 News Posted from {channel}")
                return True
                
            except Exception as e:
                logger.error(f"News Send Error: {e}")
                return False

    async def run(self):
        self.running = True
        await self.memory.setup()
        
        # ساخت استخر مختلط
        all_targets = []
        for ch in self.config.NEWS_CHANNELS: all_targets.append((ch, False))
        for ch in self.config.PROXY_CHANNELS: all_targets.append((ch, True))
            
        logger.info(f"Pool Size: {len(all_targets)} | Mode: Surgical Extraction")

        async with TelegramClient(
            StringSession(self.config.STRING_SESSION),
            self.config.API_ID,
            self.config.API_HASH
        ) as client:
            
            logger.info("Bot Online 🚀")
            
            while self.running:
                random.shuffle(all_targets)
                
                for channel_name, is_proxy in all_targets:
                    if not self.running: break
                    
                    try:
                        # برای پروکسی‌ها پیام‌های بیشتری را چک کن تا شانس یافتن کانفیگ بیشتر شود
                        limit = 3 if is_proxy else 2
                        async for msg in client.iter_messages(channel_name, limit=limit):
                            if not self.running: break
                            
                            processed = await self._handle(client, channel_name, msg, is_proxy)
                            if processed:
                                await asyncio.sleep(random.uniform(2, 4))
                            
                    except Exception as e:
                        logger.error(f"Read Error {channel_name}: {e}")
                    
                    await asyncio.sleep(random.uniform(4, 7))

                logger.info(f"Cycle Done. Total: {self.stats['posted']}")
                await asyncio.sleep(random.randint(self.config.CYCLE_MIN, self.config.CYCLE_MAX))


async def main():
    try:
        config = Config.from_env()
    except Exception as e:
        logger.error(f"Config: {e}"); return
    
    bot = NewsRadarBot(config)
    try: await bot.run()
    except Exception as e: logger.critical(f"Fatal: {e}")

if __name__ == "__main__":
    keep_alive()
    try: asyncio.run(main())
    except KeyboardInterrupt: pass

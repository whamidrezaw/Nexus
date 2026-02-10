"""
NewsRadar v9.3 - Battle-Hardened Edition
Features: 
- DB Worker Pool (Parallel Processing)
- Global Flood Lock (Coordinated Backoff)
- Real-time Metrics (Observability)
- Integrated Discovery Pipeline (With Memory Safety)
- Robust Retry Logic & Error Handling
- Telegram Channel Logging (New Feature)
"""

import os
import asyncio
import logging
import re
import hashlib
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Set

import motor.motor_asyncio
import pymongo.errors
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage

# وب‌سرور برای زنده نگه داشتن در Render
try:
    from web_server import keep_alive
except ImportError:
    def keep_alive(): pass

# ============================================================================
# 1. LOGGING & METRICS
# ============================================================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("NewsRadar-v9.3")

# --- کلاس جدید برای ارسال لاگ به کانال تلگرام ---
class TelegramLogHandler(logging.Handler):
    """
    این کلاس لاگ‌های مهم را به کانال تلگرام ارسال می‌کند.
    """
    def __init__(self, client, chat_id):
        super().__init__()
        self.client = client
        self.chat_id = chat_id

    def emit(self, record):
        # جلوگیری از ارسال لاگ‌های خود کتابخانه Telethon (برای جلوگیری از لوپ)
        if "telethon" in record.name.lower(): return
        
        try:
            msg = self.format(record)
            # ارسال پیام به صورت Task جداگانه تا سرعت برنامه اصلی گرفته نشود
            if self.client.is_connected():
                asyncio.create_task(
                    self.client.send_message(
                        self.chat_id, 
                        f"<code>{msg}</code>", 
                        parse_mode='html'
                    )
                )
        except:
            # اگر خطایی در ارسال لاگ بود، نادیده بگیر تا برنامه متوقف نشود
            pass

# ============================================================================
# 2. CONFIGURATION
# ============================================================================
@dataclass
class Config:
    API_ID: int
    API_HASH: str
    STRING_SESSION: str
    TARGET_CHANNEL: int
    MONGO_URI: str
    
    # تنظیمات صنعتی
    DB_WORKER_COUNT: int = 3         # تعداد پردازشگرهای موازی دیتابیس
    INGEST_QUEUE_SIZE: int = 2000    # کنترل رم
    PUBLISH_QUEUE_SIZE: int = 1000
    DUPLICATE_TTL: int = 86400 * 3
    
    # === تنظیمات لاگ تلگرام ===
    # آیدی کانال لاگ خود را اینجا وارد کنید (مثال: -100123456789)
    # اکانت ربات باید در این کانال ادمین باشد
    LOG_CHANNEL_ID: int = -1003821386891  # <--- اینجا را تغییر دهید
    
    # ⚠️ مهم: شناسه (ID) عددی کانال‌های خود را اینجا وارد کنید
    NEWS_SOURCES: Dict[int, str] = field(default_factory=lambda: {
        -1001056129826: "khabarfuri", 
        -1002908786619: "GTAVFREEE", 
    })
    
    PROXY_SOURCES: Dict[int, str] = field(default_factory=lambda: {
        -1003653053311: "V2rrayVPN",
        -1002908786619: "GTAVFREEE", 
    })
    
    PROXY_FILE_EXTENSIONS: tuple = ('.npvt', '.pv', '.conf', '.ovpn')
    
    BLACKLIST: tuple = (
        "@deutsch_news1", "deutsch_news1", 
        "radiofarda_official", "Tasnimnews", "@TasnimNews",
        "@KhabarFuri", "KhabarFuri", "KHABAREROOZ_IR",
        "عضو شوید", "لینک عضویت", "join", "Join",
        "تبلیغ", "vpn", "VPN", "proxy", "فیلترشکن",
        "اینستاگرام", "youtube", "twitter", "http", "www.",
        "@", "🆔", "👇", "👉", "pv", "PV",
        "tasnimnews.ir"
    )
    
    SIG_NEWS = "\n\n📡 <b>رادار اخبار</b>\n🆔 @NewsRadar_hub"
    SIG_PROXY = "\n\n🔐 <b>کانفیگ اختصاصی</b>\n🆔 @NewsRadar_hub"

    @classmethod
    def from_env(cls):
        target = os.getenv("TARGET_CHANNEL", "")
        try: target = int(target)
        except: pass 
        
        # تلاش برای خواندن کانال لاگ از Environment Variables
        log_channel = os.getenv("LOG_CHANNEL_ID", "")
        try: log_channel = int(log_channel)
        except: log_channel = None # اگر ست نشده بود، نادیده بگیر

        return cls(
            API_ID=int(os.getenv("TELEGRAM_API_ID", "0")),
            API_HASH=os.getenv("TELEGRAM_API_HASH", ""),
            STRING_SESSION=os.getenv("STRING_SESSION", ""),
            TARGET_CHANNEL=target,
            MONGO_URI=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            LOG_CHANNEL_ID=log_channel if log_channel else cls.LOG_CHANNEL_ID
        )

# ============================================================================
# 3. CONTENT ENGINE
# ============================================================================
class ContentEngine:
# 🟢 جایگزین کن:
    PROTOCOL_PATTERN = re.compile(r'(?:vmess|vless|trojan|ss|tuic|hysteria2?|http|https)://[^\s<>"\)\]]+', re.IGNORECASE)
    MTPROTO_PATTERN = re.compile(r'https://t\.me/proxy\?[^\s<>"\)\]]+', re.IGNORECASE)
    MENTION_CLEANER = re.compile(r'@[a-zA-Z0-9_]+')

    @staticmethod
    def sanitize_text(text: str) -> str:
        if not text: return ""
        return text.replace('\n', ' ').replace('\r', '').replace('\u200c', '').strip()

    @staticmethod
    def get_content_hash(text: str) -> str:
        if not text: return "empty"
        normalized = re.sub(r'\s+', '', text.lower().strip())
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

# 🟢 جایگزین کن:
    @classmethod
    def extract_proxies(cls, raw_text: str) -> list:
        if not raw_text: return []
        sanitized_text = cls.sanitize_text(raw_text)
        results = []
        results.extend(cls.PROTOCOL_PATTERN.findall(raw_text))
        results.extend(cls.MTPROTO_PATTERN.findall(raw_text))
        results.extend(cls.PROTOCOL_PATTERN.findall(sanitized_text))
        
        # فیلتر کردن لینک‌های مزاحم
        cleaned_results = []
        for p in results:
            p = p.strip(").], ")
            if len(p) < 10: continue
            # اگر http است اما مربوط به تلگرام/اینستاگرام/توییتر است، کانفیگ نیست
            if p.lower().startswith('http'):
                if any(x in p.lower() for x in ['t.me', 'instagram.com', 'youtube.com', 'twitter.com', 'x.com']):
                    continue
            cleaned_results.append(p)
            
        return list(set(cleaned_results))

    @classmethod
    def clean_news(cls, text: str, blacklist: tuple) -> str:
        if not text: return None
        for bad in blacklist:
            if bad in text: text = text.replace(bad, "")
        text = cls.MENTION_CLEANER.sub('', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        if not text or len(text.strip()) == 0: return None
        return text

    @staticmethod
    def get_emoji(text: str) -> str:
        t = text.lower()
        if any(x in t for x in ['فوری', 'urgent']): return '🔴'
        if any(x in t for x in ['اقتصاد', 'دلار', 'طلا']): return '💰'
        if any(x in t for x in ['جنگ', 'حمله', 'war']): return '⚔️'
        return '📰'

# ============================================================================
# 4. DATABASE
# ============================================================================
class Database:
    def __init__(self, uri: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self.client.newsradar_v9
        self.history = self.db.history

    async def initialize(self):
        await self.history.create_index("created_at", expireAfterSeconds=Config.DUPLICATE_TTL)
        await self.history.create_index("content_hash", unique=True)

    async def save_if_new(self, content_hash: str, source: str) -> bool:
        try:
            await self.history.insert_one({
                "content_hash": content_hash,
                "source": source,
                "created_at": datetime.now(timezone.utc)
            })
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

# ============================================================================
# 5. PIPELINE ARCHITECTURE (The Engine)
# ============================================================================
class PipelineManager:
    def __init__(self, client: TelegramClient, config: Config, db: Database):
        self.client = client
        self.config = config
        self.db = db
        
        # Queues
        self.ingest_queue = asyncio.Queue(maxsize=config.INGEST_QUEUE_SIZE)
        self.fast_publish_queue = asyncio.Queue(maxsize=config.PUBLISH_QUEUE_SIZE)
        self.slow_publish_queue = asyncio.Queue(maxsize=config.PUBLISH_QUEUE_SIZE)
        
        # Global Flood Control
        self.global_flood_lock = asyncio.Lock()
        self.flood_cooldown = 0
        
        # Metrics
        self.metrics = {
            "ingest_in": 0, "ingest_drop": 0,
            "processed_db": 0, "published": 0,
            "discovery_log": 0, "start_time": time.time()
        }
        
        # Discovery Cache with Simple Memory Limit
        self.discovery_cache: Set[int] = set()

    # --- Ingestion (Zero Latency) ---
    async def ingest(self, payload: Dict[str, Any]):
        try:
            self.ingest_queue.put_nowait(payload)
            self.metrics["ingest_in"] += 1
        except asyncio.QueueFull:
            self.metrics["ingest_drop"] += 1
            if self.metrics["ingest_drop"] % 50 == 0:
                logger.warning(f"⚠️ DROP ALERT | Queue Full | Total: {self.metrics['ingest_drop']} | Source: {payload.get('source', 'unknown')}")

    # --- Workers Management ---
    async def start_processors(self):
        logger.info("🏭 Starting Battle-Hardened Workers...")
        
        # 1. DB Worker Pool (Parallel)
        for i in range(self.config.DB_WORKER_COUNT):
            asyncio.create_task(self._safe_runner(self._db_processor, f"DB_Worker_{i}"))
            
        # 2. Publishers
        asyncio.create_task(self._safe_runner(self._fast_publisher, "Fast_Publisher"))
        asyncio.create_task(self._safe_runner(self._slow_publisher, "Slow_Publisher"))
        
        # 3. Metrics Monitor
        asyncio.create_task(self._safe_runner(self._monitor_metrics, "Metrics_Monitor"))

    async def _safe_runner(self, func, name):
        """Immortal Runner with Jitter"""
        while True:
            try:
                await func()
            except asyncio.CancelledError: break
            except Exception as e:
                sleep_time = random.uniform(3, 8)
                logger.error(f"❌ {name} Crashed! Restarting in {sleep_time:.1f}s... Error: {e}")
                await asyncio.sleep(sleep_time)

    # --- DB Processors ---
    async def _db_processor(self):
        while True:
            item = await self.ingest_queue.get()
            try:
                if item['type'] == 'discovery':
                    chat_id = item['chat_id']
                    
                    # Memory Safety Check for Discovery Cache
                    if len(self.discovery_cache) > 1000:
                        self.discovery_cache.clear()
                        logger.info("🧹 Discovery Cache Cleared (Memory Safety)")

                    if chat_id not in self.discovery_cache:
                        logger.info(f"🔍 Discovery: {item['title']} -> ID: {chat_id}")
                        self.discovery_cache.add(chat_id)
                        self.metrics["discovery_log"] += 1
                    continue

                source = item['source']
                to_publish = []
                
                # Logic
                if item['type'] == 'raw_proxy':
                    proxies = ContentEngine.extract_proxies(item['text'])
                    for conf in proxies:
                        h = ContentEngine.get_content_hash(conf)
                        if await self.db.save_if_new(h, source):
                            to_publish.append({'type': 'proxy_text', 'content': conf, 'source': source})
                    
                    if item.get('file_name'):
                        u_id = f"{item['file_name']}_{item['file_size']}"
                        h = ContentEngine.get_content_hash(u_id)
                        if await self.db.save_if_new(h, source):
                            to_publish.append({'type': 'proxy_file', 'msg_obj': item['msg_obj'], 'source': source})

                elif item['type'] == 'raw_news':
                    clean = ContentEngine.clean_news(item['text'], self.config.BLACKLIST)
                    if clean:
                        h = ContentEngine.get_content_hash(clean)
                        if await self.db.save_if_new(h, source):
                            to_publish.append({'type': 'news', 'text': clean, 'msg_obj': item['msg_obj'], 'source': source, 'is_heavy': item.get('is_heavy')})

                # Dispatch
                for p_item in to_publish:
                    target_q = self.slow_publish_queue if (p_item.get('is_heavy') or p_item['type'] == 'proxy_file') else self.fast_publish_queue
                    await target_q.put(p_item)
                
                self.metrics["processed_db"] += 1

            except Exception as e:
                logger.error(f"DB Proc Error: {e}")
            finally:
                self.ingest_queue.task_done()

    # --- Global Flood Control & Retry ---
    async def _safe_send(self, *args, **kwargs):
        """Global Flood Aware Sender with Robust Retry"""
        async with self.global_flood_lock:
            now = time.time()
            if now < self.flood_cooldown:
                wait_time = self.flood_cooldown - now
                logger.warning(f"🌊 Global FloodWait Active: Waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

        retries = 3
        while retries > 0:
            try:
                return await self.client.send_message(*args, **kwargs)
            
            except errors.FloodWaitError as e:
                # Critical: Must respect Telegram
                logger.critical(f"🌊 GLOBAL FLOODWAIT HIT: {e.seconds}s. Locking all workers.")
                async with self.global_flood_lock:
                    self.flood_cooldown = time.time() + e.seconds + 2
                await asyncio.sleep(e.seconds + 2)
                # No retry decrement on FloodWait, we wait and retry
            
            except Exception as e:
                # Retryable Network/Server Errors
                logger.error(f"⚠️ Send Error (Retry {retries}/3): {e}")
                await asyncio.sleep(1.5)
                retries -= 1
                
        logger.error("❌ Failed to send message after retries.")
        return None

    # --- Publishers ---
    async def _fast_publisher(self):
        while True:
            item = await self.fast_publish_queue.get()
            try:
                if item['type'] == 'proxy_text':
                    txt = f"🔑 <b>Connect to Freedom</b>\n\n<code>{item['content']}</code>{self.config.SIG_PROXY}"
                    await self._safe_send(self.config.TARGET_CHANNEL, txt, parse_mode='html', link_preview=False)
                elif item['type'] == 'news':
                    await self._publish_news_item(item)
                
                self.metrics["published"] += 1
                await asyncio.sleep(random.uniform(0.8, 1.5))
            finally:
                self.fast_publish_queue.task_done()

    async def _slow_publisher(self):
        while True:
            item = await self.slow_publish_queue.get()
            try:
                if item['type'] == 'proxy_file':
                    caption = f"📁 <b>Config File</b>\nSource: {item['source']}{self.config.SIG_PROXY}"
                    await self._safe_send(self.config.TARGET_CHANNEL, message=caption, file=item['msg_obj'].media, parse_mode='html')
                elif item['type'] == 'news':
                    await self._publish_news_item(item)
                
                self.metrics["published"] += 1
                await asyncio.sleep(random.uniform(2.5, 5.0))
            finally:
                self.slow_publish_queue.task_done()

    async def _publish_news_item(self, item):
        text = item['text']
        msg_obj = item['msg_obj']
        emoji = ContentEngine.get_emoji(text)
        header = text.split('\n')[0]
        body = '\n'.join(text.split('\n')[1:])
        caption = f"<b>{emoji} {header}</b>\n\n{body}{self.config.SIG_NEWS}"
        
        valid_media = msg_obj.media and not isinstance(msg_obj.media, MessageMediaWebPage)
        if valid_media:
            await self._safe_send(self.config.TARGET_CHANNEL, message=caption, file=msg_obj.media, parse_mode='html')
        else:
            await self._safe_send(self.config.TARGET_CHANNEL, caption, parse_mode='html', link_preview=False)

    # --- Observability ---
    async def _monitor_metrics(self):
        while True:
            await asyncio.sleep(60)
            uptime = int(time.time() - self.metrics["start_time"])
            
            # Simple health check
            status = "🟢 Healthy"
            if self.metrics["ingest_drop"] > 1000: status = "🔴 High Drops"
            elif self.ingest_queue.qsize() > 1500: status = "🟡 Queue Backlog"

            logger.info(
                f"📊 STATS [{status}] (Up {uptime}s) | "
                f"IngestQ: {self.ingest_queue.qsize()} | "
                f"FastQ: {self.fast_publish_queue.qsize()} | "
                f"SlowQ: {self.slow_publish_queue.qsize()} | "
                f"Drops: {self.metrics['ingest_drop']} | "
                f"DB_Proc: {self.metrics['processed_db']} | "
                f"Pub: {self.metrics['published']}"
            )

# ============================================================================
# 6. MAIN CONTROLLER
# ============================================================================
backfill_done = asyncio.Event()

async def main():
    config = Config.from_env()
    db = Database(config.MONGO_URI)
    await db.initialize()
    
    client = TelegramClient(StringSession(config.STRING_SESSION), config.API_ID, config.API_HASH)
    pipeline = PipelineManager(client, config, db)
    
    await client.start()
    
    # ------------------------------------------------------------
    # فعال‌سازی لاگ تلگرام (بخش جدید)
    # ------------------------------------------------------------
    if config.LOG_CHANNEL_ID:
        try:
            tg_handler = TelegramLogHandler(client, config.LOG_CHANNEL_ID)
            tg_handler.setLevel(logging.INFO) # فقط لاگ‌های مهم و اطلاعاتی
            formatter = logging.Formatter('<b>%(levelname)s</b>: %(message)s')
            tg_handler.setFormatter(formatter)
            logger.addHandler(tg_handler)
            logger.info(f"✅ لاگ‌های سیستم به کانال {config.LOG_CHANNEL_ID} متصل شد.")
        except Exception as e:
            print(f"خطا در اتصال لاگ تلگرام: {e}")
    # ------------------------------------------------------------

    await pipeline.start_processors()

    logger.info("⏳ Starting Backfill...")
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    
    all_source_ids = list(config.NEWS_SOURCES.keys()) + list(config.PROXY_SOURCES.keys())
    
    if not all_source_ids: logger.warning("⚠️ No IDs in Config! Discovery Mode Active.")

    for chat_id in all_source_ids:
        try:
            async for msg in client.iter_messages(chat_id, offset_date=one_hour_ago, reverse=True):
                source = config.NEWS_SOURCES.get(chat_id) or config.PROXY_SOURCES.get(chat_id)
                
                payload = {
                    'source': source,
                    'text': msg.text or "",
                    'msg_obj': msg,
                    'type': 'unknown'
                }
                
                if chat_id in config.PROXY_SOURCES:
                    payload['type'] = 'raw_proxy'
                    if msg.file and msg.file.name:
                        if any(msg.file.name.lower().endswith(ext) for ext in config.PROXY_FILE_EXTENSIONS):
                            payload['file_name'] = msg.file.name.lower()
                            payload['file_size'] = msg.file.size
                
                elif chat_id in config.NEWS_SOURCES:
                    payload['type'] = 'raw_news'
                    payload['is_heavy'] = bool(msg.video or msg.gif)

                await pipeline.ingest(payload)
                await asyncio.sleep(0.01)
                
            logger.info(f"✅ Backfill pushed for {chat_id}")
        except Exception as e:
            logger.error(f"Backfill Error {chat_id}: {e}")

    logger.info("✅ Backfill Complete. Live Mode ON.")
    backfill_done.set()

    @client.on(events.NewMessage())
    @client.on(events.MessageEdited())   # <--- این خط اضافه شود
    async def handler(event):
        if not backfill_done.is_set(): return 
        try:
            chat_id = event.chat_id
            
            # Fast Check
            is_proxy = chat_id in config.PROXY_SOURCES
            is_news = chat_id in config.NEWS_SOURCES
            
# Discovery Logic
            if not is_proxy and not is_news:
                # اصلاح باگ: دریافت ایمن نام چت/کاربر
                chat_title = "Unknown"
                if event.chat:
                    # اگر کانال یا گروه باشد، تایتل دارد
                    if hasattr(event.chat, 'title'):
                        chat_title = event.chat.title
                    # اگر کاربر باشد، نام کوچک دارد
                    elif hasattr(event.chat, 'first_name'):
                        chat_title = event.chat.first_name
                
                await pipeline.ingest({'type': 'discovery', 'chat_id': chat_id, 'title': chat_title or "Unknown"})
                return
                

            source = config.PROXY_SOURCES.get(chat_id) or config.NEWS_SOURCES.get(chat_id)
            
            payload = {
                'source': source,
                'text': event.message.text or "",
                'msg_obj': event.message,
                'type': 'raw_proxy' if is_proxy else 'raw_news'
            }
            
            if is_proxy and event.message.file and event.message.file.name:
                if any(event.message.file.name.lower().endswith(ext) for ext in config.PROXY_FILE_EXTENSIONS):
                    payload['file_name'] = event.message.file.name.lower()
                    payload['file_size'] = event.message.file.size

            if is_news:
                 payload['is_heavy'] = bool(event.message.video or event.message.gif)

            await pipeline.ingest(payload)
            
        except Exception as e:
            logger.error(f"Handler Error: {e}")

    await client.run_until_disconnected()

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except KeyboardInterrupt: pass
    except Exception as e: logger.critical(f"Fatal: {e}")





import os
import time
import logging
import asyncio
import html
import requests
import feedparser
import re
import random
import threading
from collections import deque

import pymongo
from flask import Flask
from telethon import TelegramClient
from telethon.sessions import StringSession
from telegram import Bot
from telegram.error import TelegramError
import google.generativeai as genai

# LOGGING
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# 0. CONFIG & SERVER SETUP (دریافت اطلاعات از سرور Render)
# -------------------------------------------------------------------------
# این متغیرها باید در بخش Environment Variables سایت Render وارد شده باشند
API_ID = int(os.environ.get("TELEGRAM_API_ID"))
API_HASH = os.environ.get("TELEGRAM_API_HASH")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
MONGO_URL = os.environ.get("MONGO_URL")
STRING_SESSION = os.environ.get("STRING_SESSION")
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
NEWSAPI = os.environ.get("NEWSAPI_KEY")

# --- لیست منابع خبری (طبق درخواست شما) ---
RSS_LINKS = [
    # 🇨🇳 چین
    "https://www.scmp.com/rss/91/feed",
    "https://www.chinadaily.com.cn/rss/china_rss.xml",
    # 🇮🇷 فارسی
    "https://feeds.bbci.co.uk/persian/rss.xml",
    "https://per.euronews.com/rss",
    "https://www.independentpersian.com/rss.xml",
    # 🇺🇸 آمریکا
    "http://rss.cnn.com/rss/edition_world.rss",
    "https://feeds.foxnews.com/foxnews/world",
    "https://feeds.washingtonpost.com/rss/world",
    "https://www.cbsnews.com/latest/rss/world",
    # 🇪🇺 اروپا
    "https://www.france24.com/en/rss",
    "https://www.theguardian.com/world/rss",
    "https://rss.dw.com/xml/rss-en-all",
    # 🇸🇦/🇶🇦 خاورمیانه
    "https://www.aljazeera.com/xml/rss/all.xml",
    # 💰 اقتصاد و تکنولوژی
    "https://cointelegraph.com/rss",
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "https://www.theverge.com/rss/index.xml",
]

SOURCE_CHANNELS = [
    "BBCPersian",
    "RadioFarda",
    "Tasnimnews",
    "deutsch_news1",
    "khabarfuri",
    "voafarsi",
    "ManotoNews"
]

BLACKLIST = [
    "@deutsch_news1", "deutsch_news1", "آخرین اخبارفوری آلمان",
    "@radiofarda_official", "radiofarda_official", "RadioFarda", "@RadioFarda",
    "@BBCPersian", "BBCPersian",
    "Tasnimnews", "@TasnimNews", "https://www.TasnimNews.ir", "www.TasnimNews.ir",
    "@KhabarFuri", "KhabarFuri", "KhabarFuri | اخبار",
    "عضو شوید", "join", "لینک عضویت", "کلیک کنید"
]

NEW_SIGNATURE = "\n\n🚀 <b>NEXUS new | اخبار نکس آس نیوز</b>\n🆔 @newsnew_now"

# --- FLASK SERVER (برای زنده ماندن) ---
app = Flask(__name__)

@app.route('/')
def home():
    return "NEXUS BOT IS ALIVE & RUNNING! 🦁"

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# -------------------------------------------------------------------------
# 1. CLOUD MEMORY (MONGODB)
# -------------------------------------------------------------------------
class CloudMemory:
    def __init__(self):
        self.recent_titles = deque(maxlen=50)
        try:
            self.client = pymongo.MongoClient(MONGO_URL)
            self.db = self.client['nexus_db']
            self.collection = self.db['history']
            logger.info("✅ Connected to MongoDB Atlas")
        except Exception as e:
            logger.error(f"❌ DB Error: {e}")
            self.collection = None

    def is_url_seen(self, url):
        if self.collection is not None:
            return self.collection.find_one({"url": str(url)}) is not None
        return False

    def add_posted_item(self, url, title_snippet):
        if self.collection is not None:
            try:
                self.collection.insert_one({"url": str(url), "date": time.time()})
            except: pass
        
        if title_snippet:
            clean_title = title_snippet.replace("\n", " ").strip()[:100]
            self.recent_titles.append(clean_title)

    def get_recent_titles_string(self):
        return "\n".join([f"- {t}" for t in self.recent_titles])

# -------------------------------------------------------------------------
# 2. CONTENT CLEANER (CENSORSHIP + HTML SAFETY)
# -------------------------------------------------------------------------
class ContentCleaner:
    @staticmethod
    def clean_and_sign(text):
        if not text: return ""
        
        # 1. حذف کلمات لیست سیاه
        for bad in BLACKLIST:
            text = re.sub(f"(?i){re.escape(bad)}", "", text)
        # 2. حذف آیدی‌ها و لینک‌ها
        text = re.sub(r'@\w+', '', text)
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        
        # 3. ایمن‌سازی HTML (حیاتی)
        text = html.escape(text)

        # 4. انتخاب ایموجی
        emoji = "📰"
        keywords = {
            "جنگ": "⚔️", "حمله": "💥", "انفجار": "💣", "کشته": "⚫️",
            "آمریکا": "🇺🇸", "ایران": "🇮🇷", "اسرائیل": "🇮🇱", "فلسطین": "🇵🇸",
            "دلار": "💵", "طلا": "💰", "بورس": "📈", "فوتبال": "⚽️", "فوری": "🔴"
        }
        for k,v in keywords.items():
            if k in text: 
                emoji = v
                break
        
        # 5. تمیزکاری نهایی
        clean = text.strip()
        while "\n\n\n" in clean: clean = clean.replace("\n\n\n", "\n\n")
        
        lines = clean.split('\n')
        if lines: lines[0] = f"<b>{emoji} {lines[0]}</b>"
        
        return "\n".join(lines) + NEW_SIGNATURE

# -------------------------------------------------------------------------
# 3. AI ANALYST
# -------------------------------------------------------------------------
class AIAnalyst:
    def __init__(self):
        genai.configure(api_key=GEMINI_KEY)
        self.model = self.setup_model()

    def setup_model(self):
        try: return genai.GenerativeModel('models/gemini-2.5-flash')
        except: return genai.GenerativeModel('gemini-pro')

    def analyze_web_batch(self, articles_list, recent_tg):
        if not articles_list: return []
        # تحلیل 5 خبر اول برای سرعت بیشتر
        limited_list = articles_list[:5]
        
        prompt = f"""
        ACT AS A NEWS EDITOR.
        IGNORE THESE (ALREADY POSTED): {recent_tg}
        ANALYZE THESE NEW ITEMS:
        """
        for i, a in enumerate(limited_list):
            prompt += f"--- {i+1} ---\nHEADLINE: {a['title']}\nCONTEXT: {a.get('description','')[:300]}\n"
        prompt += """
        OUTPUT PERSIAN. CHECK DUPLICATES. SHORT & PUNCHY.
        Format:
        TITLE_FA: [Title]
        SCORE: [1-10]
        CATEGORY: [Cat]
        SUMMARY: [Max 2 sentences]
        PREDICTION: [Max 1 sentence]
        ###NEXT###
        """
        try:
            res = self.model.generate_content(prompt)
            results = []
            if res.text:
                for raw in res.text.split("###NEXT###"):
                    if "TITLE_FA:" in raw: results.append(self.parse(raw))
            return results
        except: return []

    def parse(self, text):
        data = {}
        try:
            for line in text.split('\n'):
                if "TITLE_FA:" in line: data['headline'] = re.sub(r'^[\*🔻🔸🔹🔴\s]+', '', line.split("TITLE_FA:")[1].strip())
                if "SCORE:" in line: data['score'] = int(re.findall(r'\d+', line)[0])
                if "CATEGORY:" in line: data['cat'] = line.split("CATEGORY:")[1].strip()
                if "SUMMARY:" in line: data['sum'] = line.split("SUMMARY:")[1].strip()
                if "PREDICTION:" in line: data['pred'] = line.split("PREDICTION:")[1].strip()
            return data
        except: return {}

# -------------------------------------------------------------------------
# 4. NEXUS BOT CORE
# -------------------------------------------------------------------------
class NexusBot:
    def __init__(self):
        self.bot = Bot(token=BOT_TOKEN)
        self.memory = CloudMemory()
        self.analyst = AIAnalyst()

    async def telegram_loop(self):
        logger.info("🟢 Cloud Telegram Monitor Started")
        try:
            async with TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH) as client:
                if not client.is_connected(): await client.connect()
                
                while True:
                    for channel in SOURCE_CHANNELS:
                        try:
                            async for msg in client.iter_messages(channel, limit=15):
                                has_text = msg.text and len(msg.text) > 10
                                has_media = msg.media is not None
                                if not has_text and not has_media: continue

                                unique_id = f"tg_{channel}_{msg.id}"
                                if not self.memory.is_url_seen(unique_id):
                                    final_text = ContentCleaner.clean_and_sign(msg.text if msg.text else "")
                                    
                                    try:
                                        if has_media:
                                            path = await client.download_media(msg, file="temp_media")
                                            if path:
                                                if path.endswith(('.jpg','.png','.webp')):
                                                    await self.bot.send_photo(chat_id=CHANNEL_ID, photo=open(path,'rb'), caption=final_text[:1000], parse_mode="HTML")
                                                elif path.endswith(('.mp4','.mov','.avi')):
                                                    await self.bot.send_video(chat_id=CHANNEL_ID, video=open(path,'rb'), caption=final_text[:1000], parse_mode="HTML")
                                                else:
                                                    await self.bot.send_document(chat_id=CHANNEL_ID, document=open(path,'rb'), caption=final_text[:1000], parse_mode="HTML")
                                                os.remove(path)
                                        else:
                                            await self.bot.send_message(chat_id=CHANNEL_ID, text=final_text, parse_mode="HTML", disable_web_page_preview=True)
                                        
                                        logger.info(f"🚀 Sent: {unique_id}")
                                        self.memory.add_posted_item(unique_id, msg.text)
                                        await asyncio.sleep(20)
                                    except Exception as e:
                                        logger.error(f"Send Error: {e}")
                                        if os.path.exists("temp_media*"): 
                                            try: os.remove("temp_media*")
                                            except: pass
                        except: pass
                    await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"CRITICAL: Telegram Login Failed! Check STRING_SESSION. Error: {e}")

    async def web_loop(self):
        logger.info("🔵 Cloud Web Monitor Started")
        while True:
            start_time = time.time()
            articles = self.fetch_web()
            if articles:
                recent = self.memory.get_recent_titles_string()
                analyses = self.analyst.analyze_web_batch(articles, recent)
                
                queue = []
                for i, art in enumerate(articles):
                    self.memory.add_posted_item(art['url'], "WEB")
                    an = analyses[i] if i < len(analyses) else None
                    if not an or "DUPLICATE" in an.get('headline','') or an.get('score',0)<4: continue
                    queue.append(self.format_web(an, art))
                
                rem = 3600 - (time.time() - start_time) # هر 1 ساعت
                if rem < 0: rem = 100
                if queue:
                    interval = rem / len(queue)
                    for msg in queue:
                        try:
                            await self.bot.send_message(chat_id=CHANNEL_ID, text=msg, parse_mode="HTML")
                            logger.info("🐢 Web Sent")
                        except: pass
                        await asyncio.sleep(interval)
                else: await asyncio.sleep(rem)
            else: await asyncio.sleep(3600)

    def fetch_web(self):
        raw = []
        try:
            r = requests.get("https://newsapi.org/v2/top-headlines", params={"apiKey": NEWSAPI, "language": "en", "pageSize": 10}, timeout=10)
            for a in r.json().get("articles",[]): raw.append({"title":a['title'],"description":a['description'],"url":a['url'],"source":a['source']['name']})
        except: pass
        for f in RSS_LINKS:
            try:
                d = feedparser.parse(f)
                for e in d.entries[:2]: raw.append({"title":e.title,"description":e.title,"url":e.link,"source":d.feed.get('title','RSS')})
            except: pass
        final = []
        for i in raw:
            if i.get('url') and not self.memory.is_url_seen(i['url']): final.append(i)
        return final[:20]

    def format_web(self, an, art):
        cat_e = "💰" if "Econ" in an.get('cat','') else "🌍"
        return (f"{'🔴' if an['score']>7 else '🔵'} <b>{an['headline']}</b>\n\n"
                f"📡 منبع: {html.escape(art['source'])}\n📊 اهمیت: {an['score']}/10\n{cat_e} دسته‌بندی: {an.get('cat')}\n"
                f"─────────────────────\n💡 {an.get('sum')}\n\n🔮 {an.get('pred')}\n\n"
                f"🔗 <a href='{art['url']}'>مشاهده خبر معتبر</a>{NEW_SIGNATURE}")

if __name__ == "__main__":
    threading.Thread(target=run_web_server).start()
    bot = NexusBot()
    print("NEXUS CLOUD: ONLINE 🌩️")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(bot.telegram_loop(), bot.web_loop()))

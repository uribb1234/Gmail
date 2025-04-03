import aiohttp
import asyncio
import aiomysql
import os
import smtplib
from email.mime.text import MIMEText
from flask import Flask
import schedule
import threading
import time
import logging
from datetime import datetime

# הגדרת לוגים
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# הגדרת חיבור ל-MySQL ממשתני סביבה של Render
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'db': os.getenv('MYSQL_DATABASE'),
    'port': int(os.getenv('MYSQL_PORT', 3306))
}

# הגדרת פרטי SMTP ממשתני סביבה
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')  # App Password עבור Gmail
RECIPIENTS = os.getenv('EMAIL_RECIPIENTS').split(',')  # רשימה מופרדת בפסיקים

async def get_db_connection():
    try:
        conn = await aiomysql.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"שגיאה בחיבור לדטאבייס: {e}")
        return None

async def fetch_news():
    conn = await get_db_connection()
    if not conn:
        return "שגיאה בחיבור לדטאבייס"
    
    try:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            query = """
            WITH RankedNews AS (
                SELECT 
                    site,
                    time,
                    headline,
                    link,
                    ROW_NUMBER() OVER (PARTITION BY site ORDER BY date DESC, time DESC) AS rn
                FROM news_updates
                WHERE site IN ('ynet', 'arutz7', 'walla', 'keshet12', 'reshet13', 'channel14', 'sport5', 'sport1', 'one')
            )
            SELECT 
                'כללי' AS category,
                site,
                time,
                headline,
                link
            FROM RankedNews
            WHERE site IN ('ynet', 'arutz7', 'walla', 'keshet12', 'reshet13', 'channel14')
            AND rn <= 3
            UNION ALL
            SELECT 
                'ספורט' AS category,
                site,
                time,
                headline,
                link
            FROM RankedNews
            WHERE site IN ('sport5', 'sport1', 'one')
            AND rn <= 3
            ORDER BY 
                CASE 
                    WHEN category = 'כללי' THEN 1 
                    WHEN category = 'ספורט' THEN 2 
                END, 
                site, 
                time DESC
            """
            await cursor.execute(query)
            results = await cursor.fetchall()
            return results
    except Exception as e:
        logger.error(f"שגיאה בשליפת נתונים: {e}")
        return f"שגיאה בשליפת נתונים: {str(e)}"
    finally:
        conn.close()

def format_news_email(news):
    if isinstance(news, str):  # במקרה של שגיאה
        return news
    
    email_body = "מבזקי חדשות עדכניים\n\n"
    current_category = None
    
    for item in news:
        category = item['category']
        if category != current_category:
            email_body += f"--- {category} ---\n"
            current_category = category
        
        site = item['site']
        time = item['time']
        headline = item['headline']
        link = item['link']
        email_body += f"{site} [{time}]: {headline}\n{link}\n\n"
    
    return email_body

def send_email(content):
    try:
        msg = MIMEText(content)
        msg['Subject'] = f"מבזקי חדשות - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg['From'] = SMTP_USER
        msg['To'] = ", ".join(RECIPIENTS)
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())
        logger.info("מייל נשלח בהצלחה")
    except Exception as e:
        logger.error(f"שגיאה בשליחת מייל: {e}")

async def send_news_email():
    logger.info("מתחיל שליחת מייל עם מבזקים")
    news = await fetch_news()
    email_content = format_news_email(news)
    send_email(email_content)
    logger.info("שליחת מייל הושלמה")

# Endpoint לפינג כדי לשמור על השרת פעיל
@app.route('/ping', methods=['GET'])
def ping():
    return "Pong", 200

# פינג פנימי כל 5 דקות
async def keep_alive():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                url = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:10000") + "/ping"
                logger.info(f"שולח פינג ל-{url}")
                async with session.get(url) as response:
                    if response.status == 200:
                        logger.debug("פינג פנימי הצליח")
                    else:
                        logger.warning(f"פינג פנימי נכשל עם סטטוס {response.status}")
        except Exception as e:
            logger.error(f"שגיאה בפינג פנימי: {e}")
        await asyncio.sleep(300)  # 5 דקות

# תזמון שליחת המיילים
def run_scheduler():
    # שליחה כל שעה עגולה
    schedule.every().hour.at(":00").do(lambda: asyncio.run(send_news_email()))
    
    while True:
        schedule.run_pending()
        time.sleep(60)

# פונקציה להפעלת ה-Flask וה-Keep-Alive בלולאת asyncio
async def run_app_and_keep_alive():
    # התחלת ה-scheduler בשרשור נפרד
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # שליחת מייל ראשון מיידית
    await send_news_email()

    # התחלת ה-Keep-Alive
    await keep_alive()

if __name__ == "__main__":
    port = int(os.getenv('PORT', 10000))
    
    # התחלת ה-Flask בשרשור נפרד
    def run_flask():
        app.run(host='0.0.0.0', port=port, use_reloader=False)
    
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # הפעלת ה-Keep-Alive והמייל הראשון בלולאת asyncio
    asyncio.run(run_app_and_keep_alive())

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
import requests

# הגדרת לוגים מפורטים
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s - [%(funcName)s:%(lineno)d]')
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
    logger.debug(f"מנסה להתחבר לדטאבייס עם פרטים: {DB_CONFIG}")
    try:
        conn = await aiomysql.connect(**DB_CONFIG)
        logger.info("חיבור לדטאבייס הצליח")
        return conn
    except Exception as e:
        logger.error(f"שגיאה בחיבור לדטאבייס: {e}")
        return None

async def fetch_news():
    conn = await get_db_connection()
    if not conn:
        logger.error("חיבור לדטאבייס נכשל, מחזיר שגיאה")
        return "שגיאה בחיבור לדטאבייס"
    
    try:
        logger.debug("מבצע שאילתה ל-MySQL")
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
            ORDER BY category DESC, site, time DESC
            """
            await cursor.execute(query)
            results = await cursor.fetchall()
            logger.info(f"שליפת נתונים הצליחה, נמצאו {len(results)} מבזקים")
            return results
    except Exception as e:
        logger.error(f"שגיאה בשליפת נתונים: {e}")
        return f"שגיאה בשליפת נתונים: {str(e)}"
    finally:
        conn.close()
        logger.debug("חיבור לדטאבייס נסגר")

def format_news_email(news):
    if isinstance(news, str):  # במקרה של שגיאה
        logger.warning(f"קיבלתי שגיאה במקום נתונים: {news}")
        return news
    
    logger.debug("מעצב את תוכן המייל")
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
    
    logger.debug("תוכן המייל עוצב בהצלחה")
    return email_body

def send_email(content):
    logger.debug(f"מנסה לשלוח מייל לנמענים: {', '.join(RECIPIENTS)}")
    logger.debug(f"משתמש ב-SMTP: {SMTP_SERVER}:{SMTP_PORT}, משתמש: {SMTP_USER}")
    try:
        msg = MIMEText(content)
        msg['Subject'] = f"מבזקי חדשות - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg['From'] = SMTP_USER
        msg['To'] = ", ".join(RECIPIENTS)
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            logger.debug("מתחיל TLS")
            server.starttls()
            logger.debug("מבצע התחברות לשרת SMTP")
            server.login(SMTP_USER, SMTP_PASSWORD)
            logger.debug("שולח את המייל")
            server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())
        logger.info("מייל נשלח בהצלחה")
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"שגיאת אימות SMTP: {e} - בדוק את SMTP_USER ו-SMTP_PASSWORD")
    except smtplib.SMTPException as e:
        logger.error(f"שגיאה בשליחת המייל (SMTP): {e}")
    except Exception as e:
        logger.error(f"שגיאה לא צפויה בשליחת המייל: {e}")

async def send_news_email():
    logger.info("מתחיל שליחת מייל עם מבזקים")
    news = await fetch_news()
    email_content = format_news_email(news)
    send_email(email_content)
    logger.info("שליחת מייל הושלמה")

# Endpoint לפינג כדי לשמור על השרת פעיל
@app.route('/ping', methods=['GET'])
def ping():
    logger.debug("קיבלתי בקשת פינג")
    return "Pong", 200

# תזמון שליחת המיילים
def run_scheduler():
    logger.info("מתחיל scheduler")
    # שליחה פעם בשעה (כהתחלה)
    schedule.every().hour.at(":00").do(lambda: asyncio.run(send_news_email()))

    # שלב מאוחר יותר: שליחה 3 פעמים ביום (למשל, 08:00, 14:00, 20:00)
    # schedule.every().day.at("08:00").do(lambda: asyncio.run(send_news_email()))
    # schedule.every().day.at("14:00").do(lambda: asyncio.run(send_news_email()))
    # schedule.every().day.at("20:00").do(lambda: asyncio.run(send_news_email()))

    while True:
        schedule.run_pending()
        logger.debug("ממתין למשימות מתוזמנות")
        time.sleep(60)

# פינג פנימי כל 5 דקות כדי לשמור על השרת פעיל
def keep_alive():
    logger.info("מתחיל פינג פנימי")
    while True:
        try:
            url = os.getenv('RENDER_EXTERNAL_URL', 'http://localhost:5000') + '/ping'
            logger.debug(f"שולח פינג ל-{url}")
            requests.get(url)
            logger.info("פינג פנימי נשלח בהצלחה")
        except Exception as e:
            logger.error(f"שגיאה בפינג פנימי: {e}")
        time.sleep(300)  # 300 שניות = 5 דקות

if __name__ == "__main__":
    # התחלת ה-scheduler בשרשור נפרד
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # התחלת הפינג הפנימי בשרשור נפרד
    keep_alive_thread = threading.Thread(target=keep_alive)
    keep_alive_thread.daemon = True
    keep_alive_thread.start()

    # הפעלת Flask
    port = int(os.getenv('PORT', 5000))
    logger.info(f"מפעיל שרת Flask על פורט {port}")
    app.run(host='0.0.0.0', port=port)
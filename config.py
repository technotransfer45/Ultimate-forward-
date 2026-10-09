import os
from dotenv import load_dotenv

load_dotenv()

# Telegram API Configuration
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Userbot Sessions for Dual Accounts
USERBOT_SESSION_1 = os.getenv("USERBOT_SESSION_1", "")
USERBOT_SESSION_2 = os.getenv("USERBOT_SESSION_2", "")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "master_forwarder_db")

# Admin IDs
raw_admins = os.getenv("ADMINS", "")
ADMINS = [int(admin_id.strip()) for admin_id in raw_admins.split(",") if admin_id.strip().isdigit()]

# Destination / Target Channels
TERABOX_TARGET = int(os.getenv("TERABOX_TARGET", "0"))
STREAMING_TARGET = int(os.getenv("STREAMING_TARGET", "0"))
MEDIA_TARGET = int(os.getenv("MEDIA_TARGET", "0"))
ROUGH_TARGET = int(os.getenv("ROUGH_TARGET", "0"))
FINAL_CONVERTED_TARGET = int(os.getenv("FINAL_CONVERTED_TARGET", "0"))

# Converter Bot Setup
CONVERTER_BOT_USERNAME = os.getenv("CONVERTER_BOT_USERNAME", "").replace("@", "").strip()
CONVERTER_TIMEOUT_SEC = int(os.getenv("CONVERTER_TIMEOUT_SEC", "30"))

# Media Threshold Conditions (50 MB or 10 Minutes)
MIN_MEDIA_SIZE_MB = float(os.getenv("MIN_MEDIA_SIZE_MB", "50.0"))
MIN_MEDIA_DURATION_SEC = int(os.getenv("MIN_MEDIA_DURATION_SEC", "600"))

# Web Service Port
PORT = int(os.getenv("PORT", "8080"))

# Recognized TeraBox Domains & In-App Subdomains
TERABOX_DOMAINS = [
    "terabox.com",
    "teraboxapp.com",
    "1024tera.com",
    "mirrobox.com",
    "nephobox.com",
    "4funbox.com",
    "teraboxlink.com",
    "terasharelink.com",
    "freeterabox.com",
    "tibibox.com",
    "dm.nephobox.com",
    "dm.terabox.com"
]

# Configured Streaming Video Domains
STREAMING_DOMAINS = [
    "streamnet",
    "diskwala",
    "mdisk",
    "doodstream",
    "dood.",
    "filemoon",
    "streamtape",
    "streamsb",
    "vidguard",
    "hubcloud"
]

# Filtered / Blacklisted Domains
BLOCKED_DOMAINS = [
    "amazon.in",
    "amazon.com",
    "amzn.to",
    "amzn.in",
    "flipkart.com",
    "fkrt.it",
    "myntra.com",
    "ajio.com",
    "meesho.com",
    "snapdeal.com",
    "instagram.com",
    "facebook.com",
    "fb.watch",
    "twitter.com",
    "x.com",
    "youtube.com",
    "youtu.be",
    "t.me",
    "telegram.dog",
    "telegram.me",
    "mediafire.com",
    "rigi.club",
    "telegra.ph",
    "google.com"
]

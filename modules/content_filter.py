import config
from urllib.parse import urlparse

class ContentFilter:
    @staticmethod
    def is_blocked(url: str) -> bool:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        full_url = url.lower()
        for domain in config.BLOCKED_DOMAINS:
            if domain in netloc or domain in full_url:
                return True
        return False

    @staticmethod
    def is_terabox(url: str) -> bool:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        full_url = url.lower()
        
        # Domain match
        matched_domain = any(domain in netloc for domain in config.TERABOX_DOMAINS)
        # In-App Hive Feed match
        is_hive = "wap/hive/channelshare" in full_url or "bot_uk=" in full_url
        
        return matched_domain or is_hive

    @staticmethod
    def is_streaming(url: str) -> bool:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        full_url = url.lower()
        for domain in config.STREAMING_DOMAINS:
            if domain in netloc or domain in full_url:
                return True
        return False

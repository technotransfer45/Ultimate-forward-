import re
from typing import List
from urllib.parse import urlparse, urlunparse

class LinkExtractor:
    URL_REGEX = re.compile(r'https?://[^\s<>"]+|www\.[^\s<>"]+')

    @classmethod
    def extract_urls(cls, text: str) -> List[str]:
        if not text:
            return []
        found_urls = cls.URL_REGEX.findall(text)
        cleaned_urls = []
        for url in found_urls:
            url = url.rstrip('.,!?:;)]}"\'')
            if url.startswith("www."):
                url = "http://" + url
            cleaned_urls.append(url)
        return cleaned_urls

    @staticmethod
    def canonicalize(url: str) -> str:
        parsed = urlparse(url)
        clean_path = parsed.path.rstrip('/')
        canonical_netloc = parsed.netloc.lower()
        if canonical_netloc.startswith("www."):
            canonical_netloc = canonical_netloc[4:]
        
        # Keep query parameters if it's an In-App Hive link (bot_uk is needed)
        if "wap/hive" in clean_path or "channelshare" in clean_path.lower():
            return urlunparse((parsed.scheme.lower(), canonical_netloc, clean_path, '', parsed.query, ''))
        
        # Default: strip query parameters to avoid tracking duplication
        return urlunparse((parsed.scheme.lower(), canonical_netloc, clean_path, '', '', ''))

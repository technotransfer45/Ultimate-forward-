import aiohttp
import re
import logging
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger("TeraBoxScraper")

class TeraBoxAppScraper:
    @staticmethod
    def extract_shorturl_key(url_or_key: str) -> str:
        match = re.search(r'(?:/s/|surl=)([a-zA-Z0-9_-]+)', url_or_key)
        if match:
            key = match.group(1)
            return key if not key.startswith("1") else key[1:]
        return url_or_key.strip()

    @staticmethod
    def is_hive_channel(url: str) -> bool:
        return "wap/hive/channelshare" in url.lower() or "bot_uk=" in url.lower()

    @staticmethod
    def extract_bot_uk(url: str) -> str:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if "bot_uk" in params:
            return params["bot_uk"][0]
        match = re.search(r'bot_uk=(\d+)', url)
        return match.group(1) if match else ""

    @staticmethod
    async def fetch_terabox_data(url: str) -> list:
        # Case 1: TeraBox / NephoBox Hive In-App Channel (bot_uk=...)
        if TeraBoxAppScraper.is_hive_channel(url):
            return await TeraBoxAppScraper.fetch_hive_channel_posts(url)

        # Case 2: Standard TeraBox File Share Link (/s/1...)
        surl = TeraBoxAppScraper.extract_shorturl_key(url)
        api_url = f"https://www.teraboxapp.com/api/shorturlinfo?shorturl=1{surl}&root=1"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        extracted_items = []
        try:
            timeout = aiohttp.ClientTimeout(total=12)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(api_url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        file_list = data.get("list", [])
                        for f in file_list:
                            extracted_items.append({
                                "title": f.get("server_filename", "Unknown Title"),
                                "size": f.get("size", 0),
                                "url": url
                            })
        except Exception as e:
            logger.error(f"TeraBox API scraper failed for {url}: {str(e)}")

        return extracted_items

    @staticmethod
    async def fetch_hive_channel_posts(hive_url: str) -> list:
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        }

        extracted_items = []
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(hive_url) as resp:
                    if resp.status != 200:
                        logger.error(f"Failed to fetch Hive Channel URL: HTTP {resp.status}")
                        return extracted_items
                    html_content = await resp.text()

            # Find all direct video/file links inside the channel feed
            link_pattern = re.findall(
                r'https?://[a-zA-Z0-9.-]*(?:terabox|1024tera|nephobox|teraboxlink|mirrobox)[a-zA-Z0-9.-]*/s/[a-zA-Z0-9_-]+',
                html_content
            )

            # Find associated post labels/text blocks
            card_blocks = re.findall(r'([^<\n\r]+?)(?:https?://[^\s<>"]+/s/[a-zA-Z0-9_-]+)', html_content)

            unique_links = list(set(link_pattern))
            for raw_link in unique_links:
                clean_title = "TeraBox Video"
                for block in card_blocks:
                    if raw_link in block or any(part in block for part in raw_link.split('/')[-1:]):
                        potential_title = re.sub(r'[\r\n\t]+', ' ', block).strip()
                        if len(potential_title) > 3:
                            clean_title = potential_title
                            break

                extracted_items.append({
                    "title": clean_title,
                    "size": 0,
                    "url": raw_link
                })

            logger.info(f"Hive Channel scraper retrieved {len(extracted_items)} items from {hive_url}")

        except Exception as e:
            logger.error(f"Exception while scraping Hive Channel {hive_url}: {str(e)}")

        return extracted_items

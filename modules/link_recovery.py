import aiohttp
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class LinkRecovery:
    @staticmethod
    async def resolve_url(url: str) -> Tuple[str, bool]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.head(url, allow_redirects=True) as response:
                    return str(response.url), True
        except Exception:
            pass

        try:
            async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                async with session.get(url, allow_redirects=True) as response:
                    return str(response.url), True
        except Exception as e:
            logger.warning(f"Could not resolve redirect for {url}: {str(e)}")
            return url, False

from __future__ import annotations

import re
from typing import Set

import config
from modules.link_extractor import get_host, host_matches, is_terabox_host
from urllib.parse import urlsplit

BLACKLIST_DOMAINS: Set[str] = {
    # social
    "instagram.com", "instagr.am", "youtube.com", "youtu.be", "facebook.com", "fb.com", "fb.me", "fb.watch",
    "twitter.com", "x.com", "t.co", "tiktok.com", "snapchat.com", "pinterest.com", "pin.it", "linkedin.com",
    "lnkd.in", "reddit.com", "redd.it", "whatsapp.com", "wa.me", "discord.gg", "discord.com", "threads.net",
    # e-commerce
    "amzn.to", "amzn.in", "amzn.com", "a.co", "flipkart.com", "fkrt.it", "fkrt.cc", "myntra.com", "ajio.com",
    "meesho.com", "snapdeal.com", "nykaa.com", "tatacliq.com", "aliexpress.com", "shopsy.in", "jiomart.com",
    "croma.com", "bigbasket.com", "swiggy.com", "zomato.com", "play.google.com", "apps.apple.com",
}
BLACKLIST_REGEXES = [
    re.compile(r"(^|\.)amazon\.[a-z.]+$"),
    re.compile(r"(^|\.)ebay\.[a-z.]+$"),
    re.compile(r"(^|\.)flipkart\.[a-z.]+$"),
]

SHORTENER_DOMAINS: Set[str] = {
    "bit.ly", "bitly.com", "tinyurl.com", "cutt.ly", "shorturl.at", "rb.gy", "is.gd", "v.gd", "ow.ly",
    "t.ly", "rebrand.ly", "goo.gl", "tiny.cc", "adf.ly", "ouo.io", "ouo.press", "shrinkme.io",
    "shrinkearn.com", "gplinks.in", "gplinks.co", "droplink.co", "linkvertise.com", "link-to.net",
    "exe.io", "exey.io", "fc.lc", "za.gl", "clk.sh", "clicksfly.com", "urlshortx.com", "shrtfly.com",
    "bc.vc", "adfoc.us", "short.st", "tnshort.net", "tnlink.in", "earnlink.io", "linkshortify.com",
    "shortner.in", "bit.do", "soo.gd", "lnk.to", "qr.ae", "s.id", "short.io",
}

STREAM_DOMAINS: Set[str] = {
    "streaam.net", "streamnet.in", "streamnet.co", "streamnet.me", "streamnet.link",
    "diskwala.com", "diskwala.net", "mdisk.me", "mdisk.co", "mdiskplay.com", "mdisk.pro",
    "doodstream.com", "d000d.com", "d0000d.com", "d0o0d.com", "ds2play.com", "ds2video.com", "dooood.com",
    "filemoon.sx", "filemoon.to", "filemoon.in", "filemoon.link", "filemoon.nl",
    "streamtape.com", "streamtape.to", "strtape.cloud", "stape.fun",
    "streamwish.com", "streamwish.to", "awish.pro", "vidhide.com", "vidhidepro.com",
    "mixdrop.co", "mixdrop.to", "mixdrop.ag", "upstream.to", "voe.sx",
    "streamsb.net", "sbembed.com", "sbplay.org", "lulustream.com", "luluvdo.com",
    "vidguard.to", "vgfplay.com", "vembed.net", "embedsito.com", "fembed.com",
} | set(config.EXTRA_STREAM_DOMAINS)
DOOD_RE = re.compile(r"^(?:[a-z0-9-]+\.)*dood[a-z0-9]*\.[a-z]{2,}$")


def is_blacklisted_host(host: str) -> bool:
    if any(host_matches(host, domain) for domain in BLACKLIST_DOMAINS):
        return True
    return any(regex.search(host) for regex in BLACKLIST_REGEXES)


def is_shortener_host(host: str) -> bool:
    return any(host_matches(host, domain) for domain in SHORTENER_DOMAINS)


def is_stream_host(host: str) -> bool:
    if any(host_matches(host, domain) for domain in STREAM_DOMAINS):
        return True
    return bool(DOOD_RE.match(host))


def is_hive_url(url: str) -> bool:
    host = get_host(url)
    if not is_terabox_host(host):
        return False
    path = urlsplit(url).path.lower()
    return "/hive/" in path or "channelshare" in path


def classify(url: str) -> str:
    """Return one of: hive, terabox, blacklist, shortener, stream, unknown."""
    host = get_host(url)
    if not host:
        return "unknown"
    if is_terabox_host(host):
        if is_hive_url(url):
            return "hive"
        if "/s/" in urlsplit(url).path:
            return "terabox"
        return "unknown"
    if is_blacklisted_host(host):
        return "blacklist"
    if is_shortener_host(host):
        return "shortener"
    if is_stream_host(host):
        return "stream"
    return "unknown"

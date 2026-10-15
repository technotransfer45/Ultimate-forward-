from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

URL_RE = re.compile(r"""https?://[^\s<>"'\)\]\}]+""", re.I)

TERABOX_DOMAINS = {
    "terabox.com", "teraboxapp.com", "1024terabox.com", "terabox.app", "teraboxlink.com",
    "terafileshare.com", "terasharelink.com", "terasharefile.com", "4funbox.com", "4funbox.co",
    "mirrobox.com", "nephobox.com", "momerybox.com", "freeterabox.com", "tibibox.com",
    "1024tera.com", "teraboxshare.com", "terabox.fun", "gibibox.com", "goaibox.com",
    "teraboxurl.com", "dubox.com", "terabox1024.com",
}

_TB_ALT = "|".join(re.escape(d) for d in sorted(TERABOX_DOMAINS, key=len, reverse=True))
BARE_TERABOX_RE = re.compile(
    r"(?<![\w@/.\-])((?:[a-z0-9-]+\.)*(?:" + _TB_ALT + r")/(?:s/|sharing/link\?surl=|wap/share/filelist\?surl=)[A-Za-z0-9_-]+)",
    re.I,
)

TRACKING_PARAMS = {
    "fbclid", "gclid", "dclid", "msclkid", "yclid", "igshid", "si", "ref", "ref_src", "ref_url",
    "referrer", "source", "src", "share", "feature", "spm", "mkt_tok", "_hsenc", "_hsmi",
    "cmpid", "campaign", "trk", "trkid", "scid", "ncid", "ocid",
}
TRACKING_PREFIXES = ("utm_", "mc_", "pk_", "hsa_", "vero_", "ga_")


def get_host(url: str) -> str:
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def host_matches(host: str, domain: str) -> bool:
    return host == domain or host.endswith("." + domain)


def is_terabox_host(host: str) -> bool:
    return any(host_matches(host, domain) for domain in TERABOX_DOMAINS)


def _is_tracking(key: str) -> bool:
    lowered = key.lower()
    return lowered in TRACKING_PARAMS or lowered.startswith(TRACKING_PREFIXES)


def _terabox_share_id(path: str, query: List[Any]) -> str:
    match = re.search(r"/s/([A-Za-z0-9_-]+)", path)
    if match:
        return match.group(1)
    for key, value in query:
        if key.lower() == "surl" and value:
            return value if value.startswith("1") else "1" + value
    return ""


def normalize_url(url: str) -> str:
    """Canonicalize a URL: https, lowercase host, no www, no tracking, TeraBox mirrors unified."""
    raw = (url or "").strip().strip("<>\"'“”‘’()[]{}").rstrip(".,;:!?*")
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    if not re.match(r"^[a-z][a-z0-9+.\-]*://", raw, re.I):
        raw = "https://" + raw
    try:
        parts = urlsplit(raw)
        host = (parts.hostname or "").lower()
    except ValueError:
        return ""
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/{2,}", "/", parts.path)
    query = parse_qsl(parts.query, keep_blank_values=False)

    if is_terabox_host(host):
        share_id = _terabox_share_id(path, query)
        if share_id:
            return "https://terabox.com/s/" + share_id
        if "/hive/" in path.lower() or "channelshare" in path.lower():
            keep = sorted((k, v) for k, v in query if k.lower() in ("bot_uk", "uk", "channel_id"))
            suffix = "?" + urlencode(keep) if keep else ""
            return "https://" + host + path.rstrip("/") + suffix

    cleaned = sorted((k, v) for k, v in query if not _is_tracking(k))
    if len(path) > 1:
        path = path.rstrip("/")
    elif path == "/":
        path = ""
    return urlunsplit(("https", host, path, urlencode(cleaned), ""))


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def media_hash(file_unique_id: str) -> str:
    return hashlib.sha256(("media:" + file_unique_id).encode("utf-8")).hexdigest()


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hive_key(url: str) -> str:
    """Stable sources-collection key for a hive channel page."""
    normalized = normalize_url(url)
    for key, value in parse_qsl(urlsplit(normalized).query):
        if key.lower() == "bot_uk" and value:
            return "hive:" + value
    return "hive:" + hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]


def extract_links_from_text(text: str) -> List[str]:
    """Return normalized, de-duplicated links found in plain text (order preserved)."""
    if not text:
        return []
    found: List[str] = []
    for match in URL_RE.finditer(text):
        found.append(match.group(0))
    for match in BARE_TERABOX_RE.finditer(text):
        found.append("https://" + match.group(1))
    result: List[str] = []
    seen = set()
    for item in found:
        normalized = normalize_url(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def extract_links(message: Any) -> List[str]:
    """Collect links from text, caption, entities, inline buttons and web preview."""
    raw: List[str] = []
    text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
    raw.extend(extract_links_from_text(text))

    for attr in ("entities", "caption_entities"):
        for entity in getattr(message, attr, None) or []:
            url = getattr(entity, "url", None)
            if url:
                raw.append(url)

    markup = getattr(message, "reply_markup", None)
    for row in getattr(markup, "inline_keyboard", None) or []:
        for button in row:
            url = getattr(button, "url", None)
            if url and url.lower().startswith("http"):
                raw.append(url)

    web_page = getattr(message, "web_page", None)
    page_url = getattr(web_page, "url", None) if web_page is not None else None
    if page_url:
        raw.append(page_url)

    result: List[str] = []
    seen = set()
    for item in raw:
        normalized = normalize_url(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def link_contexts(text: str) -> Dict[str, str]:
    """Map each normalized link to the nearest descriptive line (same line, else the previous one)."""
    contexts: Dict[str, str] = {}
    if not text:
        return contexts
    lines = text.splitlines()
    last_descriptive = ""
    for line in lines:
        links = extract_links_from_text(line)
        stripped = URL_RE.sub(" ", line)
        stripped = BARE_TERABOX_RE.sub(" ", stripped).strip()
        descriptive = stripped if len(re.findall(r"[A-Za-z0-9]", stripped)) >= 3 else ""
        for link in links:
            if link not in contexts:
                contexts[link] = descriptive or last_descriptive
        if descriptive:
            last_descriptive = descriptive
    return contexts

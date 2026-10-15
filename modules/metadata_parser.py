from __future__ import annotations

import re
from typing import Dict, List

URL_RE = re.compile(r"https?://\S+", re.I)
SEPARATORS_RE = re.compile(r"[._]+")
BRACKETS_RE = re.compile(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}")
MENTION_RE = re.compile(r"@\w+")
LABEL_RE = re.compile(r"^\W*(?:title|name|file(?:\s*name)?|movie|series|show)\s*[:\-–]\s*", re.I)

SXXEXX_RE = re.compile(
    r"\bS(\d{1,2})[\s._-]*E(?:P)?(\d{1,4})(?:[\s._-]*(?:-|to|&)[\s._-]*E?(?:P)?(\d{1,4}))?\b", re.I
)
SEASON_RE = re.compile(r"\b(?:Seasons?|S)[\s._-]*(\d{1,2})\b", re.I)
EPISODE_RE = re.compile(r"\b(?:Episode|Ep|E)[\s._-]*(\d{1,4})(?:[\s._-]*(?:-|to)[\s._-]*E?(\d{1,4}))?\b", re.I)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
RES_RE = re.compile(r"\b(4320p|2160p|1440p|1080p|720p|576p|540p|480p|360p|240p|8k|4k|uhd|fhd)\b", re.I)
SOURCE_RE = re.compile(
    r"\b(web[\s._-]?dl|webrip|blu[\s._-]?ray|brrip|bdrip|hdrip|hdtv|dvdrip|dvdscr|hdcam|camrip|predvd|hdts|remux|"
    r"nf|amzn|dsnp|hmax|zee5|jc|atvp|hotstar|sonyliv)\b",
    re.I,
)
CODEC_RE = re.compile(
    r"\b(x\.?265|x\.?264|h\.?265|h\.?264|hevc|avc|av1|vp9|10[\s._-]?bit|8[\s._-]?bit|hdr10\+?|hdr|"
    r"dolby[\s._-]?vision|dv)\b",
    re.I,
)
AUDIO_CODEC_RE = re.compile(
    r"\b(ddp?\+?[\s._-]?\d[\s.]\d|eac3|ac3|aac(?:[\s._-]?\d[\s.]\d)?|atmos|dts(?:[\s._-]?hd)?|truehd|opus|flac|mp3)\b",
    re.I,
)
LANG_RE = re.compile(
    r"\b(hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi|gujarati|urdu|korean|japanese|"
    r"chinese|spanish|french|german|russian|turkish|thai|indonesian|hin|eng|tam|tel|mal|kan|"
    r"dual[\s._-]?audio|multi[\s._-]?audio|dubbed)\b",
    re.I,
)
LANG_ALIASES = {"hin": "Hindi", "eng": "English", "tam": "Tamil", "tel": "Telugu", "mal": "Malayalam", "kan": "Kannada"}


def _unique(values: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        key = value.lower()
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def _norm_token(token: str) -> str:
    return re.sub(r"[\s._]+", "", token).upper() if token.lower().startswith(("web", "blu", "dolby")) else token


def extract_title_line(text: str) -> str:
    """Pick the line of a caption most likely to be the title."""
    if not text:
        return ""
    for line in text.splitlines():
        cleaned = URL_RE.sub(" ", line)
        cleaned = LABEL_RE.sub("", cleaned).strip()
        if len(re.findall(r"[A-Za-z0-9]", cleaned)) >= 3:
            return cleaned
    return ""


def _clean_title(text: str) -> str:
    text = URL_RE.sub(" ", text)
    text = BRACKETS_RE.sub(" ", text)
    text = MENTION_RE.sub(" ", text)
    text = SEPARATORS_RE.sub(" ", text)
    text = re.sub(r"[^\w\s&'\-:,!?+]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip(" -–|:,")
    return text


def parse_metadata(raw: str) -> Dict[str, str]:
    """Parse title, season, episode, quality, codec and audio out of a name/caption."""
    source = extract_title_line(raw) or (raw or "")
    source = URL_RE.sub(" ", source)
    working = BRACKETS_RE.sub(lambda m: " " + m.group(0)[1:-1] + " ", source)
    working = SEPARATORS_RE.sub(" ", working)

    season = ""
    episode = ""
    cut_positions: List[int] = []

    combo = SXXEXX_RE.search(working)
    if combo:
        season = "S%02d" % int(combo.group(1))
        episode = "E%02d" % int(combo.group(2))
        if combo.group(3):
            episode += "-E%02d" % int(combo.group(3))
        cut_positions.append(combo.start())
    else:
        season_match = SEASON_RE.search(working)
        if season_match:
            season = "S%02d" % int(season_match.group(1))
            cut_positions.append(season_match.start())
        episode_match = EPISODE_RE.search(working)
        if episode_match:
            episode = "E%02d" % int(episode_match.group(1))
            if episode_match.group(2):
                episode += "-E%02d" % int(episode_match.group(2))
            cut_positions.append(episode_match.start())

    year = ""
    year_match = YEAR_RE.search(working)
    if year_match and year_match.start() > 0:
        year = year_match.group(1)
        cut_positions.append(year_match.start())

    resolutions = _unique([m.group(1).upper() if m.group(1).lower() in ("4k", "8k", "uhd", "fhd") else m.group(1).lower()
                           for m in RES_RE.finditer(working)])
    sources = _unique([_norm_token(m.group(1)) for m in SOURCE_RE.finditer(working)])
    codecs = _unique([re.sub(r"[\s._-]+", "", m.group(1)).upper() for m in CODEC_RE.finditer(working)])
    audio_codecs = _unique([re.sub(r"[\s._-]+", "", m.group(1)).upper() for m in AUDIO_CODEC_RE.finditer(working)])
    languages = _unique([
        LANG_ALIASES.get(m.group(1).lower(), re.sub(r"[\s._-]+", " ", m.group(1)).title())
        for m in LANG_RE.finditer(working)
    ])

    for regex in (RES_RE, SOURCE_RE, CODEC_RE):
        match = regex.search(working)
        if match:
            cut_positions.append(match.start())

    cut = min(cut_positions) if cut_positions else len(working)
    title = _clean_title(working[:cut])
    if len(title) < 2:
        title = _clean_title(working)[:80]
    if not title:
        title = "Unknown Title"

    quality = " ".join(resolutions[:1] + sources[:1])
    audio_parts = languages[:4] + audio_codecs[:2]

    return {
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
        "quality": quality,
        "codec": ", ".join(codecs[:3]),
        "audio": " | ".join(audio_parts),
        "raw": (raw or "")[:300],
    }


def _or_na(value: str) -> str:
    return value if value else "N/A"


def format_terabox_caption(meta: Dict[str, str], link: str) -> str:
    title = meta.get("title", "Unknown Title")
    if meta.get("year"):
        title = "%s (%s)" % (title, meta["year"])
    lines = [
        "🎬 Title: %s" % title,
        "📺 Season: %s" % _or_na(meta.get("season", "")),
        "🎞 Episode: %s" % _or_na(meta.get("episode", "")),
        "📀 Quality: %s" % _or_na(meta.get("quality", "")),
        "🧬 Codec: %s" % _or_na(meta.get("codec", "")),
        "🔊 Audio: %s" % _or_na(meta.get("audio", "")),
        "",
        "🔗 Link: %s" % link,
    ]
    return "\n".join(lines)


def format_stream_caption(meta: Dict[str, str], link: str, host: str) -> str:
    title = meta.get("title", "Unknown Title")
    if meta.get("year"):
        title = "%s (%s)" % (title, meta["year"])
    lines = [
        "🎬 Title: %s" % title,
        "📺 Season/Episode: %s %s" % (_or_na(meta.get("season", "")), meta.get("episode", "")),
        "📀 Quality: %s" % _or_na(meta.get("quality", "")),
        "🌐 Host: %s" % host,
        "",
        "▶️ Stream: %s" % link,
    ]
    return "\n".join(lines)


def format_rough_message(url: str, reason: str, source: str) -> str:
    return "⚠️ %s\n🔗 %s\n📍 Source: %s" % (reason, url, source)

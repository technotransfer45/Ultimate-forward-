from __future__ import annotations

import asyncio
import io
import logging
import re
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

URL_RE = re.compile(r"https?://\S+", re.I)

# Separator used by build_source() to pass "link context line" + "whole message" in one string.
SOURCE_SPLIT = "\n\u2063\u2063\n"

# --------------------------------------------------------------------------- cleanup regexes
TME_RE = re.compile(r"\b(?:t|telegram)\.me/\S+", re.I)
MENTION_RE = re.compile(r"(?<!\w)@\w+")
WWW_RE = re.compile(r"\bwww\.\S+", re.I)
BRACKET_RE = re.compile(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}")
EMOJI_RE = re.compile(
    "[\U0001F000-\U0001FAFF\u2190-\u21FF\u2300-\u23FF\u2460-\u24FF\u2500-\u27BF\u2900-\u297F"
    "\u2B00-\u2BFF\u3030\u303D\u3297\u3299\uFE00-\uFE0F\u200D\u20E3\u00A9\u00AE\u2122]"
)
EXT_RE = re.compile(r"\.(?:mkv|mp4|avi|mov|webm|m4v|ts|flv|wmv|mpg|mpeg|3gp|zip|rar|srt)\s*$", re.I)
SITE_PREFIX_RE = re.compile(r"^[\W_]*(?:www\.)?[a-z0-9-]+\.[a-z]{2,6}\s*[-–—|:]\s*", re.I)
SITE_SUFFIX_RE = re.compile(r"\s*[-–—|:]\s*(?:www\.)?[a-z0-9-]+\.[a-z]{2,6}\s*$", re.I)
NOISE_SITES_RE = re.compile(
    r"\b(?:yts|yify|rarbg|psa|galaxyrg|mkvcinemas|mkvking|vegamovies|vegamovie|moviesmod|hdhub4u|katmoviehd|"
    r"filmyzilla|filmywap|bolly4u|9xmovies|worldfree4u|skymovieshd|khatrimaza|cinevood|extramovies|mlwbd|"
    r"themoviesflix|desiremovies|toonsworld4all|1tamilmv|tamilmv|tamilblasters|moviezwap|jiorockers|filmyhit|"
    r"mp4moviez|okjatt|7starhd|bollyflix|moviesflix|hdmovie2|pagalmovies|cinemaza)\b",
    re.I,
)
LEADING_NUM_RE = re.compile(r"^\s*\d{1,2}\s*[.)\-:]\s+")
TITLE_LABEL_RE = re.compile(
    r"^[\W_]*(?:movie\s*name|series\s*name|file\s*name|title|name|movie|film|series|show|web\s*series)\s*[:=–—-]\s*",
    re.I,
)

# --------------------------------------------------------------------------- junk-line detection
HARD_START_RE = re.compile(
    r"^[\W_]*(?:quality|qualities|resolution|seasons?|episodes?|ep|streams?|streaming|hosts?|downloads?|"
    r"downloading|dl|links?|https?|www|t\.me|join|shar(?:e|ing)|audio|size|file\s*size|subtitles?|subs?|"
    r"powered\s+by|uploaded\s+by|click|watch\s+online|direct\s+link|mirror\s+link)\b",
    re.I,
)
LABELLED_RE = re.compile(
    r"^[\W_]*(?:language|languages|lang|genre|genres|cast|director|directors|rating|imdb|runtime|duration|format|"
    r"codec|source|type|info|information|note|notes|channel|group|backup|visit|follow|support|request|server|"
    r"mirror|uploader|uploaded|release|released|year|country|story|plot|description|synopsis|storyline|"
    r"screenshots?|trailer|sample|credits|encoded|ripped)\s*[:：=|\-–—]",
    re.I,
)
CONTAINS_JUNK_RE = re.compile(
    r"t\.me/|telegram|\bjoin\b|subscribe|click\s+here|powered\s+by|all\s+rights|\bhttps?://\S+\s*$", re.I
)
HEADLINE_NOISE_RE = re.compile(
    r"^[\W_]*(?:(?:new|latest|just|now)\s+(?:release|released|update|updated|upload|uploaded|added|arrival|"
    r"streaming|available|episodes?)|update|alert|trending|exclusive|breaking|uploaded|added|available\s+now|"
    r"watch\s+now|full\s+episodes?)\b[^\n]{0,24}$",
    re.I,
)
SUBTITLE_LINE_RE = re.compile(r"^[\W_]*(?:subtitles?|subs?|esubs?)\b", re.I)
GENERIC_TITLES = {
    "quality", "unknown", "unknown title", "link", "links", "download", "stream", "video", "movie", "series",
    "file", "hd", "hindi", "english", "audio", "host", "size", "season", "episode", "web series", "full movie",
}

# --------------------------------------------------------------------------- technical token regexes
SXXEXX_RE = re.compile(
    r"\bs(\d{1,2})\s?e(?:p)?\s?(\d{1,4})(?:\s?(?:-|–|to|&)\s?e?(?:p)?\s?(\d{1,4}))?\b", re.I
)
SEASON_RE = re.compile(
    r"\b(?:seasons?[\s:#-]*|s)(\d{1,2})(?:\s?(?:-|–|to|&)\s?(?:seasons?\s?|s)?(\d{1,2}))?\b", re.I
)
EPISODE_WORD_RE = re.compile(
    r"\b(?:episodes?|ep)[\s:#-]*(\d{1,4})(?:\s?(?:-|–|to|&)\s?(?:episodes?|ep|e)?\s?(\d{1,4}))?\b", re.I
)
EPISODE_E_RE = re.compile(r"\be(\d{1,4})(?:\s?(?:-|–|to)\s?e?(\d{1,4}))?\b", re.I)
YEAR_RE = re.compile(r"(?<!\d)(19[5-9]\d|20[0-3]\d)(?!\d)")
BRACKET_YEAR_RE = re.compile(r"[\(\[]\s*(19[5-9]\d|20[0-3]\d)\s*[\)\]]")
RES_RE = re.compile(r"(?<![\w])(?:(\d{3,4})\s?p|(4k|8k|uhd))(?![A-Za-z0-9])", re.I)
VALID_RESOLUTIONS = {144, 240, 360, 480, 540, 576, 720, 1080, 1440, 2160, 4320}
SOURCE_RE = re.compile(
    r"\b(web\s?-?\s?dl|webrip|blu\s?-?\s?ray|brrip|bdrip|hdrip|hdtv|dvdrip|dvdscr|hdcam|camrip|predvd|hdts|remux|"
    r"nf|amzn|dsnp|hmax|zee5|jc|atvp|hotstar|sonyliv)\b",
    re.I,
)
SOURCE_NAMES = {
    "webdl": "WEB-DL", "webrip": "WEBRip", "bluray": "BluRay", "brrip": "BRRip", "bdrip": "BDRip",
    "hdrip": "HDRip", "hdtv": "HDTV", "dvdrip": "DVDRip", "dvdscr": "DVDScr", "hdcam": "HDCAM",
    "camrip": "CAMRip", "predvd": "PreDVD", "hdts": "HDTS", "remux": "Remux",
}
CODEC_RE = re.compile(
    r"\b(x\s?26[45]|h\s?26[45]|hevc|avc|av1|vp9|10\s?-?\s?bit|8\s?-?\s?bit|hdr10\+?|hdr|dolby\s?vision|dv)\b", re.I
)
AUDIO_CODEC_RE = re.compile(
    r"\b(ddp?\s?\+?\s?\d\.\d|eac3|ac3|aac(?:\s?\d\.\d)?|atmos|dts(?:\s?-?\s?hd)?|truehd|opus|flac|mp3)\b", re.I
)
SIZE_RE = re.compile(r"\b\d+(?:\.\d+)?\s?(?:gb|mb|kb|tb|gib|mib)\b", re.I)
BITRATE_RE = re.compile(r"\b\d+(?:\.\d+)?\s?(?:kbps|mbps|fps)\b", re.I)
MULTI_RE = re.compile(r"\b(dual|multi)\s?-?\s?audio\b", re.I)
CUT_NOISE_RE = re.compile(
    r"\b(?:dual\s?-?\s?audio|multi\s?-?\s?audio|dubbed|e?subs?|msubs?|hc\s?sub|org|complete|batch|"
    r"web\s?-?\s?series|full\s?movie|free\s?download|watch\s?online|uncut)\b",
    re.I,
)

LANGUAGE_NAMES = (
    "hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi|gujarati|urdu|bhojpuri|odia|korean|"
    "japanese|chinese|mandarin|spanish|french|german|russian|turkish|thai|indonesian|arabic|italian|portuguese"
)
LANG_RE = re.compile(r"\b(" + LANGUAGE_NAMES + r"|hin|eng|tam|tel|mal|kan|kor|jpn)\b", re.I)
LANG_ALIASES = {
    "hin": "Hindi", "eng": "English", "tam": "Tamil", "tel": "Telugu", "mal": "Malayalam",
    "kan": "Kannada", "kor": "Korean", "jpn": "Japanese",
}
SUB_RE = re.compile(r"\b(?:(?:" + LANGUAGE_NAMES + r"|hin|eng)\s*)?(?:e|m|hc\s?)?sub(?:s|titles?)?\b", re.I)

SMALL_WORDS = {"THE", "AND", "OF", "IN", "TO", "A", "AN", "FOR", "ON", "AT", "BY", "MY", "OR", "VS"}


# --------------------------------------------------------------------------- helpers
def _normalize(text: str) -> str:
    """Drop emoji/URLs/mentions, turn '_' and decorative dots into spaces (keeps 5.1 / 2.0)."""
    value = EMOJI_RE.sub(" ", text or "")
    value = URL_RE.sub(" ", value)
    value = TME_RE.sub(" ", value)
    value = MENTION_RE.sub(" ", value)
    value = value.replace("_", " ")
    value = re.sub(r"(?<!\d)\.|\.(?!\d)", " ", value)
    return re.sub(r"[ \t]+", " ", value)


def _letter_count(text: str) -> int:
    return len(re.findall(r"[^\W\d_]", text))


def _strip_tech(text: str) -> str:
    """Remove every technical descriptor so we can tell if real title words remain."""
    value = text
    for regex in (
        SXXEXX_RE, SEASON_RE, EPISODE_WORD_RE, EPISODE_E_RE, RES_RE, SOURCE_RE, CODEC_RE, AUDIO_CODEC_RE,
        SUB_RE, LANG_RE, MULTI_RE, CUT_NOISE_RE, SIZE_RE, BITRATE_RE, YEAR_RE, NOISE_SITES_RE,
    ):
        value = regex.sub(" ", value)
    return value


def is_junk_line(line: str) -> bool:
    """True for lines like 'Quality: 480p', '1080 p', 'Download Link', 'Join @channel', 'Size: 1GB'."""
    stripped = EMOJI_RE.sub(" ", line or "").strip()
    if not stripped:
        return True
    if HARD_START_RE.match(stripped) or LABELLED_RE.match(stripped):
        return True
    if CONTAINS_JUNK_RE.search(stripped) or HEADLINE_NOISE_RE.match(stripped):
        return True
    residual = _strip_tech(_normalize(stripped))
    residual = BRACKET_RE.sub(" ", residual)
    return _letter_count(residual) < 3


def _first_after_start(regex: "re.Pattern[str]", text: str) -> Optional[int]:
    for match in regex.finditer(text):
        if match.start() > 0:
            return match.start()
    return None


def _cut_index(text: str) -> int:
    """Position where the title ends and technical/release noise begins."""
    positions: List[int] = []
    for regex in (
        SXXEXX_RE, SEASON_RE, EPISODE_WORD_RE, EPISODE_E_RE, YEAR_RE, RES_RE, SOURCE_RE, CODEC_RE,
        AUDIO_CODEC_RE, MULTI_RE, CUT_NOISE_RE, SIZE_RE,
    ):
        position = _first_after_start(regex, text)
        if position is not None:
            positions.append(position)
    for match in LANG_RE.finditer(text):
        if match.start() == 0:
            continue
        remainder = _strip_tech(text[match.start():])
        if _letter_count(remainder) < 3:
            positions.append(match.start())
            break
    return min(positions) if positions else len(text)


def _case_title(title: str) -> str:
    letters = [c for c in title if c.isalpha()]
    if not letters:
        return title
    if title.islower():
        return " ".join(word.capitalize() for word in title.split(" "))
    if title.isupper() and len(title) > 5:
        words = []
        for word in title.split(" "):
            if word in SMALL_WORDS:
                words.append(word.lower() if word not in ("A", "AN") else word.capitalize())
            elif len(word) <= 3 and word.isalpha():
                words.append(word)
            else:
                words.append(word.capitalize())
        return " ".join(words)
    return title


def _finish_title(raw_title: str) -> str:
    value = re.sub(r"[^\w\s&'’:+\-–!?,]", " ", raw_title)
    value = re.sub(r"\s+", " ", value).strip(" -–—|:,+&.")
    if _letter_count(value) < 2 and not re.search(r"\d{2,}", value):
        return ""
    if value.lower() in GENERIC_TITLES:
        return ""
    return _case_title(value)


def _year_in(text: str) -> str:
    bracketed = BRACKET_YEAR_RE.search(text)
    if bracketed:
        return bracketed.group(1)
    for match in YEAR_RE.finditer(text):
        if match.start() > 0:
            return match.group(1)
    return ""


def _title_from_line(line: str) -> Tuple[str, str]:
    value = EMOJI_RE.sub(" ", line or "")
    value = URL_RE.sub(" ", value)
    value = SITE_PREFIX_RE.sub("", value, count=1)
    value = SITE_SUFFIX_RE.sub("", value, count=1)
    value = WWW_RE.sub(" ", value)
    value = EXT_RE.sub("", value.strip())
    value = _normalize(value)
    value = TITLE_LABEL_RE.sub("", value, count=1)
    value = LEADING_NUM_RE.sub("", value)
    year = _year_in(value)
    value = BRACKET_RE.sub(" ", value)
    value = NOISE_SITES_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip()
    cut = _cut_index(value)
    title = _finish_title(value[:cut])
    return title, year


def find_title(text: str) -> Tuple[str, str]:
    """Return (title, year) from the first real header line, skipping junk lines."""
    lines = [line for line in re.split(r"[\r\n]+", text or "") if line.strip()]
    for line in lines:
        if TITLE_LABEL_RE.match(line):
            title, year = _title_from_line(line)
            if title:
                return title, year
    for line in lines:
        if is_junk_line(line):
            continue
        title, year = _title_from_line(line)
        if title:
            return title, year
    return "", ""


def extract_title_line(text: str) -> str:
    """Return the cleaned movie / series title found in a message ('' when none)."""
    return find_title(text)[0]


def build_source(context: str, text: str, file_name: str = "") -> str:
    """Combine the line next to a link with the whole message so quality/episode stay per-link."""
    body = (text or "").strip()
    if file_name:
        body = (body + "\n" + file_name).strip()
    context = (context or "").strip()
    if context:
        return context + SOURCE_SPLIT + body
    return body


def _split_source(raw: str) -> Tuple[str, str]:
    if SOURCE_SPLIT in raw:
        context, body = raw.split(SOURCE_SPLIT, 1)
        return context, body
    return "", raw


# --------------------------------------------------------------------------- value extractors
def _season_values(text: str) -> List[str]:
    values: List[str] = []
    for match in SXXEXX_RE.finditer(text):
        values.append("S%02d" % int(match.group(1)))
    for match in SEASON_RE.finditer(text):
        value = "S%02d" % int(match.group(1))
        if match.group(2):
            value += "-S%02d" % int(match.group(2))
        values.append(value)
    return values


def _episode_values(text: str) -> List[str]:
    values: List[str] = []
    for match in SXXEXX_RE.finditer(text):
        value = "E%02d" % int(match.group(2))
        if match.group(3):
            value += "-E%02d" % int(match.group(3))
        values.append(value)
    for regex in (EPISODE_WORD_RE, EPISODE_E_RE):
        for match in regex.finditer(text):
            value = "E%02d" % int(match.group(1))
            if match.group(2):
                value += "-E%02d" % int(match.group(2))
            values.append(value)
    return values


def _quality_values(text: str) -> List[str]:
    values: List[str] = []
    for match in RES_RE.finditer(text):
        if match.group(1):
            if int(match.group(1)) in VALID_RESOLUTIONS:
                values.append("%sP" % int(match.group(1)))
        else:
            values.append(match.group(2).upper())
    return values


def _source_values(text: str) -> List[str]:
    values: List[str] = []
    for match in SOURCE_RE.finditer(text):
        key = re.sub(r"[^a-z]", "", match.group(1).lower())
        values.append(SOURCE_NAMES.get(key, match.group(1).upper()))
    return values


def _codec_values(text: str) -> List[str]:
    values: List[str] = []
    for match in CODEC_RE.finditer(text):
        key = re.sub(r"[\s+]", "", match.group(1).lower())
        if key in ("x264", "x265"):
            values.append(key)
        elif key in ("h264", "h265"):
            values.append("H." + key[1:])
        elif key.startswith("hdr10"):
            values.append("HDR10+" if "+" in match.group(1) else "HDR10")
        elif key in ("dv", "dolbyvision"):
            values.append("Dolby Vision")
        elif key.endswith("bit"):
            values.append(key[:-3].replace("-", "") + "-bit")
        else:
            values.append(key.upper())
    return values


def _resolve(context_values: List[str], body_values: List[str]) -> str:
    """Prefer the link's own context line; fall back to the body only when it is unambiguous."""
    if context_values:
        return context_values[0]
    unique = list(dict.fromkeys(body_values))
    if len(unique) == 1:
        return unique[0]
    return ""


def _audio_summary(text: str) -> str:
    languages: List[str] = []
    flag = ""
    for line in _normalize(text).splitlines():
        if SUBTITLE_LINE_RE.match(line):
            continue
        cleaned = SUB_RE.sub(" ", line)
        for match in LANG_RE.finditer(cleaned):
            word = match.group(1).lower()
            name = LANG_ALIASES.get(word, word.capitalize())
            if name not in languages:
                languages.append(name)
        multi = MULTI_RE.search(cleaned)
        if multi and not flag:
            flag = "%s Audio" % multi.group(1).capitalize()
    joined = " + ".join(languages[:5])
    if joined and flag:
        return "%s (%s)" % (joined, flag)
    return joined or flag


def parse_metadata(raw: str) -> Dict[str, str]:
    """Parse title, year, season/episode, quality, codec and audio from a caption, file name or build_source() string."""
    raw = raw or ""
    context, body = _split_source(raw)
    combined = (context + "\n" + body) if context else body

    # The master title always comes from the first valid header line of the whole caption;
    # a per-link context line (e.g. 'EP 1') is only a last resort.
    title, year = find_title(body)
    if not title and context:
        title, year = find_title(context)
    if not title:
        title = "Unknown Title"
    if not year:
        bracketed = BRACKET_YEAR_RE.search(combined)
        year = bracketed.group(1) if bracketed else ""

    context_norm = _normalize(context)
    body_norm = _normalize(body)
    season = _resolve(_season_values(context_norm), _season_values(body_norm))
    episode = _resolve(_episode_values(context_norm), _episode_values(body_norm))
    quality = _resolve(_quality_values(context_norm), _quality_values(body_norm))
    source = _resolve(_source_values(context_norm), _source_values(body_norm))
    codec_values = list(dict.fromkeys(_codec_values(context_norm) + _codec_values(body_norm)))

    return {
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
        "season_episode": " ".join(part for part in (season, episode) if part),
        "quality": quality,
        "source": source,
        "codec": ", ".join(codec_values[:3]),
        "audio": _audio_summary(combined),
        "raw": raw.replace(SOURCE_SPLIT, "\n")[:300],
    }


# --------------------------------------------------------------------------- caption formatters
def _display_title(meta: Dict[str, str]) -> str:
    title = meta.get("title") or "Unknown Title"
    if meta.get("year"):
        return "%s (%s)" % (title, meta["year"])
    return title


def _meta_lines(meta: Dict[str, str], include_quality: bool = True) -> List[str]:
    lines: List[str] = ["🎬 Title: %s" % _display_title(meta)]
    if meta.get("season_episode"):
        lines.append("📺 Season/Episode: %s" % meta["season_episode"])
    if include_quality and meta.get("quality"):
        lines.append("📀 Quality: %s" % meta["quality"])
    if meta.get("codec"):
        lines.append("🧬 Codec: %s" % meta["codec"])
    if meta.get("audio"):
        lines.append("🔊 Audio: %s" % meta["audio"])
    return lines


def display_host(host: str) -> str:
    lowered = (host or "").lower()
    names = (
        ("streaam", "StreamNet"), ("streamnet", "StreamNet"), ("mdisk", "MDisk"), ("pdisk", "PDisk"),
        ("diskwala", "Diskwala"), ("dood", "DoodStream"), ("filemoon", "Filemoon"), ("streamtape", "Streamtape"),
        ("terabox", "TeraBox"),
    )
    for fragment, name in names:
        if fragment in lowered:
            return name
    return host or "Unknown"


def format_terabox_caption(meta: Dict[str, str], link: str) -> str:
    lines = _meta_lines(meta)
    lines.append("")
    lines.append("🔗 Link: %s" % link)
    return "\n".join(lines)


def format_stream_caption(meta: Dict[str, str], link: str, host: str) -> str:
    lines = _meta_lines(meta)
    lines.append("🌐 Host: %s" % display_host(host))
    lines.append("")
    lines.append("▶️ Link: %s" % link)
    return "\n".join(lines)


def format_rough_message(item: str, reason: str, origin: str) -> str:
    return "⚠️ %s\n🔗 %s\n📍 Source: %s" % (reason, item, origin)


def link_label(context: str) -> str:
    """Short per-link label such as 'EP 3 • 720P • 350MB' built from the line next to a link."""
    norm = _normalize(context)
    parts: List[str] = []
    episodes = _episode_values(norm)
    seasons = _season_values(norm)
    if episodes:
        numbers = re.findall(r"\d+", episodes[0])
        episode_label = "EP " + "-".join(str(int(n)) for n in numbers)
        if seasons and SXXEXX_RE.search(norm):
            episode_label = seasons[0] + " " + episode_label
        parts.append(episode_label)
    qualities = _quality_values(norm)
    if qualities:
        parts.append(qualities[0])
    sizes = [match.group(0).upper().replace(" ", "") for match in SIZE_RE.finditer(norm)]
    if sizes:
        parts.append(sizes[0])
    return " • ".join(parts)


def format_batch_caption(meta: Dict[str, str], entries: List[Tuple[str, str]], host: str, bullet: str) -> str:
    """One clean caption holding every link of a post: title block, then 'label: link' lines."""
    lines = _meta_lines(meta, include_quality=len(entries) == 1)
    if host:
        lines.append("🌐 Host: %s" % display_host(host))
    lines.append("")
    for label, link in entries:
        if label:
            lines.append("%s %s: %s" % (bullet, label, link))
        else:
            lines.append("%s %s" % (bullet, link))
    return "\n".join(lines)


# --------------------------------------------------------------------------- poster OCR
def _clean_ocr_candidate(text: str) -> str:
    candidate = re.sub(r"\s+", " ", text or "").strip()
    if not 3 <= len(candidate) <= 70:
        return ""
    if "@" in candidate or "http" in candidate.lower() or "www." in candidate.lower():
        return ""
    alpha = sum(1 for ch in candidate if ch.isalpha())
    if alpha < 3 or alpha / len(candidate) < 0.6:
        return ""
    if is_junk_line(candidate):
        return ""
    title, _ = _title_from_line(candidate)
    return title


def _ocr_sync(data: bytes, lang: str) -> str:
    try:
        import pytesseract
        from PIL import Image, ImageOps
    except ImportError as exc:
        logger.warning("OCR unavailable (install pytesseract and Pillow): %s", exc)
        return ""
    try:
        image = Image.open(io.BytesIO(data))
        image = ImageOps.exif_transpose(image).convert("L")
        longest = max(image.size)
        if longest > 2200:
            ratio = 2200.0 / longest
            image = image.resize((int(image.size[0] * ratio), int(image.size[1] * ratio)))
        elif longest < 900:
            image = image.resize((image.size[0] * 2, image.size[1] * 2))
        image = ImageOps.autocontrast(image)
    except Exception as exc:
        logger.warning("OCR could not open the poster image: %s", exc)
        return ""

    candidates: List[str] = []
    try:
        details = pytesseract.image_to_data(image, lang=lang, config="--psm 11", output_type=pytesseract.Output.DICT)
        grouped: Dict[Tuple[int, int, int], List[Tuple[str, float]]] = {}
        for index, word in enumerate(details.get("text", [])):
            word = (word or "").strip()
            try:
                confidence = float(details["conf"][index])
            except (TypeError, ValueError, KeyError, IndexError):
                confidence = -1.0
            if not word or confidence < 40:
                continue
            key = (int(details["block_num"][index]), int(details["par_num"][index]), int(details["line_num"][index]))
            grouped.setdefault(key, []).append((word, float(details["height"][index])))
        scored: List[Tuple[float, str]] = []
        for words in grouped.values():
            line_text = " ".join(word for word, _ in words)
            average_height = sum(height for _, height in words) / len(words)
            scored.append((average_height * min(len(line_text), 30), line_text))
        scored.sort(key=lambda item: item[0], reverse=True)
        candidates.extend(text for _, text in scored[:8])
    except Exception as exc:
        logger.info("OCR image_to_data failed, falling back to image_to_string: %s", exc)

    try:
        plain = pytesseract.image_to_string(image, lang=lang, config="--psm 6")
        candidates.extend(line for line in plain.splitlines() if line.strip())
    except Exception as exc:
        logger.warning("OCR image_to_string failed: %s", exc)

    for candidate in candidates:
        cleaned = _clean_ocr_candidate(candidate)
        if cleaned:
            return cleaned
    return ""


async def ocr_title_from_image(data: bytes, lang: str = "eng", timeout: float = 30.0) -> str:
    """Read the movie/series name off a poster image. Returns '' whenever OCR is unavailable or finds nothing."""
    if not data:
        return ""
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(loop.run_in_executor(None, _ocr_sync, data, lang), timeout)
    except asyncio.TimeoutError:
        logger.warning("OCR timed out after %.0fs", timeout)
        return ""
    except Exception as exc:
        logger.warning("OCR failed: %s", exc)
        return ""

import re

class MetadataParser:
    @staticmethod
    def parse_text(text: str) -> dict:
        meta = {
            "title": "Unknown Title",
            "type": "Movie",
            "season": None,
            "episode": None,
            "quality": None,
            "codec": None,
            "audio": None
        }
        if not text:
            return meta

        lines = [line.strip() for line in text.split("\n") if line.strip()]
        first_line = lines[0] if lines else ""

        # Extract Season and Episode
        se_pattern = re.search(r'(?i)(?:s|season\s*)(\d{1,2})(?:e|episode|\s*x)(\d{1,2})', text)
        if se_pattern:
            meta["type"] = "Series"
            meta["season"] = f"{int(se_pattern.group(1)):02d}"
            meta["episode"] = f"{int(se_pattern.group(2)):02d}"
        else:
            season_only = re.search(r'(?i)\b(?:season|s)\s*(\d{1,2})\b', text)
            if season_only:
                meta["type"] = "Series"
                meta["season"] = f"{int(season_only.group(1)):02d}"

        # Extract Quality / Resolution
        quality_pattern = re.search(
            r'(?i)\b(2160p|4k|1080p|720p|480p|360p|bluray|web-dl|webdl|hdrip|dvdrip|camrip)\b',
            text
        )
        if quality_pattern:
            meta["quality"] = quality_pattern.group(1).upper()

        # Extract Codec
        codec_pattern = re.search(r'(?i)\b(hevc|x265|h265|x264|h264|10bit|8bit)\b', text)
        if codec_pattern:
            meta["codec"] = codec_pattern.group(1).upper()

        # Extract Audio / Language
        audio_pattern = re.findall(
            r'(?i)\b(hindi|english|tamil|telugu|kannada|malayalam|bengali|marathi|dual[\s_-]*audio|multi[\s_-]*audio)\b',
            text
        )
        if audio_pattern:
            normalized_audios = []
            for item in audio_pattern:
                clean_audio = item.replace("_", " ").replace("-", " ").strip().title()
                if clean_audio not in normalized_audios:
                    normalized_audios.append(clean_audio)
            meta["audio"] = " + ".join(normalized_audios)

        # Title Extraction
        clean_title = re.split(
            r'(?i)\b(\d{4}|s\d{1,2}|season|1080p|720p|480p|2160p|bluray|web-dl|webdl|hevc|x264|x265)\b',
            first_line
        )[0]
        clean_title = re.sub(r'[\.\_\-\[\]\(\)\{\}\:\;]', ' ', clean_title)
        clean_title = " ".join(clean_title.split()).strip()

        if clean_title:
            meta["title"] = clean_title
        elif first_line:
            meta["title"] = first_line[:60]

        return meta

    @staticmethod
    def build_terabox_caption(meta: dict, link: str) -> str:
        icon = "📺" if meta["type"] == "Series" else "🎬"
        lines = [f"{icon} **{meta['title']}**", ""]

        if meta["type"] == "Series":
            lines.append("📺 **Type**: Series")
            if meta["season"]:
                lines.append(f"📅 **Season**: `{meta['season']}`")
            if meta["episode"]:
                lines.append(f"🎞️ **Episode**: `{meta['episode']}`")
        else:
            lines.append("📺 **Type**: Movie")

        if meta["quality"]:
            lines.append(f"📀 **Quality**: `{meta['quality']}`")
        if meta["codec"]:
            lines.append(f"⚙️ **Codec**: `{meta['codec']}`")
        if meta["audio"]:
            lines.append(f"🔊 **Audio**: `{meta['audio']}`")

        lines.append("")
        lines.append("🔗 **TeraBox Link**:")
        lines.append(link)
        return "\n".join(lines)

    @staticmethod
    def build_streaming_caption(meta: dict, link: str) -> str:
        lines = [f"🎬 **{meta['title']}**", ""]
        if meta["quality"]:
            lines.append(f"📀 **Quality**: `{meta['quality']}`")
        if meta["audio"]:
            lines.append(f"🔊 **Audio**: `{meta['audio']}`")
        lines.append("")
        lines.append("🔗 **Streaming Link**:")
        lines.append(link)
        return "\n".join(lines)

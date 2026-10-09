from database.sources_repo import SourcesRepo
from database.duplicates_repo import DuplicatesRepo
from database.stats_repo import StatsRepo

class LiveStatus:
    @staticmethod
    async def generate_status_report(workers_status: str = "Idle") -> str:
        tb_count = await SourcesRepo.count_by_type("terabox")
        tg_count = await SourcesRepo.count_by_type("telegram")
        hive_count = await SourcesRepo.count_by_type("terabox_hive")
        all_sources = await SourcesRepo.get_all_sources()

        pending_count = sum(1 for s in all_sources if s.get("status") == "pending")
        completed_count = sum(1 for s in all_sources if s.get("status") == "completed")
        in_progress_count = sum(1 for s in all_sources if s.get("status") == "processing")

        total_unique = await DuplicatesRepo.total_unique_items()
        metrics = await StatsRepo.get_metrics()

        report = (
            "📡 **LIVE ENGINE STATUS & WORKER METRICS**\n\n"
            "**System Pipeline:**\n"
            "• Forwarding: ▶️ Running\n"
            "• Ingestion: ▶️ Running\n"
            "• Converter Automation: ▶️ Running\n\n"
            f"**Configured Sources ({len(all_sources)} total):**\n"
            f"• Telegram TeraBox Sources: `{tb_count}`\n"
            f"• In-App Hive Sources: `{hive_count}`\n"
            f"• Standard Telegram Sources: `{tg_count}`\n"
            f"• Completed: `{completed_count}` | Active: `{in_progress_count}` | Pending: `{pending_count}`\n\n"
            f"**Dual Userbot Workers Status:**\n"
            f"`{workers_status}`\n\n"
            "**Metrics & Counters:**\n"
            f"• Total Scanned Messages: `{metrics['scanned']}`\n"
            f"• TeraBox Target Stored: `{metrics['terabox_stored']}`\n"
            f"• Final Converted Forwarded: `{metrics['converted_forwarded']}`\n"
            f"• Streaming Target Stored: `{metrics['streaming_stored']}`\n"
            f"• Media Target Forwarded: `{metrics['media_stored']}`\n"
            f"• Duplicates Filtered/Skipped: `{metrics['duplicates_skipped']}`\n"
            f"• Blocked/Dropped: `{metrics['filtered_out']}`\n"
            f"• Failures / Retries: `{metrics['failed']}`\n"
            f"• Total Unique DB Records: `{total_unique}`"
        )
        return report

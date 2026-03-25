"""
wipe_all_data.py

Clears all historical and processed data across every site.
Run this to reset after bad data gets committed.

Usage:
    python wipe_all_data.py          # wipe everything
    python wipe_all_data.py --today  # only rewrite today's entry from each history
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

SAST = timezone(timedelta(hours=2))
SITES_DIR = Path(__file__).parent / "sites"

def wipe_all():
    """Delete all data files across all sites."""
    count = 0
    for site_dir in sorted(SITES_DIR.iterdir()):
        data_dir = site_dir / "data"
        if not data_dir.is_dir():
            continue
        for f in data_dir.glob("*.json"):
            print(f"  🗑️  Deleting {f}")
            f.unlink()
            count += 1
    print(f"\n✅ Wiped {count} files across all sites.")
    print("   Next workflow run will start fresh.")


def wipe_today():
    """Remove only today's entry from each site's history.json and re-derive processed.json."""
    today = datetime.now(SAST).strftime("%Y-%m-%d")
    print(f"📅 Removing entries for {today} from all sites...\n")
    count = 0
    for site_dir in sorted(SITES_DIR.iterdir()):
        data_dir = site_dir / "data"
        history_file = data_dir / "history.json"
        if not history_file.exists():
            continue
        try:
            with open(history_file) as f:
                history = json.load(f)
            if today in history:
                del history[today]
                with open(history_file, "w") as f:
                    json.dump(history, f, indent=2)
                print(f"  ✅ {site_dir.name}: removed {today}")
                count += 1
            else:
                print(f"  ⏭️  {site_dir.name}: no entry for {today}")
        except Exception as e:
            print(f"  ❌ {site_dir.name}: error - {e}")

    # Also delete processed.json and alert_state.json so they get regenerated
    for site_dir in sorted(SITES_DIR.iterdir()):
        data_dir = site_dir / "data"
        for fname in ["processed.json", "alert_state.json"]:
            f = data_dir / fname
            if f.exists():
                f.unlink()

    print(f"\n✅ Removed today's data from {count} sites.")
    print("   Re-run the workflow to regenerate from the next scrape.")


if __name__ == "__main__":
    if "--today" in sys.argv:
        wipe_today()
    else:
        print("🧹 Wiping ALL data across all GoodWe sites...\n")
        wipe_all()

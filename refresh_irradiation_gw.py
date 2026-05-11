"""
refresh_irradiation.py

Lightweight script that refreshes today's irradiation data for all GoodWe sites.
Runs independently of the main scraper to ensure irradiation is never
left as zeros due to transient Open-Meteo API failures.

Usage:
    python refresh_irradiation.py
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests

SAST = timezone(timedelta(hours=2))
SITES_DIR = Path(__file__).parent / "sites"

SITE_COORDS = {
    "aurora":                    (-33.9600, 25.5700),
    "bmi-isuzu":                 (-33.8700, 25.5600),
    "wg-bloomingdales":          (-33.0100, 27.9100),
    "wg-circular-business-park": (-33.8700, 25.5500),
    "wg-cure-day-hospital":      (-33.9800, 25.5700),
    "wg-debi-lee-spar":          (-33.9600, 25.6100),
    "wg-gonubie-mall":           (-32.9300, 28.0200),
    "wg-heritage-mall":          (-33.0200, 27.8600),
    "wg-wellington-square":      (-33.3000, 27.2900),
}


def fetch_irradiation(date_str, lat, lon):
    """Fetch hourly irradiation from Open-Meteo with 3 retries and UTC→SAST shift."""
    for attempt in range(3):
        try:
            resp = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat, "longitude": lon,
                    "hourly": "shortwave_radiation",
                    "start_date": date_str, "end_date": date_str,
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise ValueError(f"API error: {data['error']}")
            irrad = data.get("hourly", {}).get("shortwave_radiation", [])
            while len(irrad) < 24:
                irrad.append(0)
            utc_data = [round(v if v else 0, 1) for v in irrad[:24]]
            result = [0.0] * 24
            for h in range(24):
                sast_h = h + 1
                if 0 <= sast_h <= 23:
                    result[sast_h] = utc_data[h]
            if sum(result) < 1:
                raise ValueError(f"Total irradiation is {sum(result):.1f} — likely API error")
            return result
        except Exception as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return None


def main():
    today = datetime.now(SAST).strftime("%Y-%m-%d")
    print(f"☀️  Irradiation refresh for {today}")

    if not SITES_DIR.exists():
        print(f"❌ Sites directory not found: {SITES_DIR}")
        sys.exit(1)

    updated = 0
    skipped = 0
    failed = 0

    for slug, (lat, lon) in sorted(SITE_COORDS.items()):
        site_dir = SITES_DIR / slug / "data"
        history_file = site_dir / "history.json"
        processed_file = site_dir / "processed.json"

        if not site_dir.exists():
            print(f"  ⏭️  {slug}: data dir not found — skipping")
            skipped += 1
            continue

        # Check if today's irradiation already looks good
        needs_update = False
        try:
            if processed_file.exists():
                with open(processed_file) as f:
                    proc = json.load(f)
                today_ir = proc.get("today", {}).get("irradiation", [])
                ir_sum = sum(today_ir) if today_ir else 0
                if ir_sum < 10:
                    needs_update = True
                    print(f"  🔄 {slug}: irradiation sum = {ir_sum:.0f} — needs update")
                else:
                    print(f"  ✅ {slug}: irradiation sum = {ir_sum:.0f} — OK")
                    skipped += 1
                    continue
            else:
                needs_update = True
        except Exception:
            needs_update = True

        if not needs_update:
            skipped += 1
            continue

        irrad = fetch_irradiation(today, lat, lon)
        if irrad is None:
            print(f"  ❌ {slug}: Open-Meteo failed after 3 retries")
            failed += 1
            time.sleep(1)
            continue

        print(f"  ☀️  {slug}: fetched irradiation sum = {sum(irrad):.0f} W/m²")

        # Update history.json
        try:
            history = {}
            if history_file.exists():
                with open(history_file) as f:
                    history = json.load(f)
            if today in history:
                history[today]["irradiation"] = irrad
                with open(history_file, "w") as f:
                    json.dump(history, f, indent=2)
        except Exception as e:
            print(f"  ⚠️  {slug}: could not update history.json: {e}")

        # Update processed.json
        try:
            if processed_file.exists():
                with open(processed_file) as f:
                    proc = json.load(f)
                if "today" in proc:
                    proc["today"]["irradiation"] = irrad
                if "irradiation" in proc:
                    proc["irradiation"] = irrad
                # Update avg from history
                if history_file.exists():
                    with open(history_file) as f:
                        hist = json.load(f)
                    avg_ir = [0.0] * 24
                    count = 0
                    for d in hist:
                        di = hist[d].get("irradiation", [])
                        if di and sum(di) > 0:
                            for h in range(min(24, len(di))):
                                avg_ir[h] += di[h]
                            count += 1
                    if count > 0:
                        avg_ir = [round(v / count, 1) for v in avg_ir]
                        if "stats_30day" not in proc:
                            proc["stats_30day"] = {}
                        proc["stats_30day"]["hourly_irrad_avg"] = avg_ir
                with open(processed_file, "w") as f:
                    json.dump(proc, f, indent=2)
        except Exception as e:
            print(f"  ⚠️  {slug}: could not update processed.json: {e}")

        updated += 1
        time.sleep(1)

    print(f"\n{'='*50}")
    print(f"☀️  Updated: {updated} | Skipped (OK): {skipped} | Failed: {failed}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()

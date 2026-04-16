"""
process_all_sites.py  —  GoodWe Multi-Site Processor

Parses the GoodWe Station Operation Report (single xlsx with all sites)
and produces per-site dashboard JSON files.

Status uses cumulative-hourly-avg logic, matching the dashboard "% of avg" badge.
Irradiation fetch has retry + sanity check to avoid flat zero lines.
"""

import json
import math
import sys
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# SITE REGISTRY — name must match exactly what appears in the xlsx
# =============================================================================
SITES = {
    "WG Bloomingdales": {
        "slug": "wg-bloomingdales",
        "lat": -33.97937646057296,
        "lon": 25.581219192561573,
    },
    "WG Wellington square": {
        "slug": "wg-wellington-square",
        "lat": -33.95370597693257,
        "lon": 22.467951860664744,
    },
    "WG Circular Business Park": {
        "slug": "wg-circular-business-park",
        "lat": -33.97937646057296,
        "lon": 25.581219192561573,
    },
    "WG Cure Day hospital": {
        "slug": "wg-cure-day-hospital",
        "lat": -32.948552600071515,
        "lon": 27.94150392199964,
    },
    "WG DEBI LEE SPAR": {
        "slug": "wg-debi-lee-spar",
        "lat": -32.948552600071515,
        "lon": 27.94150392199964,
    },
    "WG Gonubie Mall": {
        "slug": "wg-gonubie-mall",
        "lat": -32.948552600071515,
        "lon": 27.94150392199964,
    },
    "WG Heritage Mall": {
        "slug": "wg-heritage-mall",
        "lat": -33.58699986676595,
        "lon": 26.905759918384494,
    },
    "BMI Isuzu": {
        "slug": "bmi-isuzu",
        "lat": -33.91606455874616,
        "lon": 25.600899466686126,
    },
    "Aurora": {
        "slug": "aurora",
        "lat": -33.97937646057296,
        "lon": 25.581219192561573,
    },
}

# =============================================================================
# CONFIG
# =============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

PACE_THRESHOLD_PCT = 0.30
OFFLINE_THRESHOLD  = 0.01
HISTORY_DAYS       = 30
MIN_HISTORY_DAYS   = 7

# GoodWe data is already in SAST
REPORT_TZ_OFFSET   = 0

_HERE       = Path(__file__).parent
RAW_FILE    = _HERE / "data" / "raw_report.xlsx"
SITES_DIR   = _HERE / "sites"

SAST = timezone(timedelta(hours=2))


# =============================================================================
# Solar curve
# =============================================================================

def solar_window(month: int) -> tuple:
    mid_day   = (month - 1) * 30 + 15
    amplitude = 0.75
    angle     = 2 * math.pi * (mid_day - 355) / 365
    shift     = amplitude * math.cos(angle)
    return 6.0 - shift, 18.0 + shift


def solar_curve_fraction(hour: int, month: int) -> float:
    sunrise, sunset = solar_window(month)
    solar_day = sunset - sunrise
    if solar_day <= 0:
        return 0.0
    elapsed = (hour + 1) - sunrise
    if elapsed <= 0:
        return 0.0
    if elapsed >= solar_day:
        return 1.0
    return (1 - math.cos(math.pi * elapsed / solar_day)) / 2


# =============================================================================
# Irradiation — with retry & sanity check
# =============================================================================

def fetch_irradiation(date_str: str, lat: float, lon: float) -> list:
    """Fetch hourly irradiation from Open-Meteo with 3 retries and sanity check."""
    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat, "longitude": lon,
                    "hourly": "shortwave_radiation",
                    "timezone": "Africa/Johannesburg",
                    "start_date": date_str, "end_date": date_str,
                },
                timeout=20,
            )
            resp.raise_for_status()
            irrad = resp.json().get("hourly", {}).get("shortwave_radiation", [])
            while len(irrad) < 24:
                irrad.append(0)
            result = [round(v if v else 0, 1) for v in irrad[:24]]
            midday = sum(result[10:15])
            if midday < 10:
                raise ValueError(f"Midday irradiation suspiciously low: {midday:.1f} W/m²")
            return result
        except Exception as e:
            last_err = e
            print(f"    ⚠️  Irradiation fetch attempt {attempt+1}/3 failed: {e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    print(f"    ❌ Irradiation unavailable after 3 attempts: {last_err}")
    return [0] * 24


# =============================================================================
# Parse GoodWe xlsx
# =============================================================================

def parse_goodwe_report(filepath: Path) -> dict:
    df = pd.read_excel(filepath, header=None, sheet_name=0)
    print(f"  📐 Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} cols")

    report_date = None
    for row_idx in range(min(5, len(df))):
        cell = str(df.iloc[row_idx, 0]) if not pd.isna(df.iloc[row_idx, 0]) else ""
        if "Report Date:" in cell:
            date_part = cell.split("Report Date:")[1].strip()
            try:
                dt = datetime.strptime(date_part, "%d-%m-%Y")
                report_date = dt.strftime("%Y-%m-%d")
            except Exception:
                pass
            break
    if not report_date:
        report_date = datetime.now(SAST).strftime("%Y-%m-%d")
    print(f"  📅 Report date: {report_date}")

    header_row_idx = None
    hour_col_map = {}
    for row_idx in range(min(10, len(df))):
        row_vals = [str(v).strip() if not pd.isna(v) else "" for v in df.iloc[row_idx].tolist()]
        found_hours = {}
        for col_idx, val in enumerate(row_vals):
            if len(val) == 5 and val[2] == ':' and val[:2].isdigit() and val[3:].isdigit():
                hour_int = int(val[:2])
                if 0 <= hour_int <= 23:
                    found_hours[hour_int] = col_idx
        if len(found_hours) >= 12:
            header_row_idx = row_idx
            hour_col_map = found_hours
            break

    if header_row_idx is None or not hour_col_map:
        print("  ❌ Could not find hour column headers in xlsx!")
        return {}

    print(f"  📊 Header row: {header_row_idx} | {len(hour_col_map)} hour columns found")

    indicator_col = 1
    row_vals = [str(v).strip().lower() if not pd.isna(v) else "" for v in df.iloc[header_row_idx].tolist()]
    for col_idx, val in enumerate(row_vals):
        if val == "indicator":
            indicator_col = col_idx
            break

    results = {}
    data_start = header_row_idx + 1

    for i in range(data_start, len(df)):
        indicator = str(df.iloc[i, indicator_col]).strip() if not pd.isna(df.iloc[i, indicator_col]) else ""
        if indicator != "PV Power(kW)":
            continue

        station_info = str(df.iloc[i, 0]) if not pd.isna(df.iloc[i, 0]) else ""
        station_name = ""
        if "Station Name:" in station_info:
            station_name = station_info.split("Station Name:")[1].split("\n")[0].strip()

        if not station_name:
            continue

        raw_hourly = [0.0] * 24
        for hour_int, col_idx in hour_col_map.items():
            if col_idx < len(df.columns):
                val = df.iloc[i, col_idx]
                try:
                    raw_hourly[hour_int] = round(float(val), 4) if not pd.isna(val) else 0.0
                except (ValueError, TypeError):
                    raw_hourly[hour_int] = 0.0

        hourly = [0.0] * 24
        for hkt_hour in range(24):
            sast_hour = hkt_hour + REPORT_TZ_OFFSET
            if 0 <= sast_hour <= 23:
                hourly[sast_hour] = raw_hourly[hkt_hour]

        total = round(sum(hourly), 3)

        last_hour = 0
        for h in range(23, -1, -1):
            if hourly[h] > 0:
                last_hour = h
                break

        results[station_name] = {
            "date": report_date,
            "hourly": hourly,
            "total_kwh": total,
            "last_hour": last_hour,
        }

        nonzero = [(h, hourly[h]) for h in range(24) if hourly[h] > 0]
        nz_preview = ", ".join(f"{h:02d}:00={v:.1f}" for h, v in nonzero[:5])
        if len(nonzero) > 5:
            nz_preview += f" ... ({len(nonzero)} hrs total)"
        print(f"    ✅ {station_name}: {total:.1f} kWh | last: {last_hour:02d}:00 | {nz_preview}")

    return results


# =============================================================================
# History & stats
# =============================================================================

def load_history(history_file: Path) -> dict:
    if not history_file.exists():
        return {}
    try:
        with open(history_file) as f:
            return json.load(f)
    except Exception:
        return {}


def save_history(history: dict, history_file: Path):
    history_file.parent.mkdir(parents=True, exist_ok=True)
    cutoff = (datetime.now(SAST) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    history = {k: v for k, v in history.items() if k >= cutoff}
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)


def percentile(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    d = k - f
    return sorted_vals[f] + d * (sorted_vals[c] - sorted_vals[f])


def calculate_stats(history: dict, exclude_date: str = None) -> dict:
    empty = {
        "hourly_avg": [0]*24, "hourly_min": [0]*24, "hourly_max": [0]*24,
        "hourly_p10": [0]*24, "hourly_p25": [0]*24, "hourly_p75": [0]*24, "hourly_p90": [0]*24,
        "hourly_irrad_avg": [0]*24,
        "daily_min": 0, "daily_max": 0, "daily_avg": 0, "sample_days": 0,
    }
    if not history:
        return empty

    hourly_values = [[] for _ in range(24)]
    daily_totals = []

    for date, day in history.items():
        if date == exclude_date:
            continue
        hourly = day.get("hourly", [0]*24)
        total = day.get("total_kwh", 0)
        if total > 0:
            daily_totals.append(total)
            for h in range(24):
                if h < len(hourly):
                    hourly_values[h].append(hourly[h])

    if not daily_totals:
        return empty

    hourly_avg = [round(sum(v)/len(v), 2) if v else 0 for v in hourly_values]
    hourly_min = []
    for h in range(24):
        nz = [v for v in hourly_values[h] if v > 0]
        hourly_min.append(round(min(nz), 2) if nz else 0)
    hourly_max = [round(max(v), 2) if v else 0 for v in hourly_values]

    hourly_p10, hourly_p25, hourly_p75, hourly_p90 = [], [], [], []
    for h in range(24):
        sv = sorted(hourly_values[h])
        hourly_p10.append(round(percentile(sv, 10), 2))
        hourly_p25.append(round(percentile(sv, 25), 2))
        hourly_p75.append(round(percentile(sv, 75), 2))
        hourly_p90.append(round(percentile(sv, 90), 2))

    irrad_values = [[] for _ in range(24)]
    for date, day in history.items():
        if date == exclude_date:
            continue
        irrad = day.get("irradiation", [0]*24)
        total = day.get("total_kwh", 0)
        if total > 0:
            for h in range(24):
                if h < len(irrad):
                    irrad_values[h].append(irrad[h])

    hourly_irrad_avg = [round(sum(v)/len(v), 1) if v else 0 for v in irrad_values]

    return {
        "hourly_avg": hourly_avg, "hourly_min": hourly_min, "hourly_max": hourly_max,
        "hourly_p10": hourly_p10, "hourly_p25": hourly_p25,
        "hourly_p75": hourly_p75, "hourly_p90": hourly_p90,
        "hourly_irrad_avg": hourly_irrad_avg,
        "daily_min": round(min(daily_totals), 1),
        "daily_max": round(max(daily_totals), 1),
        "daily_avg": round(sum(daily_totals)/len(daily_totals), 1),
        "sample_days": len(daily_totals),
    }


# =============================================================================
# Status — CUMULATIVE HOURLY AVG (matches dashboard "% of avg")
# =============================================================================

def determine_status(data: dict, month: int, stats: dict, irradiation: list = None) -> tuple:
    total       = data["total_kwh"]
    hour        = data["last_hour"]
    sunrise, sunset = solar_window(month)
    alerts      = {"offline": False, "pace_low": False, "total_low": False}
    sample_days = stats.get("sample_days", 0)

    if hour < int(sunrise) or hour >= int(sunset):
        return "ok", alerts, {"reason": "nighttime", "sample_days": sample_days}

    if total < OFFLINE_THRESHOLD:
        alerts["offline"] = True
        return "offline", alerts, {"reason": "no generation during daylight", "sample_days": sample_days}

    curve_frac = solar_curve_fraction(hour, month)
    if curve_frac < 0.10:
        return "ok", alerts, {"reason": "too early", "sample_days": sample_days}

    if sample_days < MIN_HISTORY_DAYS:
        return "ok", alerts, {"reason": f"bootstrap ({sample_days}/{MIN_HISTORY_DAYS})", "sample_days": sample_days}

    hourly_avg      = stats.get("hourly_avg", [0] * 24)
    expected_by_now = sum(hourly_avg[:hour + 1])

    irrad_factor = 1.0
    if irradiation and stats.get("hourly_irrad_avg"):
        avg_irrad = stats["hourly_irrad_avg"]
        today_cum = sum(irradiation[:hour + 1])
        avg_cum   = sum(avg_irrad[:hour + 1])
        if avg_cum > 0:
            irrad_factor = max(min(today_cum / avg_cum, 1.5), 0.1)

    effective_expected = expected_by_now * irrad_factor
    pace_trigger       = effective_expected * PACE_THRESHOLD_PCT
    pct_of_avg         = round((total / expected_by_now) * 100) if expected_by_now > 0 else 0

    if total < pace_trigger:
        alerts["pace_low"] = True

    projected_total = total / curve_frac if curve_frac > 0 else 0
    daily_min       = stats.get("daily_min", 0)
    adjusted_min    = daily_min * irrad_factor if irrad_factor < 1.0 else daily_min
    if projected_total < adjusted_min:
        alerts["total_low"] = True

    status = "low" if (alerts["pace_low"] or alerts["total_low"]) else "ok"
    return status, alerts, {
        "pct_of_avg":         pct_of_avg,
        "expected_by_now":    round(expected_by_now, 1),
        "effective_expected": round(effective_expected, 1),
        "irrad_factor":       round(irrad_factor, 3),
        "actual_kwh":         round(total, 2),
        "pace_trigger":       round(pace_trigger, 1),
        "projected_total":    round(projected_total, 1),
        "daily_min":          daily_min,
        "curve_fraction":     round(curve_frac, 3),
        "sunrise":            round(sunrise, 2),
        "sunset":             round(sunset, 2),
        "sample_days":        sample_days,
    }


# =============================================================================
# Telegram
# =============================================================================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


def send_alerts(plant_name: str, status: str, alerts: dict, data: dict, debug: dict, state_file: Path):
    now_str     = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    total       = data["total_kwh"]
    hour        = data["last_hour"]
    sample_days = debug.get("sample_days", 0)

    if sample_days < MIN_HISTORY_DAYS and not alerts["offline"]:
        return

    prev_status = "ok"
    if state_file.exists():
        try:
            with open(state_file) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    if alerts["offline"]:
        send_telegram(
            f"🔴 <b>{plant_name} — OFFLINE</b>\n"
            f"No generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"🕐 {now_str}"
        )
    else:
        if alerts["pace_low"]:
            send_telegram(
                f"🟡 <b>{plant_name} — LOW PACE</b>\n"
                f"Actual: <b>{total:.1f} kWh</b> | Expected: <b>~{debug.get('expected_by_now',0):.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )
        if alerts["total_low"]:
            send_telegram(
                f"🟠 <b>{plant_name} — POOR DAY PROJECTED</b>\n"
                f"Projected: <b>~{debug.get('projected_total',0):.0f} kWh</b> | "
                f"Min: <b>{debug.get('daily_min',0):.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )
        if status == "ok" and prev_status in ("low", "offline"):
            send_telegram(
                f"✅ <b>{plant_name} — RECOVERED</b>\n"
                f"Total: <b>{total:.1f} kWh</b> (as of {hour:02d}:00)\n"
                f"🕐 {now_str}"
            )

    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing GoodWe multi-site report")

    if not RAW_FILE.exists():
        print(f"❌ Raw file not found: {RAW_FILE}")
        sys.exit(1)

    now   = datetime.now(SAST)
    month = now.month

    print(f"📥 Reading: {RAW_FILE}")
    all_sites = parse_goodwe_report(RAW_FILE)

    if not all_sites:
        print("❌ No PV Power data found in report")
        sys.exit(1)

    expected_names = set(SITES.keys())
    found_names    = set(all_sites.keys())
    missing_sites  = expected_names - found_names
    unknown_sites  = found_names - expected_names

    print(f"\n🔍 Site validation: {len(found_names)} found, {len(expected_names)} expected")

    if unknown_sites:
        print(f"  ⚠️  UNKNOWN sites in report ({len(unknown_sites)}):")
        for name in sorted(unknown_sites):
            print(f"       + {name}")
        send_telegram(
            f"⚠️ <b>GoodWe Report — New/Unknown Sites</b>\n"
            + "\n".join(f"  • {n}" for n in sorted(unknown_sites))
        )

    if missing_sites:
        print(f"\n  ⚠️  MISSING SITES ({len(missing_sites)}):")
        for name in sorted(missing_sites):
            print(f"       ⚠️  {name}")
        print(f"  ℹ️  Continuing with {len(found_names)} available sites...")

        send_telegram(
            f"⚠️ <b>GoodWe Report — Missing Sites</b>\n"
            f"Expected {len(expected_names)} sites but {len(missing_sites)} missing.\n\n"
            + "\n".join(f"  ⚠️ {n}" for n in sorted(missing_sites))
        )
    else:
        print(f"  ✅ All {len(expected_names)} expected sites present")

    print(f"\n📊 Processing {len(found_names)} sites...\n")

    for station_name, site_data in all_sites.items():
        config = SITES.get(station_name)
        if not config:
            print(f"  ⚠️  '{station_name}' not in SITES registry — skipping")
            continue

        slug = config["slug"]
        lat  = config["lat"]
        lon  = config["lon"]
        site_dir     = SITES_DIR / slug / "data"
        history_file = site_dir / "history.json"
        output_file  = site_dir / "processed.json"
        state_file   = site_dir / "alert_state.json"

        print(f"  ── {station_name} ({slug}) ──")

        irradiation = fetch_irradiation(site_data["date"], lat, lon)

        history = load_history(history_file)
        history[site_data["date"]] = {
            "total_kwh":    site_data["total_kwh"],
            "hourly":       site_data["hourly"],
            "irradiation":  irradiation,
            "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
            "last_hour":    site_data["last_hour"],
        }
        save_history(history, history_file)

        stats = calculate_stats(history, exclude_date=site_data["date"])
        status, alerts, debug = determine_status(site_data, month, stats, irradiation)

        print(f"    ⚡ {site_data['total_kwh']:.1f} kWh | Avg: {stats['daily_avg']:.1f} | "
              f"Days: {stats['sample_days']} | Status: {status.upper()}")

        send_alerts(station_name, status, alerts, site_data, debug, state_file)

        output = {
            "plant":        station_name,
            "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
            "date":         site_data["date"],
            "total_kwh":    site_data["total_kwh"],
            "last_hour":    site_data["last_hour"],
            "status":       status,
            "alerts":       alerts,
            "today": {
                "hourly_pv":   site_data["hourly"],
                "irradiation": irradiation,
            },
            "hourly_pv":   site_data["hourly"],
            "irradiation": irradiation,
            "stats_30day": stats,
            "history":     history,
            "thresholds": {
                "daily_avg":          stats["daily_avg"],
                "daily_min":          stats["daily_min"],
                "pace_threshold_pct": PACE_THRESHOLD_PCT,
                "sample_days":        stats["sample_days"],
                "min_history_days":   MIN_HISTORY_DAYS,
            },
            "debug": debug,
        }
        site_dir.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"    ✅ Saved: {output_file}")

    print(f"\n✅ All sites processed!")


if __name__ == "__main__":
    main()

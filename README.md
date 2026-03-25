# GoodWe Sites — PV Monitoring Dashboards

Multi-site PV monitoring dashboards for all GoodWe SEMS+ stations.

## Architecture

```
download_goodwe_report.py       ← GoodWe SEMS+ scraper (single xlsx for all sites)
process_all_sites.py            ← Parses xlsx → per-site JSON + alerts
data/
  raw_report.xlsx               ← Latest scraped report (all sites)
sites/
  wg-bloomingdales/
    index.html                  ← Dashboard (GitHub Pages)
    data/
      processed.json            ← Dashboard-ready JSON
      history.json              ← Rolling 30-day history
      alert_state.json          ← Telegram alert state
  wg-wellington-square/
    ...
  (9 sites total)
.github/workflows/
  scrape.yml                    ← Hourly cron
```

## Sites

| Site | Slug | Location |
|------|------|----------|
| WG Bloomingdales | wg-bloomingdales | -33.9794, 25.5812 |
| WG Wellington Square | wg-wellington-square | -33.9537, 22.4680 |
| WG Circular Business Park | wg-circular-business-park | -33.9794, 25.5812 |
| WG Cure Day Hospital | wg-cure-day-hospital | -32.9486, 27.9415 |
| WG Debi Lee Spar | wg-debi-lee-spar | -32.9486, 27.9415 |
| WG Gonubie Mall | wg-gonubie-mall | -32.9486, 27.9415 |
| WG Heritage Mall | wg-heritage-mall | -33.5870, 26.9058 |
| BMI Isuzu | bmi-isuzu | -33.9161, 25.6009 |
| Aurora | aurora | -33.9794, 25.5812 |

## Setup

1. Create GitHub repo, push via `git init` → `git push`
2. Add secrets: `GOODWE_USERNAME`, `GOODWE_PASSWORD`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
3. Enable GitHub Pages: Settings → Pages → Source: `main`, root
4. Each site dashboard is accessible at `https://<user>.github.io/<repo>/sites/<slug>/`

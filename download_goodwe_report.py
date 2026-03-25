"""
download_goodwe_report.py

Downloads the daily Station Operation Report from GoodWe SEMS+ portal.
Contains hourly PV Power data for all configured stations in a single xlsx.

Environment variables (set as GitHub secrets):
  GOODWE_USERNAME  - GoodWe SEMS+ email
  GOODWE_PASSWORD  - GoodWe SEMS+ password
"""

import time
import random
import os
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

# =============================================================================
# CONFIG
# =============================================================================
GOODWE_BASE = "https://hk-semsplus.goodwe.com"
LOGIN_URL   = f"{GOODWE_BASE}/#/login"
OUTPUT_FILE = Path(__file__).parent / "data" / "raw_report.xlsx"


def human_delay(min_s=2, max_s=5):
    delay = random.uniform(min_s, max_s)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


def download_goodwe_report():
    username = os.environ.get("GOODWE_USERNAME")
    password = os.environ.get("GOODWE_PASSWORD")
    if not username or not password:
        print("❌ GOODWE_USERNAME and GOODWE_PASSWORD must be set")
        sys.exit(1)

    print(f"🚀 Starting GoodWe SEMS+ download")
    print(f"🔐 Username: {username[:4]}***")
    print(f"📁 Output: {OUTPUT_FILE}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        print("🌐 Launching browser...")
        browser = playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Johannesburg",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = context.new_page()

        try:
            # ── Step 1: Login ──────────────────────────────────────────
            print("📱 Step 1: Navigating to GoodWe login...")
            page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
            human_delay(3, 5)

            print("👤 Step 2: Entering credentials...")
            page.get_by_role("textbox", name="Email").click()
            page.get_by_role("textbox", name="Email").fill(username)
            human_delay(1, 2)

            page.get_by_role("textbox", name="Password").click()
            page.get_by_role("textbox", name="Password").fill(password)
            human_delay(1, 2)

            # Accept terms checkbox
            print("  ☑️  Accepting terms...")
            page.get_by_role("checkbox", name="I have read and agreed to the").check()
            human_delay(0.5, 1)

            # Click Login
            page.get_by_role("button", name="Login").click()
            human_delay(3, 5)

            # Accept cookies if prompted
            try:
                page.get_by_role("button", name="Accept cookies").click(timeout=5000)
                print("  🍪 Accepted cookies")
                human_delay(1, 2)
            except Exception:
                print("  ℹ️  No cookie banner")

            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(5, 8)
            print(f"  📍 After login: {page.url[:80]}")

            # ── Step 2: Navigate to Report Center ──────────────────────
            print("📊 Step 3: Opening Report Center...")
            page.get_by_role("menuitem", name="Report Center").get_by_role("img").click()
            human_delay(3, 5)

            # Click Station Report
            print("  📋 Selecting Station Report...")
            page.get_by_text("Station ReportGeneration and").click()
            human_delay(3, 5)

            # ── Step 3: Select all stations ────────────────────────────
            print("🏢 Step 4: Selecting stations...")
            # Click checkboxes for all stations (indices 1-10 in the tree)
            page.locator(".ant-tree-checkbox-inner").first().click()
            human_delay(0.5, 1)
            for i in range(2, 11):
                try:
                    page.locator(f"div:nth-child({i}) > .ant-tree-checkbox > .ant-tree-checkbox-inner").click()
                    human_delay(0.3, 0.6)
                except Exception:
                    print(f"  ℹ️  Checkbox {i} not found (may be fewer stations)")
                    break
            human_delay(2, 3)

            # ── Step 4: Switch to Operational Report, 60 min ───────────
            print("⚙️  Step 5: Configuring report type...")
            page.get_by_text("Operational Report").click()
            human_delay(2, 3)

            page.get_by_text("5 min").click()
            human_delay(1, 2)

            page.get_by_text("60 min").click()
            human_delay(2, 3)

            # ── Step 5: Generate and Download ──────────────────────────
            print("📤 Step 6: Generating report...")
            # Click the generate/query button (the image button next to date)
            page.locator("div:nth-child(2) > .index-module_wrap_640bd > img").click()
            human_delay(3, 5)

            # Click Confirm if prompted
            try:
                page.get_by_role("button", name="Confirm").click(timeout=5000)
                human_delay(3, 5)
            except Exception:
                pass

            print("💾 Step 7: Downloading file...")
            with page.expect_download(timeout=60000) as dl_info:
                page.get_by_role("alert").get_by_text("Download", exact=True).click()

            download = dl_info.value
            download.save_as(OUTPUT_FILE)
            print(f"✅ Downloaded to: {OUTPUT_FILE}")

            human_delay(2, 3)
            print("✅ Download complete!")
            return str(OUTPUT_FILE)

        except Exception as err:
            print(f"❌ Download failed: {err}")
            try:
                page.screenshot(path="error_screenshot.png", full_page=True)
                Path("error_page.html").write_text(page.content())
                print("📸 Debug files saved: error_screenshot.png, error_page.html")
            except Exception:
                pass
            raise

        finally:
            human_delay(1, 2)
            context.close()
            browser.close()
            print("🔒 Browser closed")


if __name__ == "__main__":
    try:
        download_goodwe_report()
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)

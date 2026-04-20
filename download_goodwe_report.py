"""
download_goodwe_report.py

Downloads the daily Station Operation Report from GoodWe SEMS+ portal.
Searches for each station by name to ensure the correct sites are selected.

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
# ✏️  STATION LIST — Add or remove station names here
# =============================================================================
STATIONS = [
    "Aurora",
    "BMI Isuzu",
    "WG Bloomingdales",
    "WG Circular Business Park",
    "WG Cure Day hospital",
    "WG DEBI LEE SPAR",
    "WG Gonubie Mall",
    "WG Heritage Mall",
    "WG Wellington square",
]

GOODWE_BASE = "https://hk-semsplus.goodwe.com"
LOGIN_URL   = f"{GOODWE_BASE}/#/login"
OUTPUT_FILE = Path(__file__).parent / "data" / "raw_report.xlsx"


def human_delay(min_s=2, max_s=5):
    delay = random.uniform(min_s, max_s)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


def search_and_select_station(page, station_name):
    """Search for a station by name and tick its checkbox."""
    print(f"    🔎 Searching: '{station_name}'...")

    # Find search box with retry — it can disappear after selecting several stations
    search_box = None
    for attempt in range(3):
        try:
            search_box = page.get_by_role("textbox", name="Station Name")
            search_box.click(timeout=10000)
            break
        except Exception:
            if attempt < 2:
                print(f"    ⚠️  Search box not found (attempt {attempt+1}/3) — waiting...")
                human_delay(3, 5)
                # Try scrolling up or dismissing any overlay
                try:
                    page.keyboard.press("Escape")
                    human_delay(1, 2)
                except Exception:
                    pass
            else:
                print(f"    ❌ Search box unavailable for '{station_name}' — skipping")
                return

    search_box.fill("")
    human_delay(0.5, 1)
    search_box.fill(station_name)
    human_delay(1, 2)

    try:
        page.locator(".ant-input-suffix > .index-module_wrap_640bd > img").click()
    except Exception:
        try:
            page.locator(".ant-input-suffix img").first.click()
        except Exception:
            search_box.press("Enter")
    human_delay(3, 5)

    try:
        node = page.locator(".ant-tree-treenode").filter(has_text=station_name).first
        checkbox = node.locator(".ant-tree-checkbox-inner")
        checkbox.click(timeout=5000)
        print(f"    ✅ Selected: '{station_name}' (via tree node filter)")
    except Exception:
        try:
            checkboxes = page.locator(".ant-tree-checkbox:not(.ant-tree-checkbox-checked) .ant-tree-checkbox-inner")
            count = checkboxes.count()
            if count == 1:
                checkboxes.first.click(timeout=5000)
                print(f"    ✅ Selected: '{station_name}' (single unchecked)")
            else:
                node = page.locator(f"[title='{station_name}']").first
                parent = node.locator("xpath=ancestor::*[contains(@class,'ant-tree-treenode')]").first
                parent.locator(".ant-tree-checkbox-inner").click(timeout=5000)
                print(f"    ✅ Selected: '{station_name}' (via title attr)")
        except Exception:
            try:
                nodes = page.locator(".ant-tree-treenode")
                found = False
                for idx in range(nodes.count()):
                    node = nodes.nth(idx)
                    text = node.inner_text()
                    if station_name.lower() in text.lower():
                        node.locator(".ant-tree-checkbox-inner").click(timeout=3000)
                        print(f"    ✅ Selected: '{station_name}' (via text scan)")
                        found = True
                        break
                if not found:
                    raise Exception(f"No matching tree node found")
            except Exception as e:
                print(f"    ⚠️  Could not select '{station_name}': {e}")
                try:
                    page.screenshot(path=f"error_select_{station_name.replace(' ','_')}.png")
                except Exception:
                    pass

    human_delay(0.5, 1)


def download_goodwe_report():
    username = os.environ.get("GOODWE_USERNAME")
    password = os.environ.get("GOODWE_PASSWORD")
    if not username or not password:
        print("❌ GOODWE_USERNAME and GOODWE_PASSWORD must be set")
        sys.exit(1)

    print(f"🚀 Starting GoodWe SEMS+ download")
    print(f"🔐 Username: {username[:4]}***")
    print(f"📁 Output: {OUTPUT_FILE}")
    print(f"🏢 Stations to select: {len(STATIONS)}")
    for s in STATIONS:
        print(f"     • {s}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        print("\n🌐 Launching browser...")
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

            # Accept cookies if prompted
            try:
                page.get_by_role("button", name="Accept cookies").click(timeout=5000)
                print("  🍪 Accepted cookies")
                human_delay(1, 2)
            except Exception:
                print("  ℹ️  No cookie banner")

            print("👤 Step 2: Entering credentials...")
            page.get_by_role("textbox", name="Email").click()
            page.get_by_role("textbox", name="Email").fill(username)
            human_delay(1, 2)

            page.get_by_role("textbox", name="Password").click()
            page.get_by_role("textbox", name="Password").fill(password)
            human_delay(1, 2)

            try:
                page.get_by_role("checkbox", name="I have read and agreed to the").check()
                human_delay(0.5, 1)
            except Exception:
                pass

            page.get_by_role("button", name="Login").click()
            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(10, 15)

            # Verify login — wait for URL to change from /#/login
            for attempt in range(3):
                current_url = page.url
                print(f"  📍 URL check {attempt+1}: {current_url[:80]}")
                if "/login" not in current_url:
                    print("  ✅ Login successful")
                    break
                if attempt < 2:
                    print("  ⚠️  Still on login page — waiting...")
                    human_delay(8, 12)
                    # Try clicking Login again in case first click didn't register
                    try:
                        page.get_by_role("button", name="Login").click(timeout=3000)
                        page.wait_for_load_state("networkidle", timeout=30000)
                        human_delay(8, 12)
                    except Exception:
                        pass
            else:
                # Last resort: navigate directly to dashboard
                print("  ⚠️  Login may have failed — trying direct navigation...")
                page.goto(f"{GOODWE_BASE}/#/dashboard", wait_until="networkidle", timeout=60000)
                human_delay(5, 8)
                print(f"  📍 After direct nav: {page.url[:80]}")

            # ── Step 3: Navigate to Report Center ──────────────────────
            print("📊 Step 3: Opening Report Center...")

            # Click Report Center in the sidebar menu
            rc_found = False
            rc_selectors = [
                ("menuitem img", lambda: page.get_by_role("menuitem", name="Report Center").get_by_role("img").click()),
                ("menuitem click", lambda: page.get_by_role("menuitem", name="Report Center").click()),
                ("text exact", lambda: page.get_by_text("Report Center", exact=True).click()),
                ("text first", lambda: page.get_by_text("Report Center").first.click()),
                ("li has-text", lambda: page.locator("li:has-text('Report Center')").first.click()),
                ("a has-text", lambda: page.locator("a:has-text('Report Center')").first.click()),
                ("span has-text", lambda: page.locator("span:has-text('Report Center')").first.click()),
            ]
            for name, action in rc_selectors:
                try:
                    action()
                    print(f"  ✅ Report Center clicked via: {name}")
                    rc_found = True
                    break
                except Exception as e:
                    print(f"  ⏭️  '{name}' failed: {str(e)[:60]}")
                    continue

            if not rc_found:
                page.screenshot(path="error_report_center.png", full_page=True)
                raise RuntimeError("Could not click Report Center in menu")

            page.wait_for_load_state("networkidle", timeout=30000)
            human_delay(4, 6)
            print(f"  📍 URL after Report Center: {page.url[:80]}")

            # ── Step 4: Select Station Report ──────────────────────────
            print("  📋 Selecting Station Report...")

            # Wait for report page content to load
            human_delay(3, 5)

            sr_found = False
            sr_selectors = [
                ("text 'Station ReportGeneration'", lambda: page.get_by_text("Station ReportGeneration and").click()),
                ("text 'Station Report' exact", lambda: page.get_by_text("Station Report", exact=True).click()),
                ("text 'Station Report' first", lambda: page.get_by_text("Station Report").first.click()),
                ("locator card", lambda: page.locator("[class*='card']:has-text('Station Report')").first.click()),
                ("locator div", lambda: page.locator("div:has-text('Station Report')").nth(1).click()),
                ("first clickable card", lambda: page.locator("[class*='report'] [class*='card'], [class*='report'] [class*='item']").first.click()),
            ]
            for name, action in sr_selectors:
                try:
                    action()
                    print(f"  ✅ Station Report selected via: {name}")
                    sr_found = True
                    break
                except Exception as e:
                    print(f"  ⏭️  '{name}' failed: {str(e)[:60]}")
                    continue

            if not sr_found:
                page.screenshot(path="error_station_report.png", full_page=True)
                visible = page.locator("body").inner_text()[:300]
                print(f"  ❌ Page content: {visible}")
                raise RuntimeError("Could not find Station Report option")

            human_delay(4, 6)

            # ── Step 5: Select stations ────────────────────────────────
            print(f"🏢 Step 4: Selecting {len(STATIONS)} stations...")
            selected = []
            failed = []
            for station in STATIONS:
                try:
                    search_and_select_station(page, station)
                    selected.append(station)
                except Exception as e:
                    print(f"    ❌ Station '{station}' failed: {str(e)[:80]}")
                    failed.append(station)
                    # Try to recover page state
                    try:
                        page.keyboard.press("Escape")
                        human_delay(2, 3)
                    except Exception:
                        pass
                # Longer pause between stations to let page settle
                human_delay(1, 2)

            print(f"  📊 Selected: {len(selected)}/{len(STATIONS)}")
            if failed:
                print(f"  ⚠️  Failed: {', '.join(failed)}")
            if not selected:
                raise RuntimeError("No stations could be selected")

            # ── Step 6: Configure report ───────────────────────────────
            print("⚙️  Step 5: Configuring report...")
            page.get_by_text("Operational Report").click()
            human_delay(2, 3)

            page.get_by_text("5 min").click()
            human_delay(1, 2)

            page.get_by_text("60 min").click()
            human_delay(2, 3)

            # ── Step 7: Generate and Download ──────────────────────────
            print("📤 Step 6: Generating report...")
            page.locator("div:nth-child(2) > .index-module_wrap_640bd > img").click()
            human_delay(3, 5)

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

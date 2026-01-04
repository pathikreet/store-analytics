import os
import time
import random
import argparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(override=True)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Scrape store analytics data in batches.")
    parser.add_argument('--start', type=int, default=1, help='Starting store code index (e.g., 1 for A001)')
    parser.add_argument('--size', type=int, default=50, help='Number of stores to scrape in this batch')
    return parser.parse_args()

def main():
    """
    Logs into the Tumbledry website using Playwright to handle the complex session flow
    and WAF protections. Navigates to the store summary page and scrapes the data.
    """
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    if not username or not password or username == "your_username":
        print("Error: Please update the USERNAME and PASSWORD in the .env file.")
        return

    login_url = "https://simplifytumbledry.in/home/login"
    
    # Parse CLI arguments
    args = parse_arguments()

    with sync_playwright() as p:
        print("Launching browser...")
        # Headless mode is safer for servers, but set headless=False if debugging is needed
        browser = p.chromium.launch(headless=True)
        # Use a realistic User-Agent to match the curl success
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            # --- Step 1: Login ---
            print(f"Navigating to login page: {login_url}")
            page.goto(login_url)
            
            # Wait for form to be ready
            page.wait_for_selector("input[name='user_name']")

            print("Filling credentials...")
            # Use type instead of fill to mimic human typing
            page.click("input[name='user_name']")
            page.type("input[name='user_name']", username, delay=100)
            
            page.click("input[name='password']")
            page.type("input[name='password']", password, delay=100)
            
            # Verify inputs were filled
            user_val = page.input_value("input[name='user_name']")
            pass_val = page.input_value("input[name='password']")
            print(f"Verified inputs - User: {user_val}, Password: {'*' * len(pass_val)}")
            
            print("Submitting login form...")
            page.click("button[type='submit']")
            
            # Wait for navigation - use domcontentloaded which is faster, or verify URL change
            try:
                page.wait_for_url("**/home/dashboard", timeout=10000) # Assumption: goes to dashboard
            except:
                print("URL did not change to dashboard (or timeout).")
            
            page.wait_for_load_state('networkidle')
            print(f"Post-login URL: {page.url}")
            print(f"Post-login Title: {page.title()}")

            # --- Step 2: Access Support Hub (Session Activation) ---
            print("Looking for 'Support Hub' to activate session...")
            
            # Try multiple selectors
            support_hub_mw = page.locator("h5.card-title", has_text="Support Hub")
            if not support_hub_mw.count():
                 # Fallback to simple text
                 support_hub_mw = page.get_by_text("Support Hub", exact=False)

            if not support_hub_mw.count():
                print("Login failed or 'Support Hub' card not found.")
                print("Taking screenshot for usage analysis...")
                page.screenshot(path="debug_login.png")
                print("--- Page Text Content ---")
                print(page.inner_text("body"))
                print("-------------------------")
                return

            print("Found Support Hub. Clicking to open new session tab...")
            with context.expect_page() as new_page_info:
                support_hub_mw.click()
            
            session_page = new_page_info.value
            session_page.wait_for_load_state()
            print(f"Session activated. New tab URL: {session_page.url}")

            # --- Step 3: Batch Process Stores ---
            # Generate store codes based on CLI args
            start_index = args.start
            end_index = start_index + args.size
            stores_to_scrape = [f"A{i:03d}" for i in range(start_index, end_index)]
            
            print(f"Starting batch process for {len(stores_to_scrape)} stores: A{start_index:03d} to A{end_index-1:03d}")
            
            # Initialize DB execution
            from tinydb import TinyDB
            db = TinyDB('stores_db.json')
            
            # Add strict rate limiting
            for store_code in stores_to_scrape:
                print(f"\nProcessing Store: {store_code}")
                try:
                    rows_extracted = process_store(session_page, store_code, db)
                    
                    if rows_extracted > 0:
                        delay = random.uniform(2, 4)
                        print(f"Sleeping for {delay:.2f} seconds...")
                        time.sleep(delay)
                    else:
                        print("0 records found. Skipping delay.")
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"Failed to process {store_code}: {error_msg}")
                    if "ERR_NETWORK_IO_SUSPENDED" in error_msg:
                        print("CRITICAL: Network IO Suspended. Stopping batch to prevent further errors.")
                        break
            
            print("Batch processing complete.")

        except Exception as e:
            print(f"An error occurred: {e}")
            # Optionally take a screenshot on error
            # page.screenshot(path="error.png")
        finally:
            browser.close()


def process_store(session_page, store_code, db):
    target_url = f"https://tms.simplifytumbledry.in/mis/store_summary_yearly?store_code={store_code}"
    print(f"Navigating to: {target_url}")
    
    session_page.goto(target_url)
    try:
        session_page.wait_for_load_state("networkidle", timeout=30000)
    except:
        print("Timeout waiting for networkidle, proceeding...")

    html_content = session_page.content()
    
    if "Login" in session_page.title():
        raise Exception("Session Expired/Redirected to Login")

    page_soup = BeautifulSoup(html_content, 'html.parser')

    def get_text_from_selector(soup, selector, prefix):
        element = soup.select_one(selector)
        if element:
            return element.get_text(strip=True).replace(prefix, "").strip()
        return "Not found"

    store_name = get_text_from_selector(page_soup, "span.label-primary", "Store:")
    extracted_code = get_text_from_selector(page_soup, "span.label-info", "Code:")
    launch_date = get_text_from_selector(page_soup, "span.label-success", "Launch:")

    print(f"Store: {store_name}, Code: {extracted_code}")

    if extracted_code != "Not found" and extracted_code != store_code:
        print(f"Warning: Requested {store_code} but Page showed {extracted_code}")

    yearly_data = []
    table = page_soup.find('table', id='ticket-table')
    if not table:
        tables = page_soup.find_all('table', class_='dataTable')
        for t in tables:
                if t.find('tbody') and len(t.find('tbody').find_all('tr')) > 0:
                    table = t
                    break
    
    if table:
        header_table = page_soup.select_one(".dataTables_scrollHeadInner table")
        if header_table:
            headers = [th.text.strip() for th in header_table.find_all('th')]
        else:
            headers = [th.text.strip() for th in table.find_all('th')]
        
        headers = [h for h in headers if h]
        
        rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
        for row in rows:
            cols = [td.text.strip() for td in row.find_all('td')]
            if len(cols) == len(headers):
                row_data = dict(zip(headers, cols))
                yearly_data.append(row_data)
        
        print(f"Extracted {len(yearly_data)} rows.")
    else:
        print("Data table not found.")

    from tinydb import Query
    from datetime import datetime

    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut

    # --- Location & Status Logic ---
    city = "Unknown"
    state = "Unknown"
    
    # Determine Status
    # Logic:
    # - Closed: Store info missing ("Not found" or empty) BUT has data.
    # - Inactive: Store info missing AND no data.
    # - Active: Store info present.
    
    info_missing = (store_name == "Not found" or not store_name) or (extracted_code == "Not found" or not extracted_code)
    has_data = len(yearly_data) > 0
    
    if info_missing:
        status = "Closed" if has_data else "Inactive"
    else:
        status = "Active"

    if status == "Active":
        try:
            geolocator = Nominatim(user_agent="tumbledry_scraper_analytics")
            location = geolocator.geocode(store_name, addressdetails=True, timeout=5)
            if location:
                address = location.raw.get('address', {})
                city = address.get('city') or address.get('town') or address.get('village') or address.get('county') or "Unknown"
                state = address.get('state', "Unknown")
            else:
                # Fallback: Try cleaning the name (remove code/digits)
                clean_name = ''.join([i for i in store_name if not i.isdigit()]).strip()
                location = geolocator.geocode(clean_name, addressdetails=True, timeout=5)
                if location:
                    address = location.raw.get('address', {})
                    city = address.get('city') or address.get('town') or address.get('village') or "Unknown"
                    state = address.get('state', "Unknown")
        except Exception as e:
            print(f"Geocoding error for {store_name}: {e}")

    # --- Save Record ---
    Store = Query()
    store_record = {
        "store_code": store_code,
        "store_name": store_name,
        "status": status,
        "launch_date": launch_date,
        "last_updated_at": datetime.now().isoformat(),
        "yearly_data": yearly_data
    }
    
    # Only update city/state if we have valid data
    # This preserves existing DB values if we failed to fetch new ones (e.g. store is closed)
    # and avoids an extra DB read.
    if city != "Unknown":
        store_record["city"] = city
    if state != "Unknown":
        store_record["state"] = state
    
    db.upsert(store_record, Store.store_code == store_code)
    print(f"Saved {store_code}. Status: {status}, Location: {city}, {state}")
    return len(yearly_data)

if __name__ == "__main__":
    main()

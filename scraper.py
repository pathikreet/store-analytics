import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

def main():
    """
    This script logs into the Tumble Dry website, navigates to the TMS Support Hub
    to establish a session, then navigates to a specific store's summary page to
    scrape its information and yearly summary data.

    **IMPORTANT:**
    This script makes several assumptions about the HTML structure of the target page.
    If the script fails to extract the data correctly, you may need to provide me with the
    HTML source of the page so I can adjust the parsing logic. To get the HTML source:
    1. Log in to the website in your browser.
    2. Navigate to the page you want to scrape.
    3. Right-click anywhere on the page and select "View Page Source" or "Inspect".
    4. Copy the entire HTML content and provide it to me.
    """
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    if not username or not password or username == "your_username":
        print("Error: Please update the USERNAME and PASSWORD in the .env file.")
        return

    login_url = "https://simplifytumbledry.in/home/login"
    # The support hub URL is needed to establish the session on the tms subdomain.
    support_hub_url = "https://tms.simplifytumbledry.in/client/tickets"
    target_url = "https://tms.simplifytumbledry.in/mis/store_summary_yearly?store_code=A673"

    with requests.Session() as session:
        try:
            # --- Step 1: Get the main login page to find the CSRF token ---
            print(f"Attempting to get login page: {login_url}")
            login_page_response = session.get(login_url)
            login_page_response.raise_for_status()
            soup = BeautifulSoup(login_page_response.text, 'html.parser')
            print("Login page fetched successfully.")

            # Assumption: The CSRF token is in an input field with the name 'csrf_token'.
            csrf_token_input = soup.find('input', {'name': 'csrf_token'})
            csrf_token = csrf_token_input['value'] if csrf_token_input else ''

            if not csrf_token:
                print("Could not find CSRF token. Trying to log in without it.")

            # --- Step 2: Send the POST request to log in ---
            login_data = {
                'username': username,
                'password': password,
                'csrf_token': csrf_token,
                'submit': 'Login' # Assumption: The submit button has the name 'submit'.
            }

            print("Sending login request...")
            login_response = session.post(login_url, data=login_data)
            login_response.raise_for_status()

            # Check if login was successful by looking for the "Support Hub" card.
            soup = BeautifulSoup(login_response.text, 'html.parser')
            support_hub_card = soup.find('h5', class_='card-title', string='Support Hub')

            if not support_hub_card:
                print("Login failed. Could not find 'Support Hub' card. Please check your credentials.")
                return

            print("Login successful!")

            # --- Step 3: Navigate to the Support Hub to activate the session ---
            print(f"Navigating to Support Hub to establish session: {support_hub_url}")
            session.get(support_hub_url).raise_for_status()
            print("Session established.")

            # --- Step 4: Fetch the target page ---
            print(f"Fetching target page: {target_url}")
            target_page_response = session.get(target_url)
            target_page_response.raise_for_status()
            print("Target page fetched successfully.")

            # --- Step 5: Parse the target page content ---
            page_soup = BeautifulSoup(target_page_response.text, 'html.parser')

            print("\n--- Store Information ---")
            store_name_label = page_soup.find(text="Store")
            store_code_label = page_soup.find(text="Code")
            launch_date_label = page_soup.find(text="Launch")

            store_name = store_name_label.find_next().text.strip() if store_name_label else "Not found"
            store_code = store_code_label.find_next().text.strip() if store_code_label else "Not found"
            launch_date = launch_date_label.find_next().text.strip() if launch_date_label else "Not found"

            print(f"Store: {store_name}")
            print(f"Code: {store_code}")
            print(f"Launch: {launch_date}")


            print("\n--- Yearly Summary ---")
            table = page_soup.find('table')
            if table:
                headers = [th.text.strip() for th in table.find_all('th')]
                print(", ".join(headers))

                rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')[1:]
                for row in rows:
                    cols = [td.text.strip() for td in row.find_all('td')]
                    print(", ".join(cols))
            else:
                print("Could not find the data table on the page.")

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

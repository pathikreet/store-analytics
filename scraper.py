import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

def main():
    """
    This script logs into the Tumble Dry MIS website, navigates to a specific
    store's summary page, and scrapes the store's information and yearly summary data.

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
        print("You can do that by running: nano .env")
        return

    login_url = "https://tms.simplifytumbledry.in/mis/"
    target_url = "https://tms.simplifytumbledry.in/mis/store_summary_yearly?store_code=A673"

    with requests.Session() as session:
        try:
            # First, get the login page to get the CSRF token
            print("Attempting to get login page...")
            login_page_response = session.get(login_url)
            login_page_response.raise_for_status()
            soup = BeautifulSoup(login_page_response.text, 'html.parser')
            print("Login page fetched successfully.")

            # Assumption: The CSRF token is in an input field with the name 'csrf_token'.
            csrf_token_input = soup.find('input', {'name': 'csrf_token'})
            csrf_token = csrf_token_input['value'] if csrf_token_input else ''

            if not csrf_token:
                print("Could not find CSRF token. Trying to log in without it.")

            # Prepare the login data
            login_data = {
                'username': username,
                'password': password,
                'csrf_token': csrf_token,
                'submit': 'Login' # Assumption: The submit button has the name 'submit' and value 'Login'.
            }

            # Send the POST request to log in
            print("Sending login request...")
            login_response = session.post(login_url, data=login_data)
            login_response.raise_for_status()

            # Check if login was successful
            if "dashboard" not in login_response.text.lower() and login_response.url == login_url:
                print("Login failed. Please check your credentials.")
                # You can uncomment the following line to see the full response for debugging
                # print(login_response.text)
                return

            print("Login successful!")

            # Fetch the target page
            print(f"Fetching target page: {target_url}")
            target_page_response = session.get(target_url)
            target_page_response.raise_for_status()
            print("Target page fetched successfully.")

            # Parse the target page content
            page_soup = BeautifulSoup(target_page_response.text, 'html.parser')

            # --- Data Extraction ---

            # Assumption: Store details are in a specific structure.
            # I'll look for specific text labels and then get the next piece of text.
            # This is a bit fragile and might need adjustment.

            print("\n--- Store Information ---")
            store_name_label = page_soup.find(text="Store Name")
            store_code_label = page_soup.find(text="Code")
            launch_date_label = page_soup.find(text="Launch Date")

            store_name = store_name_label.find_next().text.strip() if store_name_label else "Not found"
            store_code = store_code_label.find_next().text.strip() if store_code_label else "Not found"
            launch_date = launch_date_label.find_next().text.strip() if launch_date_label else "Not found"

            print(f"Store Name: {store_name}")
            print(f"Code: {store_code}")
            print(f"Launch Date: {launch_date}")


            # Assumption: The data is in the first `<table>` on the page.
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

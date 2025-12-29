# Store Analytics Scraper

This project contains a Python script to scrape data from the Tumble Dry MIS website.

## Setup

1.  **Install Dependencies:**

    Install the required Python libraries using pip:

    ```bash
    pip install -r requirements.txt
    ```

2.  **Create a `.env` file:**

    Create a `.env` file in the root of the project and add your credentials:

    ```
    USERNAME="your_username"
    PASSWORD="your_password"
    ```

    Replace `"your_username"` and `"your_password"` with your actual login credentials.

## Usage

Once you have set up the environment, you can run the script from your terminal:

```bash
python3 scraper.py
```

The script will then log in, navigate to the specified page, and print the scraped data to the console.

## Troubleshooting

The script makes several assumptions about the HTML structure of the target page. If the script fails to extract the data correctly, you may need to provide the HTML source of the page to the developer so they can adjust the parsing logic.

To get the HTML source:

1.  Log in to the website in your browser.
2.  Navigate to the page you want to scrape.
3.  Right-click anywhere on the page and select "View Page Source" or "Inspect".
4.  Copy the entire HTML content and provide it to the developer.

# Marianos Product Scraper

This script scrapes product information from Marianos.com for a specific store location.

## Setup

1. Ensure you have Python 3.7+ installed on your system.

2. Clone the repository:

```bash
git clone https://github.com/your-username/marianos-product-scraper.git
cd marianos-product-scraper
```

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

4. Make sure you have the `promo_validator.py` file in the same directory as `marianos.py`.

## Running the Script

1. Open a terminal or command prompt.

2. Navigate to the directory containing `marianos.py`.

3. Run the script using Python:

```bash
python marianos.py
```

4. The script will start scraping product information for the store with postal code 60453.

5. Progress will be logged in the console, showing the number of products processed.

6. Once complete, the results will be saved in a CSV file named `marianos_YYYY-MM-DD.csv` in the same directory, where YYYY-MM-DD is the current date.

Note: The scraping process may take a while depending on the number of products and your internet connection speed.

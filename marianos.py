import asyncio
import aiohttp
import json
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import time
import logging
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOTAL_PROCESSED = 0

async def get_store_details(search_postal_code):
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'device-memory': '8',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }

    params = {
        'filter.query': search_postal_code,
        'projections': 'full',
    }

    async with aiohttp.ClientSession() as session:
        async with session.get('https://www.marianos.com/atlas/v1/stores/v2/locator', params=params, headers=headers) as response:
            data_dict = await response.json()
    
    stores = data_dict['data']['stores']
    result_data = [i for i in stores if "11000 S Cicero Ave" in i['locale']['address']['addressLines']][0]
    
    logger.info(f"Store details retrieved for postal code: {search_postal_code}")
    return {
        "loyaltyDivisionNumber": result_data['loyaltyDivisionNumber'],
        "postalCode": result_data['locale']['address']['postalCode'],
        "store_name": result_data['locale']['address']['name'],
        "store_location": ", ".join(result_data['locale']['address']['addressLines']) + ", " + result_data['locale']['address']['cityTown'] + ", " + result_data['locale']['address']['stateProvince'] + ", " + result_data['locale']['address']['countryCode'],
        "location": result_data['locale']['location'],
        "locationId": result_data['locationId'],
        'storeNumber': result_data['storeNumber']
    }

async def get_product_urls():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    }
    async with aiohttp.ClientSession() as session:
        async with session.get("https://www.marianos.com/product-details-sitemap.xml", headers=headers) as response:
            text = await response.text()
    
    soup = BeautifulSoup(text, 'html.parser')
    details_links = [i.text for i in soup.find_all("loc")]
    
    async def fetch_urls(link):
        async with aiohttp.ClientSession() as session:
            async with session.get(link, headers=headers) as response:
                text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            return [i.text for i in soup.find_all("loc")]
    
    tasks = [fetch_urls(link) for link in details_links]
    pc_urls = await asyncio.gather(*tasks)
    pc_urls = [url for sublist in pc_urls for url in sublist]
    
    pc_urls = list(set(pc_urls))
    upcs = [i.split("/")[-1] for i in pc_urls]
    logger.info(f"Retrieved {len(upcs)} unique product URLs")
    return list(set(upcs))

async def get_product_data(upc_list, store_dict, headers):
    params = {
        'filter.gtin13s': upc_list,
        'filter.verified': 'true',
        'projections': 'items.full,offers.compact,variantGroupings.compact',
    }
    
    async with aiohttp.ClientSession() as session:
        for _ in range(5):
            try:
                async with session.get('https://www.marianos.com/atlas/v1/product/v2/products', params=params, headers=headers, timeout=30) as response:
                    response.raise_for_status()
                    products_dict = await response.json()
                    return products_dict['data']['products']
            except Exception as e:
                logger.error(f"Error retrieving product data. Retrying...")
                await asyncio.sleep(10)
    
    logger.error("Failed to retrieve product data after 5 attempts")
    return []

def process_product(product, store_dict): 
    with open("test.json", "w") as f:
        json.dump(product, f, indent=4)   
    modality_availabilities = [i for i in product['sourceLocations'] if i['id'] == store_dict['locationId']][0]['modalityAvailabilities']
    available_list = [i['modalityType'] for i in modality_availabilities if i['availability'] and any(x in str(i).lower() for x in ['delivery', 'in_store', 'pickup'])]
    
    brand = product['item'].get('brand', {}).get('name', "")
    upc = product['item']['upc']
    url = f"https://www.marianos.com/p/{product['item']['seoDescription']}/{upc}"
    taxonomies = product['item']['taxonomies']
    category = taxonomies[0]['department']['name']
    sub_category = taxonomies[0]['commodity']['name']
    product_title = product['item']['description']
    weight = product['item'].get('weight', "Approx")
    customer_facing_size = product['item']['customerFacingSize']
    
    price_dict = product.get('price', {})
    regular_price = price_dict.get('storePrices', {}).get('regular', {}).get('price', '').replace('USD', '').strip()
    sale_price = price_dict.get('storePrices', {}).get('promo', {}).get('price', '').replace('USD', '').strip()
    sale_price = sale_price if sale_price != regular_price else ""
    promo_description = price_dict.get('storePrices', {}).get('promo', {}).get('defaultDescription', '') or price_dict.get('storePrices', {}).get('regular', {}).get('defaultDescription', '')
    
    image_url = next((i['url'] for size in ['xlarge', 'large', 'medium', 'small'] 
                      for i in product['item'].get('images', []) 
                      if i['perspective'] == 'front' and i['size'] == size), 
                     next((i['url'] for i in product['item'].get('images', []) if i['perspective'] == 'top' and i['size'] == 'xlarge'), 
                          product['item'].get('images', [{}])[0].get('url', "") if product['item'].get('images') else ""))
    
    price_offer_code = price_dict.get('offerCode', '')
    crawl_date = str(datetime.now().date())
    
    if promo_description:
    
        product_data = {
            "zipcode": store_dict['postalCode'],
            "store_name": store_dict['store_name'],
            "store_location": store_dict['store_location'],
            "store_logo": "https://www.marianos.com/content/v2/binary/image/marianos_svg_logo-desktop-1556242659761.svg",
            "category": category,
            "sub_category": sub_category,
            "product_title": product_title,
            "weight": weight,
            "regular_price": regular_price,
            "sale_price": sale_price,
            "volume_deals_description": promo_description,
            "volume_deals_price": "",
            "unit_price": "",
            "image_url": image_url,
            "url": url,
            "upc": upc,
            "crawl_date": crawl_date
        }
        return product_data


async def process_batch(batch, store_dict, headers, output_filename):
    global TOTAL_PROCESSED
    products_data_list = await get_product_data(batch, store_dict, headers)
    
    with ThreadPoolExecutor() as executor:
        product_infos = list(executor.map(lambda p: process_product(p, store_dict), products_data_list))
        product_infos = [info for info in product_infos if info is not None]
    
    info_df = pd.DataFrame(product_infos)
        
    with open(output_filename, 'a', newline='', encoding='utf-8') as f:
        info_df.to_csv(f, mode='a', header=f.tell() == 0, index=False)
    
    TOTAL_PROCESSED += len(batch)
    logger.info(f"Processed {TOTAL_PROCESSED} products")

async def scrape(crawl=True):
    output_filename = Path(f"marianos_raw_{datetime.now().date()}.csv")
    if not crawl and output_filename.exists():
        return output_filename
    
    if output_filename.exists():
        output_filename.unlink()
    
    search_postal_code = "60453"
    store_dict = await get_store_details(search_postal_code)
    upc_list = await get_product_urls()
    
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'device-memory': '8',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'x-laf-object': json.dumps([
            {
                "fallbackFulfillment": store_dict['locationId'],
                "createdDate": int(time.time() * 1000),
                "destination": {"locationId": store_dict['locationId']},
                "id": "d31b795d-d3af-4ca5-ad2d-6f547b7213ad",
                "isCrossBanner": False,
                "modalityType": "PICKUP",
                "fulfillment": [store_dict['locationId']],
                "isTrustedSource": False
            },
            {
                "fallbackDestination": store_dict['locationId'],
                "createdDate": int(time.time() * 1000),
                "destination": {
                    "address": {"postalCode": store_dict['postalCode']},
                    "location": store_dict['location']
                },
                "id": "f86c2b91-a597-493f-a9c9-2b28daa9f64f",
                "fallbackFulfillment": "491DC001",
                "modalityType": "SHIP",
                "source": "SHIP_AUTOGEN",
                "fulfillment": ["491DC001", "309DC309", "310DC310", "DSV00001", "MKTPLACE"],
                "isTrustedSource": False
            }
        ])
    }


    batch_size = 200
    tasks = []
    for i in range(0, len(upc_list), batch_size):
        batch = upc_list[i:i+batch_size]
        tasks.append(process_batch(batch, store_dict, headers, output_filename))
    
    await asyncio.gather(*tasks)
    logger.info(f"Processed all {len(upc_list)} products")
    return output_filename

if __name__ == "__main__":
    asyncio.run(scrape())
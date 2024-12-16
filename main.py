import asyncio
import pandas as pd
from marianos import scrape
from get_coupon import get_coupons
import json
import re
import random
from datetime import datetime
from promo_processor import PromoProcessor
from utils import reformat_data
from pathlib import Path



def remove_invalid_promos(description):
    description = re.sub(r'\$\d+\.\d+/lb', '', description)
    description = re.sub(r'about \$\d+\.\d+ each', '', description)
    description = re.sub(r'^\$\d+\.\d{2}$', '', description)
    return description.strip()


def reorder_item(data):
    order = [
        "zipcode", "store_name", "store_location", "store_logo", "store_brand",
        "category", "sub_category", "product_title", "weight",
        "regular_price", "sale_price", "volume_deals_description",
        "volume_deals_price", "digital_coupon_description",
        "digital_coupon_price", "unit_price", "image_url", "url",
        "upc", "crawl_date"
    ]
    return [{key: item.get(key, "") for key in order}  for item in data if item]

def process_others(data):
    return [item for item in data if item]

def filter_categories(data) -> list:
    categories = {
        
        "produce", "meat", "seafood", "deli", "bakery", "floral", 
        "grocery", "dairy", "frozen", "beverages", "breakfast", 
        "coffee", "candy", "beer-wine-liquor", "health", 
        "personal-care", "household", "kitchen-dining", 
        "pet-supplies", "natural-organic", "beauty", "baby"
    }
    return [item for item in data 
            if any(cat in categories for cat in [item['category'].lower(), item['sub_category'].lower()])]
    
def filter_final(data):
    filtered_data = filter_categories(data)
    
    with open(Path(f"marianos_filtered_{datetime.now().date()}.json"), 'w') as f:
        json.dump(filtered_data, f, indent=4)
    return filtered_data
    
async def main():
    output_file = await scrape(crawl=False)
    coupons_file = await get_coupons(crawl=False)
    
    df = pd.read_csv(output_file)
    df_coupons = pd.read_json(coupons_file)
    df['upc'] = df['upc'].astype(str).str.zfill(13)
    df_coupons['upc'] = df_coupons['upc'].astype(str).str.zfill(13)
    
    df_merged = pd.merge(df, df_coupons, on='upc', how='left')
    df_merged.fillna(value="", inplace=True)
    df_merged['volume_deals_description'] = df_merged['volume_deals_description'].apply(remove_invalid_promos)
    # df_merged['volume_deals_description'] = df_merged['volume_deals_description'].apply(reformat_data)
    df_merged.fillna(value="", inplace=True)
    data = df_merged.to_dict(orient='records')
    
    # data = [i for i in data if i.get("digital_coupon_description") == "Save $3.00 off 10 Yoplait Single Serve"]
    
    processed_data = PromoProcessor.process_item(data)
    # processed_data.apply(filter_final)
    processed_data.apply(reorder_item)
    processed_data.to_json(Path(f"marianos_{datetime.now().date()}.json"))
    

if __name__ == "__main__":
    asyncio.run(main())
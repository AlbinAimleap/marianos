import asyncio
import pandas as pd
from marianos import scrape
from get_coupon import process_coupons
import json
# from promo_validator import PromoProcessor
import re
import random
from datetime import datetime
from promo_processor import PromoProcessor
from utils import reformat_data


def apply_promos():
    
    with open('marianos_2024-11-04.json', 'r') as f:
        data = json.load(f)
        
        # promo_coupon = [i for i in data if i['digital_coupon_short_description'] and i["volume_deals_description"]][:5]
        # coupon = [i for i in data if i['digital_coupon_short_description']][:5]
        # promo = [i for i in data if i['volume_deals_description']][:5]
        # random.shuffle(data)
        # random_data = data[:5]
        
        # data = promo_coupon + coupon + promo + random_data
        
    
    data = reformat_data(data)
    processed_data = PromoProcessor.process_item(data)
    
    with open('marianos_2024-11-04.json', 'w') as f:
        data = [i for i in processed_data.results if i.get("volume_deals_description") or i.get("digital_coupon_short_description")]
        json.dump(data, f, indent=4)

def remove_invalid_promos(description):
    # remove patterns
    description = re.sub(r'\$\d+\.\d+/lb', '', description)
    description = re.sub(r'about \$\d+\.\d+ each', '', description)
    return description.strip()
    
    
    

async def main():
    output_file = await scrape()
    coupons = await process_coupons()
    
    # output_file = 'marianos_2024-11-04.csv'
    
    df = pd.read_csv(output_file)
    df_coupons = pd.DataFrame(coupons)
    df['upc'] = df['upc'].astype(str).str.zfill(13)
    df_coupons['upc'] = df_coupons['upc'].astype(str).str.zfill(13)
    
    df_merged = pd.merge(df, df_coupons, on='upc', how='left')
    df_merged.fillna(value="", inplace=True)
    df_merged['volume_deals_description'] = df_merged['volume_deals_description'].apply(remove_invalid_promos)
    df_merged.fillna(value="", inplace=True)
    data = df_merged.to_dict(orient='records')
    
    # with open("marianos_raw_2024-11-13.json", "w") as f:
    #     json.dump(data, f, indent=4)
    
    
    
    processed_data = PromoProcessor.process_item(data)
    
    with open(output_file.with_suffix(".json"), "w") as f:
        json.dump(processed_data.results, f, indent=4)
    

if __name__ == "__main__":
    asyncio.run(main())
    # apply_promos()
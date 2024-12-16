import pandas as pd
import json
import re
from datetime import datetime
from promo_processor import PromoProcessor
from pathlib import Path



def remove_invalid_promos(description):
    description = re.sub(r'\$\d+\.\d+/lb', '', description)
    description = re.sub(r'^about \$\d+\.\d+ each', '', description)
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

def split_promos(data):
    
    for item in data:
        if not item["digital_coupon_description"]:
            continue
        promos = item["digital_coupon_description"].split(";")
        amounts = []
        
        for promo in promos:
            match = re.search(r'\$(\d+\.?\d*)', promo.strip())
            if match:
                amounts.append(float(match.group(1)))
        
        if amounts:
            highest_promo = max(amounts)
            item["digital_coupon_description"] = f"${highest_promo:.2f} off"
        
    return data
    


def skip_invalids(data):
    return [i for i in data if not i["unit_price"] or (i["unit_price"] and i["unit_price"] > 0)]
    
def main():
    output_file = Path(r"C:\Users\Albia\Downloads\Marianos_Code\Marianos_Code\16-12-2024-grocessary-Target-v1 1.xlsx")
    df = pd.read_excel(output_file)
    df['upc'] = df['upc'].astype(str).str.zfill(13)
    
    df.fillna(value="", inplace=True)
    df['volume_deals_description'] = df['volume_deals_description'].apply(remove_invalid_promos)
    df.fillna(value="", inplace=True)
    data = df.to_dict(orient='records')
    processed_data = PromoProcessor.pre_process(split_promos)
    processed_data = processed_data.process_item(data)
    processed_data.apply(reorder_item)
    processed_data.apply(skip_invalids)
    processed_data.apply(split_promos)
    processed_data.to_json(Path(f"taret_{datetime.now().date()}.json"))
    

if __name__ == "__main__":
    main()
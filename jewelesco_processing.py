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
    new_data = []
    for item in data:
        if not item["digital_coupon_description"]:
            new_data.append(item.copy())
            continue
        promos = item["digital_coupon_description"].split(";")
        for promo in promos:
            item["digital_coupon_description"] = promo.strip()
            new_data.append(item.copy())
    return new_data

def get_lowest_unit_price(data):
    if not data:
        return data
        
    data = sorted(data, key=lambda x: x.get("upc"))
    i = 0
    
    while i < len(data) - 1:
        try:
            if data[i]["upc"] == data[i + 1]["upc"]:
                if not data[i]["unit_price"]:
                    data[i]["unit_price"] = 0
                if not data[i + 1]["unit_price"]:
                    data[i + 1]["unit_price"] = 0
                if data[i]["unit_price"] > data[i + 1]["unit_price"]:
                    data[i]["unit_price"] = data[i + 1]["unit_price"]
                data.pop(i + 1)
            else:
                i += 1
        except IndexError:
            break
    return data    
def skip_invalids(data):
    for item in data:
        sale_price = float(item.get("sale_price", 0) or 0)
        regular_price = float(item.get("regular_price", 0) or 0)
        
        if item.get("unit_price") and item["unit_price"] < 0:
            volume_deals_price = float(item.get("volume_deals_price", 0) or 0)
            digital_coupon_price = float(item.get("digital_coupon_price", 0) or 0)
            
            if volume_deals_price and (volume_deals_price > sale_price or volume_deals_price > regular_price or volume_deals_price == sale_price):
                item.update({"volume_deals_description": "", "volume_deals_price": "", "unit_price": ""})
            elif digital_coupon_price and (digital_coupon_price > sale_price or digital_coupon_price > regular_price or digital_coupon_price == sale_price):
                item.update({"digital_coupon_description": "", "digital_coupon_price": "", "unit_price": ""})
    return data
def format_zeros(data):
    keys = ["regular_price", "sale_price", "volume_deals_price", "digital_coupon_price", "unit_price"]
    for item in data:
        for key in keys:
            if item[key] == 0:
                item[key] = ""
    return data
  
def main():
    output_file = Path(r"C:\Users\Albia\Downloads\Marianos_Code\Marianos_Code\Jewelosco_pre_processed_20241224_v2.json")
    df = pd.read_json(output_file)
    df['upc'] = df['upc'].astype(str).str.zfill(13)
    df.fillna(value="", inplace=True)
    data = df.to_dict(orient='records')
    processed_data = PromoProcessor.pre_process(split_promos)
    processed_data = processed_data.process_item(data)
    processed_data.apply(reorder_item)
    processed_data.apply(skip_invalids)
    processed_data.apply(get_lowest_unit_price)
    processed_data.apply(format_zeros)
    processed_data.to_json(Path(f"jewelosco_{datetime.now().date()}.json"))
    

if __name__ == "__main__":
    main()
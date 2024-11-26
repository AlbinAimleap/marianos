import pandas as pd
import json
from typing import Dict, Any


def load_json_file(file_path):
    with open(file_path) as f:
        return json.load(f)

def apply_store_brands(item: Dict[str, Any]) -> Dict[str, Any]:
    store_brands = {
        'marianos': ["Private Selection", "Kroger", "Simple Truth", "Simple Truth Organic"],
        'target': ["Deal Worthy", "Good & Gather", "Market Pantry", "Favorite Day", "Kindfull", "Smartly", "Up & Up"],
        'jewel': ['Lucerne', "Signature Select", "O Organics", "Open Nature", "Waterfront Bistro", "Primo Taglio",
                    "Soleil", "Value Corner", "Ready Meals"],
        'walmart': ["Clear American", "Great Value", "Home Bake Value", "Marketside", 
                    "Co Squared", "Best Occasions", "Mash-Up Coffee", "World Table"]
    }
    store_brands_list = [brand for brands in store_brands.values() for brand in brands if brand.casefold() in item["product_title"].casefold()]    
    item["brandStatus"] = any(store_brands_list)
    return item



input_file = r"C:\Users\Albia\Downloads\Marianos_Code\Marianos_Code\20241113_Walmart_raw.json"
output_file = r"C:\Users\Albia\Downloads\Marianos_Code\Marianos_Code\20241113_Walmart.json"

data = load_json_file(input_file)

for item in data:
    item = apply_store_brands(item)
    item["volume_deals_description"] = ""
    item["volume_deals_price"] = ""
    item["digital_coupon_description"] = ""
    item["digital_coupon_price"] = ""


with open(output_file, 'w') as f:
    json.dump(data, f, indent=4)


from promo_processor.processor import PromoProcessor
from typing import Dict, Any
import re

class FixedPriceMealProcessor(PromoProcessor):
    patterns = [
        r'\$(?P<price>\d+\.?\d*)'
    ]
    
    def calculate_deal(self, item: Dict[str, Any], match: re.Match) -> Dict[str, Any]:
        item_data = item.copy()
        price = float(match.group('price'))
        
        item_data["volume_deals_price"] = round(price, 2)
        item_data["unit_price"] = round(price / 1, 2)
        item_data["digital_coupon_price"] = "" 
        
        return item_data
    
    def calculate_coupon(self, item: Dict[str, Any], match: re.Match) -> Dict[str, Any]:
        item_data = item.copy()
        price = float(match.group('price'))
        
        item_data["digital_coupon_price"] = round(price, 2)
        item_data["unit_price"] = round(price / 1, 2)
        
        return item_data
from promo_processor.processor import PromoProcessor

class SelectDealProcessor(PromoProcessor):
    patterns = [
        r"Deal:\s+\$(?P<price>\d+(?:\.\d{2})?)\s+price\s+on\s+" 
    ]
    
    
    def calculate_deal(self, item, match):
        """Process 'Deal: $X price on select' type promotions."""
        item_data = item.copy()
        select_price = float(match.group('price'))
        
        item_data["volume_deals_price"] = round(select_price, 2)
        item_data["unit_price"] = round(select_price, 2)
        item_data["digital_coupon_price"] = ""
        return item_data
        
    
    def calculate_coupon(self, item, match):
        """Calculate the price for 'Deal: $X price on select' promotions when a coupon is applied."""
        item_data = item.copy()
        select_price = float(match.group('price'))
        unit_price = item_data.get("unit_price", 0) - select_price
        
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = round(select_price, 2)
        return item_data
        
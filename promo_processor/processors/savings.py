from promo_processor.processor import PromoProcessor

class SavingsProcessor(PromoProcessor):
    patterns = [
        r'Save\s+\$(?P<savings>\d+(?:\.\d{2})?)', 
        r'SAVE\s+\$(?P<savings>\d+(?:\.\d{2})?)\s+on\s+(?P<quantity>\d+)\s+.*?(?:®|\\u00ae)?.*?', 
        r'Save\s+\$(?P<savings>0?\.\d{2})\s+on\s+.*?'
    ]    
    
       
    def calculate_deal(self, item, match):
        """Calculate the volume deals price for a deal."""
        item_data = item.copy()
        savings_value = float(match.group('savings'))
        price = item_data.get('price', 0)
        volume_deals_price = price - savings_value
        
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(volume_deals_price / 1, 2)
        item_data["digital_coupon_price"] = ""
        return item_data
        

    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount."""
        item_data = item.copy()
        savings_value = float(match.group('savings'))
        price = item_data.get('unit_price') or item_data.get('sale_price') or item_data.get('regular_price', 0)
        
        try:
            quantity = float(match.group('quantity'))
            volume_deals_price = ((price * quantity) - savings_value) / quantity
        except (IndexError, KeyError):
            volume_deals_price = price - savings_value
        
        item_data["unit_price"] = round(volume_deals_price / 1, 2)
        item_data["digital_coupon_price"] = round(savings_value, 2)
        return item_data
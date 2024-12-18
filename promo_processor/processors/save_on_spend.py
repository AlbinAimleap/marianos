from promo_processor.processor import PromoProcessor

class SpendSavingsProcessor(PromoProcessor):
    patterns = [
        r'Spend\s+\$(?P<spend>\d+(?:\.\d{2})?)\s+Save\s+\$(?P<savings>\d+(?:\.\d{2})?)\s+on\s+.*?'
    ]    

    
    def calculate_deal(self, item, match):
        """Calculate the volume deals price for a deal."""
        item_data = item.copy()
        savings_value = float(match.group('savings'))
        spend_requirement = float(match.group('spend'))
        price = item_data.get('sale_price') or item_data.get('regular_price', 0)
    
        discount_rate = savings_value / spend_requirement
        unit_price = price - (price * discount_rate)
    
        item_data["volume_deals_price"] = round(unit_price, 2)
        item_data["unit_price"] = round(unit_price / 1, 2)
        item_data["digital_coupon_price"] = 0
        return item_data
    

    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount."""
        item_data = item.copy()
        savings_value = float(match.group('savings'))
        spend_requirement = float(match.group('spend'))
        price = item_data.get('sale_price') or item_data.get('regular_price', 0)
    
        discount_rate = savings_value / spend_requirement
        unit_price = price - (price * discount_rate)
    
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = round((price * discount_rate), 2)
        return item_data
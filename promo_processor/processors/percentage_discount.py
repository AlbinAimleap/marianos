from promo_processor.processor import PromoProcessor

class PercentageDiscountProcessor(PromoProcessor):
    patterns = [
        r"^Deal:\s+(?P<discount>\d+)%\s+off", 
        r"^Save\s+(?P<discount>\d+)%\s+on\s+(?P<product>[\w\s-]+)",
        r"^(?P<discount>\d+)%\s+off\s+(?P<product>[\w\s-]+)",
    ]
    
    
    
    def calculate_deal(self, item, match):
        """Process 'X% off' type promotions."""
        item_data = item.copy()
        discount_percentage = float(match.group('discount'))
        discount_amount = item_data.get("sale_price") or item_data.get('regular_price', 0) * (discount_percentage / 100)
        volume_deals_price = item_data['regular_price'] - discount_amount
        
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(volume_deals_price / 1, 2)
        item_data["digital_coupon_price"] = ""
        return item_data
        
    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon for percentage-based discounts."""
        item_data = item.copy()
        discount_percentage = float(match.group('discount'))
        price = item_data.get('unit_price') or item_data.get("sale_price") or item_data.get("regular_price", 0)
        price = float(price) if price else 0
        discount_amount = price * (discount_percentage / 100)
        volume_deals_price = price - discount_amount
        
        item_data["unit_price"] = round(volume_deals_price / 1, 2)
        item_data["digital_coupon_price"] = round(volume_deals_price, 2)
        return item_data
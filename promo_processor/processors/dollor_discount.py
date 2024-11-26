from promo_processor.processor import PromoProcessor

class DollarDiscountProcessor(PromoProcessor):
    """Processor for handling '$X off' type promotions."""
    
    patterns = [r'\$(?P<discount>\d+(?:\.\d+)?)\s+off']
    
    
    
    def calculate_deal(self, item, match):
        """Process '$X off' type promotions for deals."""
        
        item_data = item.copy()
        discount_value = float(match.group('discount'))
        price = item_data.get('price', 0)  
        volume_deals_price = price - discount_value
        
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(volume_deals_price / 1, 2)
        item_data["digital_coupon_price"] = ""
        return item_data
        

    def calculate_coupon(self, item, match):
        """Process '$X off' type promotions for coupons."""
        item_data = item.copy()
        discount_value = float(match.group('discount'))
        price = item_data.get('unit_price') or item_data.get("sale_price") or item_data.get("regular_price", 0)
        volume_deals_price = price - discount_value
        
        item_data["unit_price"] = round(volume_deals_price / 1, 2)
        item_data["digital_coupon_price"] = round(discount_value, 2)
        return item_data
from promo_processor.processor import PromoProcessor

class SelectProductPriceProcessor(PromoProcessor):
    """Processor for handling '$X price on select Product' type promotions."""

    patterns = [r'\$(?P<price>\d+(?:\.\d{2})?)\s+price\s+on\s+select\s+(?P<product>[\w\s-]+)']
    
    
    def calculate_deal(self, item, match):
        """Process '$X price on select Product' type promotions for deals."""
        item_data = item.copy()
        select_price = float(match.group('price'))
        weight = item_data.get('weight', 1)
        
        item_data["volume_deals_price"] = round(select_price, 2)
        item_data["unit_price"] = round(select_price / 1, 2)
        item_data["digital_coupon_price"] = 0
        

    def calculate_coupon(self, item, match):
        """Process '$X price on select Product' type promotions for coupons."""
        item_data = item.copy()
        select_price = float(match.group('price'))
        weight = item_data.get('weight', 1)
        
        item_data["unit_price"] = round(select_price / 1, 2)
        item_data["digital_coupon_price"] = round(select_price, 2)
        return item_data
        
       
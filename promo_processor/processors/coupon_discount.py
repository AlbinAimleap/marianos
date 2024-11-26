from promo_processor.processor import PromoProcessor

class CouponDiscountProcessor(PromoProcessor):
    patterns = [
        r"(?:Coupon):\s+\$?(?P<discount>\d+(?:\.\d+)?)\s+(?:off|%)",
    ]
    
    
    def calculate_deal(self, item, match):
        """Process 'Coupon: $X off' type promotions."""        
        item_data = item.copy()
        discount = float(match.group('discount'))
        price = item_data.get("promo_price", item_data.get("regular_price", 0))
        volume_deals_price = price - discount
        
        item_data['volume_deals_price'] = round(volume_deals_price, 2)
        item_data['unit_price'] = round(volume_deals_price / 1, 2)
        item_data['digital_coupon_price'] = ""
        
        return item_data
    
    def calculate_coupon(self, item, match):
        """Process coupon discount calculation."""
        
        item_data = item.copy()
        discount = float(match.group('discount'))
        price = item_data.get("promo_price", item_data.get("regular_price", 0))
        volume_deals_price = price - discount
        
        item_data['digital_coupon_price'] = round(volume_deals_price, 2)
        item_data['unit_price'] = round(volume_deals_price / 1, 2)
        return item_data
        
        
from promo_processor.processor import PromoProcessor

class PriceEachWithQuantityProcessor(PromoProcessor):
    """Processor for handling '$X price each with Y' type promotions."""
    
    patterns = [r'\$(?P<price>\d+(?:\.\d{2})?)\s+price\s+each\s+(?:when\s+you\s+buy|with|for)\s+(?P<quantity>\d+)']
    
    
    
    def calculate_deal(self, item, match):
        """Process '$X price each with Y' type promotions for deals."""
        item_data = item.copy()
        price_each = float(match.group('price'))
        quantity = int(match.group('quantity'))
        total_price = price_each * quantity
        
        item_data["volume_deals_price"] = round(total_price, 2)
        item_data["unit_price"] = round(price_each, 2)
        item_data["digital_coupon_price"] = ""
        return item_data

    def calculate_coupon(self, item, match):
        """Process '$X price each with Y' type promotions for coupons."""
        item_data = item.copy()
        price_each = float(match.group('price'))
        quantity = int(match.group('quantity'))
        unit_price = round(item_data['unit_price'] - (price_each / quantity), 2)
        
        item_data["unit_price"] = round(unit_price)
        item_data["digital_coupon_price"] = round(price_each)
        return item_data
        
        
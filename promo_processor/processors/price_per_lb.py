from promo_processor.processor import PromoProcessor

class PricePerLbProcessor(PromoProcessor):
    """Processor for handling '$X/lb' type promotions."""

    patterns = [r'\$(?P<price_per_lb>\d+(?:\.\d{2})?)\/lb', r'\$(?P<price_per_lb>\d+\.\d{2})\/lb']    
    
    
    
    def calculate_deal(self, item, match):
        """Process '$X/lb' type promotions for deals."""
        item_data = item.copy()
        # price_per_lb = float(match.group('price_per_lb'))
        # weight = item_data.get('weight', 1)
        
        # weight = weight.split("[")[0] if "[" in weight else weight.split()[0] if len(weight.split()) > 1 else weight
        
        # if weight:
        #     total_price = float(price_per_lb) * float(weight)
        #     item_data["volume_deals_price"] = round(total_price, 2)
        #     item_data["unit_price"] = round(price_per_lb, 2)
        #     item_data["digital_coupon_price"] = ""
        return item_data


    def calculate_coupon(self, item, match):
        """Process '$X/lb' type promotions for coupons."""
        item_data = item.copy()
        # price_per_lb = float(match.group('price_per_lb'))
        # weight = item_data.get('weight', 1)
        
        # unit_price = round(item_data['unit_price'] - (price_per_lb * weight), 2)
        
        # item_data["unit_price"] = unit_price
        # item_data["digital_coupon_price"] = price_per_lb
        return item_data
        
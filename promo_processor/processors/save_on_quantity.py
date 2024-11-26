from promo_processor.processor import PromoProcessor


class SaveOnQuantityProcessor(PromoProcessor):
    patterns = [
        r"\$(?P<total_price>\d+(?:\.\d+)?)\s+SAVE\s+\$(?P<discount>\d+(?:\.\d+)?)\s+on\s+(?P<quantity>\w+)\s+\(\d+\)", 
        r"Save\s+\$(?P<discount>\d+(?:\.\d+)?)\s+on\s+(?P<quantity>\d+)\s+(?P<product>[\w\s-]+)" 
    ]
    
    

    def calculate_deal(self, item, match):
        """Process '$X SAVE $Y on Z' type promotions."""
        item_data = item.copy()
        try:
            total_price = float(match.group('total_price'))
        except IndexError:
            total_price = item_data.get("sale_price", item_data.get("regular_price", 0))
            
        discount = float(match.group('discount'))
        quantity = float(match.group('quantity'))        
        
        volume_deals_price = total_price - discount
        
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(volume_deals_price / quantity, 2)
        item_data["digital_coupon_price"] = ""
        return item_data

    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount for Save $X on Y promotions."""
        item_data = item.copy()
        price = item_data.get("unit_price", 0)
        quantity = float(match.group('quantity'))
        discount = float(match.group('discount'))
        
        unit_price = ((price * quantity) - discount) / quantity
        
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = round(discount, 2)
        return item_data
        

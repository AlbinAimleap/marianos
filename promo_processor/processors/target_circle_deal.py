from promo_processor.processor import PromoProcessor

class TargetCircleDealProcessor(PromoProcessor):
    patterns = [
        r'Target Circle Deal\s*:\s*Buy\s+(?P<buy_qty>\d+),\s*get\s+(?P<get_qty>\d+)\s+(?P<discount>\d+)%\s+off\s+select\s+(?P<product>[\w\s]+)',
    ]
    
    
    def calculate_deal(self, item, match) -> dict:
        """No volume deals calculation needed for this case"""
        item_data = item.copy()
        return item_data

    def calculate_coupon(self, item, match) -> dict:
        """Calculate coupon price for buy X get Y Z% off deals"""
        item_data = item.copy()
        buy_qty = int(match.group('buy_qty'))
        get_qty = int(match.group('get_qty'))
        discount_percent = int(match.group('discount'))
    
        base_price = item_data.get('sale_price') or item_data.get("regular_price", 0)
        
        base_price = float(base_price)
    
        regular_total = base_price * (buy_qty + get_qty)
        discount_amount = (base_price * get_qty) * (discount_percent / 100)
        final_price = regular_total - discount_amount
    
        unit_price = final_price / (buy_qty + get_qty)
    
        item_data["digital_coupon_price"] = round(final_price, 2)
        item_data["unit_price"] = round(unit_price, 2)
        return item_data

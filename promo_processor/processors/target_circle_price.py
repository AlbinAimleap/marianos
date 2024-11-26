from promo_processor.processor import PromoProcessor

class TargetCirclePriceProcessor(PromoProcessor):
    patterns = [
        r'Target Circle Deal\s*:\s*\$(?P<price>\d+\.?\d*)\s+price\s+on\s+(?P<product>[\w\s-]+)',
        r'Target Circle Coupon\s*:\s*\$(?P<amount>\d+\.?\d*)\s+off',
    ]
    
    def calculate_deal(self, item, match) -> dict:
        """No volume deals calculation needed for this case"""
        item_data = item.copy()
        return item_data

    def calculate_coupon(self, item, match) -> dict:
        """Calculate coupon price for fixed price deals and amount off deals"""
        item_data = item.copy()
        if 'price' in match.groupdict():
            price = float(match.group('price'))
            item_data["digital_coupon_price"] = price
            item_data["unit_price"] = price
            
        elif 'amount' in match.groupdict():
            amount_off = float(match.group('amount'))
            original_price = item_data.get("regular_price")
            new_price = max(0, original_price - amount_off)
            item_data["digital_coupon_price"] = round(amount_off)
            item_data["unit_price"] = round(new_price)
            
        return item_data
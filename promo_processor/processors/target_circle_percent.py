from promo_processor.processor import PromoProcessor

class TargetCirclePercentProcessor(PromoProcessor):
    patterns = [
        r'Target Circle Deal\s*:\s*(?P<discount>\d+)%\s+off\s+(?P<product>[\w\s&,-]+)',
        r'Target Circle\s*:\s*(?P<discount>\d+)%\s+off\s+(?P<product>[\w\s&,-]+)',
        r'Target Circle Deal\s*:\s*Save\s+(?P<discount>\d+)%\s+on\s+(?P<product>[\w\s&,\'-]+)(?:\s*-\s*(?P<quantity>\d+)(?:pk|pks|pack|packs))?'
    ]

    def calculate_deal(self, item, match) -> dict:
        """No volume deals calculation needed for this case"""
        item_data = item.copy()
        return item_data

    def calculate_coupon(self, item, match) -> dict:
        """Calculate coupon price for percent off deals"""
        item_data = item.copy()
        discount_percent = int(match.group('discount'))
    
        base_price = item_data.get('sale_price') or item_data.get("regular_price", 0)
        base_price = float(base_price)
    
        discount_amount = base_price * (discount_percent / 100)
        final_price = base_price - discount_amount
    
        item_data["digital_coupon_price"] = round(discount_amount, 2)

        item_data["unit_price"] = round(final_price, 2)
        return item_data

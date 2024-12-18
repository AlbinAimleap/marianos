from promo_processor.processor import PromoProcessor

class AboutEachPriceProcessor(PromoProcessor):
    patterns = [r"\$(?P<unit_price>\d+(?:\.\d+)?)\s+Each"]
    
    
    # Example: "$5.99 Each" or "$2.50 Each"

    def calculate_deal(self, item, match):
        item_data = item.copy()
        unit_price = float(match.group('unit_price'))
        quantity = item_data.get("quantity", 1)
        volume_deals_price = unit_price * quantity
        unit_price_calculated = volume_deals_price / quantity
        
        item_data['volume_deals_price'] = round(volume_deals_price, 2)
        item_data['unit_price'] = round(unit_price_calculated, 2)
        item_data['digital_coupon_price'] = 0
        
        return item_data

    def calculate_coupon(self, item, match):
        item_data = item.copy()
        unit_price = float(match.group('unit_price'))
        quantity = item_data.get("quantity", 1)
        volume_deals_price = unit_price * quantity
        unit_price_calculated = volume_deals_price / quantity
        
        item_data['digital_coupon_price'] = round(unit_price_calculated, 2)
        item_data["unit_price"] = round(unit_price_calculated, 2)
        
        return item_data
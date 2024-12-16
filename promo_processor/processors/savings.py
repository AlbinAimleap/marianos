from promo_processor.processor import PromoProcessor

class SavingsProcessor(PromoProcessor):
    patterns = [
    r'^Save\s+\$(?P<savings>\d+\.\d{2})\s+off\s+(?P<quantity>\d+)\s+',  # Matches "Save $3.00 off 10 ..."
    r'^Save\s+\$(?P<savings>\d+\.\d{2})\s+(?!on\s+(?P<quantity>\d+)\s+)',  # Matches "Save $3.00" but excludes "on X ..."
    r'^Save\s+\$(?P<savings>\d+(?:\.\d{2})?)',  # Matches "Save $3.00" or "Save $3"
    r'^Save\s+\$(?P<savings>0?\.\d{2})\s+on\s+(?P<quantity>\d+)\s+',  # Matches "Save $0.05 on 10 ..."
]

    
       
    def calculate_deal(self, item, match):
        """Calculate the volume deals price for a deal."""
        item_data = item.copy()
        savings_value = float(match.group('savings'))
        price = item_data.get('sale_price') or item_data.get('regular_price')
        
        quantity = 1
        if 'quantity' in match.groupdict() and match.group('quantity'):
            quantity = float(match.group('quantity'))
            price_for_quantity = price * quantity
            savings_value_for_quantity = price_for_quantity - savings_value
            volume_deals_price = price 
            unit_price = savings_value_for_quantity / quantity
        else:
            volume_deals_price = price - savings_value
            unit_price = volume_deals_price
            
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = ""
        return item_data
        

    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount."""
        item_data = item.copy()
        savings_value = float(match.group('savings'))
        price = item_data.get('sale_price') or item_data.get('regular_price')
        
        
        quantity = 1
        if 'quantity' in match.groupdict() and match.group('quantity'):
            quantity = float(match.group('quantity'))
            price_for_quantity = price * quantity
            savings_value_for_quantity = price_for_quantity - savings_value
            volume_deals_price = price 
            unit_price = savings_value_for_quantity / quantity
        else:
            volume_deals_price = price - savings_value
            unit_price = volume_deals_price
        
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = round(savings_value, 2)
        return item_data
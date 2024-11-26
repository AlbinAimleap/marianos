from promo_processor.processor import PromoProcessor

class BuyGetFreeProcessor(PromoProcessor):
    patterns = [
        r"Buy\s+(?P<quantity>\d+),?\s+Get\s+(?P<free>\d+)\s+Free",
        r"Buy\s+(?P<quantity>\d+),\s+get\s+(?P<free>\d+)\s+(?P<discount>\d+)%\s+off"
    ]
    
    

    def calculate_deal(self, item, match):
        """Process 'Buy X Get Y Free' and 'Buy X Get Y % off' specific promotions."""
        
        item_data = item.copy()
        
        quantity = int(match.group('quantity'))
        free = int(match.group('free'))
        discount = match.groupdict().get('discount')
        
        if discount:
            discount_decimal = int(discount) / 100
            full_price_items = quantity
            discounted_items = free
            
            full_price_total = item_data['regular_price'] * full_price_items
            discounted_total = (item_data['regular_price'] * (1 - discount_decimal)) * discounted_items
            volume_deals_price = full_price_total + discounted_total
            total_quantity = quantity + free
        else:
            volume_deals_price = item_data['regular_price'] * quantity
            total_quantity = quantity + free
        
        unit_price = volume_deals_price / total_quantity
        
        item_data['volume_deals_price'] = round(volume_deals_price, 2)
        item_data['unit_price'] = round(unit_price, 2)
        item_data['digital_coupon_price'] = ""
        
        return item_data

    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount for 'Buy X Get Y Free' promotions."""
        # Target Circle Deal : Buy 1, get 1 25%
        item_data = item.copy()
        
        quantity = int(match.group('quantity'))
        free = int(match.group('free'))
        discount = match.groupdict().get('discount')
        price = item_data.get('unit_price') or item_data.get("sale_price") or item_data.get("regular_price", 0)
        
        total_quantity = quantity + free
        
        if discount:
            total_price = price * total_quantity
            discount_decimal = int(discount) / 100
            discount_amount = total_price * discount_decimal
            volume_deals_price = total_price - discount_amount
            unit_price = volume_deals_price / total_quantity
            
        else:
            price = float(price) if price else 0
            volume_deals_price = price * quantity
            total_quantity = quantity + free
        
            unit_price = volume_deals_price / total_quantity
        
        item_data['unit_price'] = round(unit_price, 2)
        item_data['digital_coupon_price'] = round(volume_deals_price, 2)
        
        return item_data
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
        price = item_data.get('sale_price') or item_data.get('regular_price', 0)
        total_quantity = quantity + free
        
        if discount:
            discount_decimal = int(discount) / 100
            price_for_full_price_items = price * quantity
            price_for_discounted_items = (price * free) * (1 - discount_decimal)
            volume_deals_price = price_for_full_price_items + price_for_discounted_items
            unit_price = volume_deals_price / total_quantity
        else:
            price = item_data.get('regular_price', 0)
            volume_deals_price = price * quantity
            unit_price = volume_deals_price / total_quantity if total_quantity > 0 else 1
        
        item_data['volume_deals_price'] = round(total_quantity * unit_price, 2)
        item_data['unit_price'] = round(unit_price, 2)
        item_data['digital_coupon_price'] = ""
        
        return item_data
    
    
    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount for 'Buy X Get Y Free' promotions."""
        item_data = item.copy()
        
        quantity = int(match.group('quantity'))
        free = int(match.group('free'))
        discount = match.groupdict().get('discount')
        price = item_data.get('unit_price') or item_data.get("sale_price") or item_data.get("regular_price", 0)
        total_quantity = quantity + free
        
        if discount:
            price_for_full_price_items = price * quantity
            price_for_discounted_items = (price * free) * (1 - int(discount) / 100)
            volume_deals_price = price_for_full_price_items + price_for_discounted_items
            unit_price = volume_deals_price / total_quantity
        else:
            price = float(price) if price else 0
            volume_deals_price = price * quantity
            unit_price = volume_deals_price / total_quantity
        
        item_data['unit_price'] = round(unit_price, 2)
        item_data['digital_coupon_price'] = round(total_quantity * unit_price, 2)
        
        return item_data
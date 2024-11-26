from promo_processor.processor import PromoProcessor

class WordBasedQuantityPriceProcessor(PromoProcessor):
    patterns = [
        r"\$(?P<volume_deals_price>\d+(?:\.\d+)?)\s+When\s+you\s+buy\s+(?P<quantity>\w+)",
        r"\$(?P<volume_deals_price>\d+(?:\.\d+)?)\s+When\s+you\s+buy\s+[any]?\s?+(?P<quantity>\w+)\s+\(\d+\)"
    ]
    def calculate_deal(self, item, match):
        """Calculate promotion price for '$X When you buy ONE' type promotions."""
        item_data = item.copy()
        volume_deals_price = float(match.group('volume_deals_price'))
        quantity_word = match.group('quantity')
        quantity = self.NUMBER_MAPPING.get(quantity_word.upper(), 1)
        unit_price = volume_deals_price / quantity
        
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = ""
        return item_data



    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount."""
        item_data = item.copy()
        price = item_data.get("sale_price", item_data.get("regular_price"))
        volume_deals_price = float(match.group('volume_deals_price'))
        quantity_word = match.group('quantity')
        quantity = self.number_mapping.get(quantity_word.upper(), 1)
        unit_price = price / quantity
        
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = round(volume_deals_price, 2)
        return item_data
        
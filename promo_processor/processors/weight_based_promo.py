from promo_processor.processor import PromoProcessor

class WeightBasedPromoProcessor(PromoProcessor):
    patterns = [
        r"\$(?P<volume_deals_price>\d+(?:\.\d+)?)\/lb\s+When\s+you\s+buy\s+(?P<quantity>\w+)\s+\(\d+\)" 
    ]
   
    def calculate_deal(self, item, match):
        """Process '$X/lb When you buy Y (Z)' type promotions."""
        item_data = item.copy()
        volume_deals_price = float(match.group('volume_deals_price'))
        quantity_word = match.group('quantity')
        quantity = self._convert_word_to_number(quantity_word)
        unit_price = volume_deals_price / quantity
        
        item_data["volume_deals_price"] = round(volume_deals_price, 2)
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = 0
        return item_data
        

    def calculate_coupon(self, item, match):
        """Calculate the price after applying a coupon discount for weight-based promotions."""
        item_data = item.copy()
        volume_deals_price = float(match.group('volume_deals_price'))
        quantity_word = match.group('quantity')
        quantity = self._convert_word_to_number(quantity_word)
        unit_price = volume_deals_price / quantity
        
        item_data["unit_price"] = round(unit_price, 2)
        item_data["digital_coupon_price"] = round(volume_deals_price, 2)
        return item_data

    def _convert_word_to_number(self, word: str) -> int:
        """Convert word-based quantity (e.g., 'ONE') to its numeric value using number mapping."""
        return self.NUMBER_MAPPING.get(word.upper(), 1)
from promo_processor.processor import PromoProcessor

class PricePerLbProcessor(PromoProcessor):
    """Processor for handling '$X/lb' and related promotions."""

    patterns = [
        r'\$(?P<price_per_lb>\d+(?:\.\d{2})?)\/lb',
        r'\$(?P<price_per_lb>\d+\.\d{2})\/lb',
        r'\$(?P<price_per_lb>\d+\.\d{2})\s+Lb\s+Save\s+Up\s+To:\s+\$(?P<savings>\d+\.\d{2})\s+Lb',
        r'\$(?P<price_per_lb>\d+\.\d{2})\s+Lb\s+Save\s+Up\s+To:\s+\$(?P<savings>\d+(?:\.\d{1,2})?)\s+Lb',
        r'\$(?P<price_per_lb>\d+\.\d{2})\s+Lb\s+Save\s+Up\s+To:\s+\$(?P<savings>\d+(?:\.\d{1,2})?)'
    ]

    def calculate_deal(self, item, match):
        """Process '$X/lb' type promotions for deals."""
        item_data = item.copy()
        price_per_lb = float(match.group('price_per_lb'))
        if isinstance(item_data.get('weight', 0), str):
            weight = 1
        else:
            weight = float(item_data.get('weight', 0) or 1)

        # Extract weight if it contains additional information

        if weight:
            weight = float(weight)
            total_price = price_per_lb * weight
            item_data["volume_deals_price"] = round(total_price, 2)
            item_data["unit_price"] = round(price_per_lb, 2)
            item_data["digital_coupon_price"] = 0

            # Adjust for savings if available
            if 'savings' in match.groupdict():
                savings_per_lb = float(match.group('savings'))
                item_data["volume_deals_price"] = round(savings_per_lb * weight, 2)

        return item_data

    def calculate_coupon(self, item, match):
        """Process '$X/lb' type promotions for coupons."""
        item_data = item.copy()
        price_per_lb = float(match.group('price_per_lb'))
        if isinstance(item_data.get('weight', 0), str):
            weight = 1
        else:
            weight = float(item_data.get('weight', 0) or 1)

        # Extract weight if it contains additional informatio

        if weight:
            weight = float(weight)
            coupon_savings = price_per_lb * weight
            item_data["digital_coupon_price"] = round(coupon_savings, 2)

            # Adjust unit price if savings are mentioned
            if 'savings' in match.groupdict():
                savings_per_lb = float(match.group('savings'))
                coupon_savings = savings_per_lb * weight
                item_data["unit_price"] = round(item_data['unit_price'] - coupon_savings, 2)
            else:
                item_data["unit_price"] = round(item_data['unit_price'] - price_per_lb, 2)

        return item_data
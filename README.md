  ## Steps for Price Calculations in PromoProcessor

  ### Flow Diagram
  ![Price Calculation Flow](image.png)

  ### 1. Initial Processing
  - Get base price from sale_price or regular_price
  - Get weight from item if available, default to 1
  - Process both volume deals and digital coupons separately

  ### 2. Volume Deals Price Calculation
  - Match promo description against defined patterns
  - For quantity-based deals (e.g. "3 For $9.99"):
    * volume_deals_price = advertised total price
    * unit_price = volume_deals_price / quantity
  - For percentage discounts (e.g. "20% off"):
    * volume_deals_price = base_price - (base_price * discount_percentage)
    * unit_price = volume_deals_price
  - For dollar amount off (e.g. "$2 off"):
    * volume_deals_price = base_price - discount_amount
    * unit_price = volume_deals_price

  ### 3. Digital Coupon Price Calculation
  - Match coupon description against patterns
  - For fixed amount off:
    * digital_coupon_price = discount_amount
    * unit_price = (base_price - discount_amount)
  - For percentage off:
    * digital_coupon_price = base_price * (discount_percentage/100)
    * unit_price = base_price - digital_coupon_price
  - For quantity-based deals:
    * digital_coupon_price = discounted_total_price
    * unit_price = digital_coupon_price / quantity

  ### 4. Price Validation Rules
  - If unit_price equals base_price, clear unit_price
  - If volume_deals_price equals base_price, clear volume_deals_price
  - If digital_coupon_price equals base_price, clear digital_coupon_price
  - Round all calculated prices to 2 decimal places

  ### 5. Order of Processing
  - Initial processing is performed first
  - Volume deals are calculated second
  - Digital coupons are applied third
  - Price validation rules are applied last
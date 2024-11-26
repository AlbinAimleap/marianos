import re
from typing import Dict, List, Tuple, Union
from pathlib import Path
import json
import argparse
import logging
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

base_dir = Path(__file__).parent.parent

class PromoProcessor:
    number_mapping = {
        "ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5, "SIX": 6, "SEVEN": 7, "EIGHT": 8, "NINE": 9,"TEN": 10
    }

    def __init__(self):
        self.results = []
        self.base_price = 0
        self.patterns: List[Tuple[str, callable]] = [
            # Match patterns like "3 For $9.99" or "Buy 2 For $5.99"
            (r'(?P<quantity>\d+)\s+For\s+\$(?P<volume_deals_price>\d+(?:\.\d+)?)', self._process_quantity_for_price),
    
            # Match patterns like "$2.99 When you buy ONE" (word number format)
            (r'\$(?P<volume_deals_price>\d+(?:\.\d+)?)\s+When\s+you\s+buy\s+(?P<quantity>\w+)', self._process_word_based_quantity_price),
    
            # Match patterns like "$2.99 When you buy any ONE (1)"
            (r'\$(?P<volume_deals_price>\d+(?:\.\d+)?)\s+When\s+you\s+buy\s+[any]?\s?+(?P<quantity>\w+)\s+\(\d+\)', self._process_word_based_quantity_price),
    
            # Match patterns like "Add 3 Total For Offer"
            (r'Add\s+(?P<quantity>\d+)\s+Total\s+For\s+Offer', self._process_add_total_for_offer),

            # Match patterns like "About $3.99 Each"
            (r'\$(?P<unit_price>\d+(?:\.\d+)?)\s+Each', self._process_about_each_price),

            # Match patterns like "Buy 2, Get 1 Free"
            (r'Buy\s+(?P<quantity>\d+),?\s+Get\s+(?P<free>\d+)\s+Free', self._process_buy_get_free_specific),

            # Match patterns like "$16.99 SAVE $5.00 on TWO (2)"
            (r'\$(?P<total_price>\d+(?:\.\d+)?)\s+SAVE\s+\$(?P<discount>\d+(?:\.\d+)?)\s+on\s+(?P<quantity>\w+)\s+\(\d+\)', self._process_save_on_quantity),
    
            # Match patterns like "$9.99/lb When you buy One (1)"
            (r'\$(?P<volume_deals_price>\d+(?:\.\d+)?)\/lb\s+When\s+you\s+buy\s+(?P<quantity>\w+)\s+\(\d+\)', self._process_weight_based_price),
    
            # Match patterns like "Buy 4 get 10% off"
            (r'Buy\s+(?P<quantity>\d+)\s+get\s+(?P<discount>\d+)%\s+off', self._process_buy_get_discount),
    
            # Match patterns like "Coupon: $0.50 off"
            (r'(?:Coupon):\s+\$?(?P<discount>\d+(?:\.\d+)?)\s+(?:off|%)', self._process_coupon_discount),
    
            #Match patterns like "Buy 1 get 25% Off"
            (r'Buy\s+(\d+),\s+get\s+(\d+)\s+(?P<discount>\d+)%\s+off', self._process_buy_one_get_one),
    
            # Match patterns like "$5.99 price on selected products"
            (r'Deal:\s+\$(?P<price>\d+(?:\.\d{2})?)\s+price\s+on\s+', self._process_select_deal),
    
            # Match patterns like "15% off"
            (r'Deal:\s+(?P<discount>\d+)%\s+off', self._process_percentage_discount),
    
            # Match patterns like "2$ off"
            (r'\$(?P<discount>\d+(?:\.\d+)?)\s+off', self._process_dollar_discount),
    
            # Match patterns like "$12/lb"
            (r'\$(?P<price_per_lb>\d+(?:\.\d{2})?)\/lb', self._process_price_per_lb),
    
            # Match patterns like "$12.99 price each when you buy 2" or "$10.99 price each with 2 Keurig K-Cup pods" or "$14.99 price each for 2 Peet's coffee K-Cup pods - 22ct"
            (r'\$(?P<price>\d+(?:\.\d{2})?)\s+price\s+each\s+(?:when\s+you\s+buy|with|for)\s+(?P<quantity>\d+)', self._process_price_each_with_quantity),
    
            # Match patterns like "$1.69 price on select Noosa yoghurt - 8oz"
            (r'\$(?P<price>\d+(?:\.\d{2})?)\s+price\s+on\s+select\s+(?P<product>[\w\s-]+)', self._process_select_product_price),
    
            # Match patterns like "Save 20% on Trick-or-Treat candy"
            (r'Save\s+(?P<discount>\d+)%\s+on\s+(?P<product>[\w\s-]+)', self._process_percentage_discount),
    
            # Match patterns like "10% off Oreo halloween trick or treat bag"
            (r'(?P<discount>\d+)%\s+off\s+(?P<product>[\w\s-]+)', self._process_percentage_discount),
    
            # Match patterns like "Save $2.00 on 2 Silk Almond or Oat creamer"
            (r'Save\s+\$(?P<discount>\d+(?:\.\d+)?)\s+on\s+(?P<quantity>\d+)\s+(?P<product>[\w\s-]+)', self._process_save_on_quantity),
            
            # Match patterns like "save 8$"
            (r'Save\s+\$(?P<savings>\d+(?:\.\d{2})?)', self._process_savings),
        ]
        
        
    def process(self, items: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """Process single dict item or list of dict items and calculate the promo price and unit price based on promo description."""
        if isinstance(items, dict):
            self.results.append(self._process_item(items))
            return self.results
        elif isinstance(items, list):
            self.results.extend([self._process_item(item) for item in items])
            return self.results
        else:
            raise ValueError("Input must be a dictionary or a list of dictionaries")
    
    def to_csv(self, file_path: str):
        """Save the results to a CSV file."""
        results = [item for item in self.results if item is not None]
        results = [dict(t) for t in {tuple(d.items()) for d in results}]
        df = pd.DataFrame(results)
        df.to_csv(file_path, index=False)
    
    def to_json(self, file_path: str):
        """Save the results to a JSON file."""
        results = [item for item in self.results if item is not None]
        results = [dict(t) for t in {tuple(d.items()) for d in results}]
        with open(file_path, 'w') as f:
            json.dump(results, f, indent=4)

    def _process_item(self, item: Dict) -> Dict:
        """Process each item and calculate the promo price and unit price based on promo description."""
        updated_item = item.copy()
        price = self._get_price(item)
        weight = self._get_weight(item)
        
        deals, coupon = None, None
        
        disqualified_patterns = [
            r'\$(?P<unit_price>\d+(?:\.\d+)?)\s+Each',
            r'\$(?P<price_per_lb>\d+(?:\.\d{2})?)\/lb'
        ]
        
        if item.get("volume_deals_description"):
            deals, pattern = self._process_volume_deals(item.get("volume_deals_description", ""), price, weight)
            
            if pattern in disqualified_patterns:
                return
        
        if (updated_item.get("unit_price") == price) or (updated_item.get("volume_deals_price") == price):
            updated_item["volume_deals_price"] = ""
            updated_item["unit_price"] = ""
        
        if item.get("digital_coupon_short_description"):
            coupon, pattern = self._process_digital_coupon(item.get("digital_coupon_short_description", ""), deals.get("unit_price", 0), weight)
            
            if pattern in disqualified_patterns:
                return
        
        if (updated_item.get("unit_price") == price) or (updated_item.get("digital_coupon_price") == price):
            updated_item["digital_coupon_price"] = ""
            updated_item["unit_price"] = ""
        
        if deals:
            updated_item.update(deals)
        if coupon:
            updated_item.update(coupon)
        
        updated_item["digital_coupon_description"] = updated_item["digital_coupon_short_description"]
        del updated_item["digital_coupon_short_description"]
            
        return self.re_order(updated_item)
    
    def re_order(self, item):
        column_order = [
            "zipcode", "store_name", "store_location", "store_logo", "category",
            "sub_category", "product_title", "weight", "regular_price", "sale_price",
            "volume_deals_description", "volume_deals_price", "digital_coupon_description",
            "digital_coupon_price", "unit_price", "image_url", "url", "upc", "crawl_date"
        ]
        
        ordered_item = {column: item.get(column, "") for column in column_order}
        return ordered_item
    
    def _get_price(self, item: Dict) -> float:
        price = item.get("sale_price", "") or item.get("regular_price", "")
        price = price.replace("$", "").replace(",", "") if isinstance(price, str) else price
        self.base_price = price
        return float(price)
        
    def _get_weight(self, item: Dict) -> float:
        try:
            return float(item.get("weight", "0").split()[0])
        except:
            return 1
            
    def _process_volume_deals(self, description: str, price: float, weight: float) -> None:
        for pattern, processor in self.patterns:
            if match := re.search(pattern, description, re.IGNORECASE):
                try:
                    return processor(match, price, weight, mode="volume_deals"), pattern
                except Exception as e:
                    continue
                    logger.error(str(e))
                break
        return None, None
                
    def _process_digital_coupon(self, description: str, price: float, weight: float) -> None:
        for pattern, processor in self.patterns:
            if match := re.search(pattern, description, re.IGNORECASE):
                try:
                    return processor(match, price, weight, mode="digital_coupon"), pattern
                except Exception as e:
                    logger.error(str(e))
                    continue
                break    
        return None, None
    
    def _process_select_product_price(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process '$X price on select Product' type promotions."""
        select_price = float(match.group('price'))
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(select_price, 2),
                "unit_price": round(select_price / weight if weight else select_price, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            return {
                "unit_price": round(select_price / weight if weight else select_price, 2),
                "digital_coupon_price": round(select_price, 2),
            }
    
    def _process_price_each_with_quantity(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process '$X price each with Y' type promotions."""
        price_each = float(match.group('price'))
        quantity = int(match.group('quantity'))
        total_price = price_each * quantity
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(total_price, 2),
                "unit_price": round(price_each, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price = round(price - (price_each / quantity), 2)
            return {
                "unit_price": unit_price,
                "digital_coupon_price": price_each
            }
    
    
    def _process_price_per_lb(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process '$X/lb' type promotions."""
        price_per_lb = float(match.group('price_per_lb'))
        total_price = price_per_lb * weight

        if mode == "volume_deals":
            return {
                "volume_deals_price": round(total_price, 2),
                "unit_price": round(price_per_lb, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price = round(price - (price_per_lb * weight), 2)
            return {
                "unit_price": unit_price,
                "digital_coupon_price": price_per_lb
            }
    
    def _process_savings(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Save $X' type promotions."""
        savings_value = float(match.group('savings'))
        volume_deals_price = price - savings_value
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2),
                "unit_price": round(volume_deals_price / 1, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            return {
                "unit_price": round(volume_deals_price / 1, 2),
                "digital_coupon_price": round(savings_value, 2)
            }
    
    def _process_dollar_discount(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Deal: $X off' type promotions."""
        discount_value = float(match.group('discount'))
        volume_deals_price = price - discount_value
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2),
                "unit_price": round(volume_deals_price / 1, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price = volume_deals_price
            return {
                "unit_price": round(discount_value, 2),
                "digital_coupon_price": round(discount_value, 2)
            }

    def _process_percentage_discount(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Deal: X% off' type promotions."""
        discount_percentage = float(match.group('discount'))
        discount_amount = price * (discount_percentage / 100)
        volume_deals_price = price - discount_amount
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2),
                "unit_price": round(volume_deals_price / 1, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            return {
                "unit_price": round(discount_percentage, 2)
            }

    
    def _process_select_deal(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Deal: $X price on select' type promotions."""
        select_price = float(match.group('price'))
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(select_price, 2),
                "unit_price": round(select_price, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price -= select_price
            return {
                "unit_price": round(select_price, 2)
            }
      
    
    def _process_buy_one_get_one(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Buy X, get Y% off' type promotions."""
        #NOTE: Buy 2 get 10% , price 10, 2=20, final = 18, unit_price= 9
        buy_quantity = int(match.group(1))
        get_quantity = int(match.group(2)) or 1
        discount_percentage = float(match.group('discount'))

        total_price = buy_quantity * price
        discount_amount = total_price * (discount_percentage / 100)
        volume_deals_price = total_price - discount_amount
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2),
                "unit_price": round(volume_deals_price / (buy_quantity + get_quantity), 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price = price - ((((discount_percentage / 100)*buy_quantity) * price) / buy_quantity)
            return {
                "unit_price": round(unit_price, 2),
                "digital_coupon_price": round(volume_deals_price, 2)
            }

    
    def _process_coupon_discount(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Coupon: $X off' type promotions."""
        discount = float(match.group('discount'))
        volume_deals_price = price - discount
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2),
                "unit_price": round(volume_deals_price / 1, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            return {
                "unit_price": round(volume_deals_price / 1, 2),
                "digital_coupon_price": volume_deals_price
            }

    def _process_price_and_quantity(self, volume_deals_price: float, quantity: int, weight: float = None, mode="volume_deals") -> Dict:
        """Calculate unit price and return processed data."""
        unit_price = volume_deals_price / quantity
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2), 
                "unit_price": round(unit_price, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            return {"unit_price": round(unit_price, 2), "digital_coupon_price": volume_deals_price}
   

    def _convert_word_to_number(self, word: str) -> int:
        """Convert word-based number (e.g., 'ONE') to its numeric value using number mapping."""
        return self.number_mapping.get(word.upper(), 1)

    def _process_quantity_for_price(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'X For $Y' type promotions."""
        return self._process_price_and_quantity(float(match.group('volume_deals_price')), int(match.group('quantity')))

    def _process_word_based_quantity_price(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process '$X When you buy ONE' type promotions with word-based quantity."""
        volume_deals_price = float(match.group('volume_deals_price'))
        quantity_word = match.group('quantity') 
        quantity = self._convert_word_to_number(quantity_word)
        unit_price = volume_deals_price / quantity
        
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2), 
                "unit_price": round(unit_price, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price = price / quantity
            return {
                "unit_price": round(unit_price, 2),
                "digital_coupon_price": volume_deals_price
                }

    def _process_buy_get_free(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Buy X Get Y Free' type promotions."""
        quantity = int(match.group('quantity'))
        volume_deals_price = price * quantity
        if mode == "volume_deals":
            return {
                "volume_deals_price": round(volume_deals_price, 2), 
                "unit_price": round(unit_price, 2),
                "digital_coupon_price": ""
            }
        elif mode == "digital_coupon":
            unit_price = price - (price / quantity)
            return {
                "unit_price": round(unit_price, 2),
                "digital_coupon_price": volume_deals_price
                }


    def _process_add_total_for_offer(self, match: re.Match, unit_price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Add X Total For Offer' type promotions."""
        quantity = int(match.group('quantity'))
        return self._process_price_and_quantity(unit_price * quantity, quantity)

    def _process_about_each_price(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'About $X Each' type promotions."""
        return self._process_price_and_quantity(float(match.group('unit_price')), 1)

    def _process_buy_get_free_specific(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Buy X Get Y Free' specific promotions."""
        quantity = int(match.group('quantity'))
        free = int(match.group('free'))
        volume_deals_price = price * quantity
        return self._process_price_and_quantity(volume_deals_price, quantity + free)

    def _process_save_on_quantity(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process '$X SAVE $Y on Z' type promotions."""
        try:
            total_price = float(match.group('total_price'))
        except IndexError:
            total_price = price
            
        discount = float(match.group('discount'))
        quantity = float(match.group('quantity'))        
        
        if mode =="volume_deals":
            volume_deals_price = total_price - discount
            return self._process_price_and_quantity(volume_deals_price, quantity)
        elif mode == "digital_coupon":
            volume_deals_price = total_price - discount
            unit_price = round(price - (discount / quantity), 2)
            return {"unit_price": unit_price, "digital_coupon_price": discount}
    
    def _process_weight_based_price(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process '$X/lb When you buy Y (Z)' type promotions."""
        volume_deals_price = float(match.group('volume_deals_price'))
        quantity_word = match.group('quantity')
        quantity = self._convert_word_to_number(quantity_word)
        return self._process_price_and_quantity(volume_deals_price, quantity)
    
    def _process_buy_get_discount(self, match: re.Match, price: float, weight: float = None, mode="volume_deals") -> Dict:
        """Process 'Buy X get Y% off' type promotions."""
        quantity = int(match.group('quantity'))
        discount_percentage = int(match.group('discount'))
        volume_deals_price = price * quantity * (1 - discount_percentage / 100)
        return self._process_price_and_quantity(volume_deals_price, quantity)

    def has_no_valid_volume_deals_description(self, item):
        try:
            return item["volume_deals_description"].split() and not (
                item.get("volume_deals_price") or
                item.get("unit_price") or
                re.match(r"\$(?P<unit_price>\d+(?:\.\d+)?)\s?$", item["volume_deals_description"])
            )
        except Exception as e:
            print(f"Item: {item}, Promo Description: {item['volume_deals_description']}")
            print(f"Error: {e}")
    
    def has_no_valid_digital_coupon_description(self, item):
        try:
            return item["digital_coupon_short_description"].split() and not (
                item.get("volume_deals_price") or
                item.get("unit_price") or
                re.match(r"\$(?P<unit_price>\d+(?:\.\d+)?)\s?$", item["digital_coupon_short_description"])
            )
        except Exception as e:
            print(f"Item: {item}, Digital coupon Description: {item['digital_coupon_short_description']}")
            print(f"Error: {e}")


    def valid_results(self, item):
        if not item["volume_deals_description"] or not item["digital_coupon_short_description"]:
            return False
        elif re.match(r"\$(?P<unit_price>\d+(?:\.\d+)?)\s?$", item["volume_deals_description"]) \
            or re.match(r"\$(?P<unit_price>\d+(?:\.\d+)?)\s?$", item["digital_coupon_short_description"]):
            return False
        elif re.match(r"\$(?P<unit_price>\d+(?:\.\d+)?)\/lb\s?$", item["volume_deals_description"]) \
            or re.match(r"\$(?P<unit_price>\d+(?:\.\d+)?)\/lb\s?$", item["digital_coupon_short_description"]):
            return False
        return True
        
    
    def save_results(self, file_path: Path, missing_items_file: Path) -> None:
        """Save the processed results and missing items (items without promo/unit price)."""
        def write_json(path: Path, data: List[Dict]) -> None:
            with open(path, "w") as f:
                json.dump(data, f, indent=4)

        results = [i for i in self.results if self.valid_results(i)]
        write_json(file_path, results)
        write_json(missing_items_file, [
            {  
                "regular_price": item["regular_price"],
                "sale_price": item["sale_price"],
                "volume_deals_description": item["volume_deals_description"],
                "volume_deals_price": item.get("volume_deals_price"),
                "digital_coupon_short_description": item["digital_coupon_short_description"],
                "unit_price": item.get("unit_price")
            }
            for item in self.results if self.has_no_valid_volume_deals_description(item)
        ])



def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Batch Processing CLI")
    parser.add_argument("-I", "--input-file", required=True, help="Path to the input file")
    parser.add_argument("-O", "--output-file", required=True, help="Output file name (without extension)")
    parser.add_argument("-f", "--format", choices=["json", "csv", "tsv", "excel"], default="json",
                        help="Output file format (default: json)")
    parser.add_argument("-p", "--prompt-file", help="Path to the prompt file (optional)")
    parser.add_argument("--pre-process", action="store_true", help="Pre-process the input file (optional)")

    return parser.parse_args()

def pre_process(items):
    for item in items:
        if isinstance(item["volume_deals_description"], list):
            item["volume_deals_description"] = " ".join(item["volume_deals_description"])
        
        if isinstance(item["digital_coupon_short_description"], list):
            item["digital_coupon_short_description"] = " ".join(item["digital_coupon_short_description"])
            
        if isinstance(item["regular_price"], str) and "$" in item["regular_price"]:
            item["regular_price"] = float(item.get("regular_price", "").replace("$", ""))
            
        if isinstance(item["sale_price"], str) and "$" in item["sale_price"]:
            item["sale_price"] = float(item.get("sale_price", "").replace("$", ""))
            
    return items

def main():
    args = parse_arguments()    
    with open(args.input_file, "r") as f:
        items = json.load(f)
        if args.pre_process:
            items = pre_process(items)

    processor = PromoProcessor()
    for item in items:
        processor.process(item)
    
    logger.info(f"Processed {len(processor.results)} items.")
       
    processor.save_results(args.output_file, base_dir / "output" / "target_missing.json")

if __name__ == "__main__":
    item = {
        "regular_price": "$13.99",
        "sale_price": "",
        "volume_deals_description": "$10.99 price each with 2  Keurig K-Cup pods",
    }
    processor = PromoProcessor()
    print(processor.process(item))

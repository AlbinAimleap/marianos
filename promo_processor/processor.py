import re
import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, TypeVar, Union, List
from pathlib import Path
from abc import ABC, abstractmethod


T = TypeVar("T", bound="PromoProcessor")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = RotatingFileHandler('app.log', maxBytes=1000000, backupCount=10)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(handler)

class PromoProcessor(ABC):
    subclasses = []
    results = []
    NUMBER_MAPPING = {"ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5, "SIX": 6, "SEVEN": 7, "EIGHT": 8, "NINE": 9, "TEN": 10}

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        PromoProcessor.subclasses.append(cls)
        cls.logger = logging.getLogger(cls.__name__)
        PromoProcessor.set_processor_precedence()

    @classmethod
    def apply_store_brands(cls, item: Dict[str, Any]) -> Dict[str, Any]:
        store_brands = {
            'marianos': ["Private Selection", "Kroger", "Simple Truth", "Simple Truth Organic"],
            'target': ["Deal Worthy", "Good & Gather", "Market Pantry", "Favorite Day", "Kindfull", "Smartly", "Up & Up"],
            'jewel': ['Lucerne', "Signature Select", "O Organics", "Open Nature", "Waterfront Bistro", "Primo Taglio",
                      "Soleil", "Value Corner", "Ready Meals"],
            'walmart': ["Clear American", "Great Value", "Home Bake Value", "Marketside", 
                        "Co Squared", "Best Occasions", "Mash-Up Coffee", "World Table"]
        }
        store_brands_list = [brand for brands in store_brands.values() for brand in brands if brand.casefold() in item["product_title"].casefold()]    
        item["store_brand"] = "y" if any(store_brands_list) else "n"
        return item

    @property
    @abstractmethod
    def patterns(self):
        """Each subclass must define its own patterns."""
        pass

    @abstractmethod
    def calculate_deal(self, item_data: Dict[str, Any], match: re.Match) -> Dict[str, Any]:
        """Each subclass should implement deal calculation logic here."""
        pass

    @abstractmethod
    def calculate_coupon(self, item_data: Dict[str, Any], match: re.Match) -> Dict[str, Any]:
        """Each subclass should implement coupon calculation logic here."""
        pass

    @classmethod
    def process_item(cls, item_data: Dict[str, Any]) -> T:
        """Process a list of items or a single item."""
        if isinstance(item_data, list):
            cls.results.extend([cls.apply_store_brands(cls.process_single_item(item)) for item in item_data])
        else:
            cls.results.append(cls.apply_store_brands(cls.process_single_item(item_data)))
        return cls

    @classmethod
    def to_json(cls, filename: Union[str, Path]) -> None:
        with open(filename, "w") as f:
            json.dump(cls.results, f, indent=4)

    @classmethod
    def process_single_item(cls, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a single item, checking all processors and patterns."""
        updated_item = item_data.copy()

        if not hasattr(cls, "logger"):
            cls.logger = logging.getLogger(cls.__name__)

        # Process deals first across all processors and patterns
        deal_processed = False
        for processor_class in sorted(cls.subclasses, key=lambda x: getattr(x, 'PRECEDENCE', float('inf'))):
            processor = processor_class()

            for pattern in processor.patterns:
                deal_match = re.search(pattern, updated_item.get("volume_deals_description", ""))
                if deal_match:
                    cls.logger.info(f"DEALS: {processor_class.__name__}: {item_data['volume_deals_description']}")
                    updated_item = processor.calculate_deal(updated_item, deal_match)
                    deal_processed = True
                    break

            if deal_processed:
                break
        # Process coupons similarly across all processors and patterns
        coupon_processed = False
        for processor_class in sorted(cls.subclasses, key=lambda x: getattr(x, 'PRECEDENCE', float('inf'))):
            processor = processor_class()

            for pattern in processor.patterns:
                coupon_match = re.search(pattern, updated_item.get("digital_coupon_description", ""))
                if coupon_match:
                    cls.logger.info(f"COUPONS: {processor_class.__name__}: ({item_data['digital_coupon_description']}, {pattern})")
                    updated_item = processor.calculate_coupon(updated_item, coupon_match)
                    coupon_processed = True
                    break

            if coupon_processed:
                break

        return updated_item

    @classmethod
    def calculate_pattern_precedence(cls, pattern: str) -> int:
        """
        Calculate precedence score for a pattern. Higher score = higher precedence.
        Scoring criteria:
        - Exact matches (fewer wildcards/optional parts)
        - Pattern length
        - Number of capture groups
        - Specific character classes vs general wildcards
        """
        score = 0
        score += len(pattern)
        score += len(re.findall(r'\(.*?\)', pattern)) * 10
        score -= len(re.findall(r'[\.\*\+\?\[\]]', pattern)) * 5
        score += len(re.findall(r'\[.*?\]', pattern)) * 3
        score += len(re.findall(r'\b', pattern)) * 2
        return score

    @classmethod
    def set_processor_precedence(cls) -> None:
        """
        Set precedence for all processor subclasses based on their patterns.
        Higher precedence = processed first
        """
        for processor_class in cls.subclasses:
            max_pattern_score = 0
            for pattern in processor_class.patterns:
                pattern_score = cls.calculate_pattern_precedence(pattern)
                max_pattern_score = max(max_pattern_score, pattern_score)
            processor_class.PRECEDENCE = max_pattern_score

import json
import pandas as pd
from promo_processor import PromoProcessor

from pathlib import Path

cur_dir = Path(__file__).parent


def main():
    path = cur_dir / "26-11-2024-Grocessary-Target-output-v1.xlsx"
    data = pd.read_excel(path)
    data.fillna("", inplace=True)
    data["crawl_date"] = data["crawl_date"].astype(str)
    data = data.to_dict(orient="records")
    processor = PromoProcessor.process_item(data)
    results = processor.results
    
    with open("target_26-11-2024.json", "w") as f:
        json.dump(results, f, indent=4)
    
    data = pd.DataFrame(results)
    data.to_csv("target_26-11-2024.csv", index=False)

if __name__ == "__main__":
    main()
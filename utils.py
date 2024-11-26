import re
import pandas as pd

def extract_quantity(description):
    """Extract quantity from volume deals description."""
    quantity_pattern = r'(?<!\$\d+(?:\.\d+)?/|\babout\s+\$\d+(?:\.\d+)?\s+each)(?:\b(\d+)\s*(?:x|for|pack|items?|piece|pc|pcs|count|\bct\b|buy|\bget\b))'    
    match = re.search(quantity_pattern, description, re.IGNORECASE)
    return description if match else ""

def process_dataframe(df):
    """Process dataframe to extract volume deals quantities."""
    df["volume_deals_description"] = df["volume_deals_description"].apply(extract_quantity)
    return df

def load_data(file_path):
    """Load and prepare data from JSON file."""
    data = pd.read_json(file_path)
    return data.fillna("")

def reformat_data(data):
    data = pd.DataFrame(data)
    data = process_dataframe(data)
    return data.to_dict(orient="records")
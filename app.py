from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict
import pandas as pd
import numpy as np
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name
import math
from datetime import datetime
import base64
import logging
import gspread
from google.oauth2.service_account import Credentials
import json
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
SHIPPING_RATE = 77
OPERATIONAL_RATE = 65
processed_files: Dict[str, bytes] = {}
processed_json_data = {
    'shopify': [],
    'campaign': {}
}
processed_filenames = {
    'shopify': None,
    'campaign': None
}
# ---- GOOGLE SHEETS FUNCTIONS ----
def validate_store_consistency(filenames: list) -> str:
    """
    Extract and validate store name from filenames.
    All files must be from the same store.
    
    Expected format: STORENAME_type_dates.ext
    Example: HCC_campaign_raw_4thOct-16thOct.xlsx
    
    Returns: Store name (e.g., "HCC")
    Raises: HTTPException if store names are inconsistent
    """
    store_names = set()
    
    for filename in filenames:
        # Remove file extension
        name_without_ext = filename.rsplit('.', 1)[0]
        
        # Extract store name (first part before underscore)
        parts = name_without_ext.split('_')
        if len(parts) >= 1:
            store_name = parts[0].strip()
            store_names.add(store_name)
    
    if len(store_names) == 0:
        raise HTTPException(
            status_code=400, 
            detail="Could not extract store name from filenames"
        )
    
    if len(store_names) > 1:
        raise HTTPException(
            status_code=400, 
            detail=f"All files must be from the same store. Found stores: {', '.join(store_names)}"
        )
    
    return store_names.pop()


def safe_lookup_get(lookup_dict, product, default=0.0):
    """Safely get value from lookup dictionary. Always returns float."""
    if not lookup_dict or product not in lookup_dict:
        return float(default)
    
    value = lookup_dict[product]
    
    if isinstance(value, dict):
        logger.error(f"⚠️ WARNING: Found nested dict for product '{product}' - using default")
        return float(default)
    
    try:
        return float(value)
    except (TypeError, ValueError):
        logger.error(f"⚠️ WARNING: Could not convert to float for '{product}': {value}")
        return float(default)


def parse_product_data_json(json_string: str) -> pd.DataFrame:
    """
    Parse product data JSON and convert to DataFrame
    Expected JSON structure:
    [
      {
        "Product Title": "Product A",
        "Product Variant Title": "Variant 1",
        "Delivery Rate": 75,
        "Product Cost (Input)": 150,
        "Store name": "Store1",
        "Status": "Complete"
      }
    ]
    """
    try:
        if not json_string or json_string.strip() == "":
            logger.info("No product data JSON provided - empty database")
            return pd.DataFrame()
        
        # Parse JSON string
        product_list = json.loads(json_string)
        
        if not product_list or len(product_list) == 0:
            logger.info("Product data JSON is empty")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(product_list)
        
        logger.info(f"✓ Parsed {len(df)} products from JSON input")
        
        # Validate required columns
        required_columns = ["Product Title", "Product Variant Title", "Store name"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns in JSON: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Ensure Delivery Rate and Product Cost columns exist
        if "Delivery Rate" not in df.columns:
            df["Delivery Rate"] = 0
        if "Product Cost (Input)" not in df.columns:
            df["Product Cost (Input)"] = 0
        
        return df
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {str(e)}")
        raise ValueError(f"Invalid JSON format: {str(e)}")
    except Exception as e:
        logger.error(f"Error parsing product data JSON: {str(e)}")
        raise ValueError(f"Error parsing product data: {str(e)}")


def match_products_with_database(shopify_df: pd.DataFrame, database_df: pd.DataFrame, store_name: str):
    """
    Match Shopify products with Google Sheets database
    Returns: (updated_shopify_df, matched_count, unmatched_products_list)
    """
    if database_df.empty:
        logger.warning("Database is empty - treating all products as unmatched")
    # Add empty columns for Delivery Rate and Product Cost
        shopify_df['Delivery Rate'] = ''
        shopify_df['Product Cost (Input)'] = ''
    
    # Create unmatched list for ALL products
        unmatched = []
        for _, row in shopify_df.iterrows():
          unmatched.append({
            'product_title': row['Product title'],
            'variant_title': row['Product variant title'],
            'store_name': store_name,
            'net_items_sold': int(row.get('Net items sold', 0)) if pd.notna(row.get('Net items sold', 0)) else 0,
            'reason': 'New store - database empty'
          })
    
        logger.info(f"All {len(unmatched)} products marked as unmatched (new store)")
        return shopify_df, 0, unmatched
    
    # Normalize database data for matching
    database_df['_db_product_norm'] = database_df['Product Title'].astype(str).str.strip().str.lower()
    database_df['_db_variant_norm'] = database_df['Product Variant Title'].astype(str).str.strip().str.lower()
    # Find Store Name column (case-insensitive)
    store_col = None
    for col in database_df.columns:
        if col.lower().replace(' ', '') == 'storename':
            store_col = col
            break
    
    if store_col:
        database_df['_db_store_norm'] = database_df[store_col].astype(str).str.strip().str.lower()
    else:
        logger.warning("Store Name column not found in database")
        database_df['_db_store_norm'] = ''
    
    # Create lookup dictionary
    database_lookup = {}
    for _, row in database_df.iterrows():
        key = (
            row['_db_product_norm'],
            row['_db_variant_norm'],
            row['_db_store_norm']
        )
        database_lookup[key] = {
            'delivery_rate': row.get('Delivery Rate', ''),
            'product_cost': row.get('Product Cost (Input)', '')
        }
    
    logger.info(f"Created database lookup with {len(database_lookup)} entries")
    
    # Normalize Shopify data for matching
    shopify_df['_shopify_product_norm'] = shopify_df['Product title'].astype(str).str.strip().str.lower()
    shopify_df['_shopify_variant_norm'] = shopify_df['Product variant title'].astype(str).str.strip().str.lower()
    store_name_norm = store_name.strip().lower()
    
    # Match and populate
    matched_count = 0
    unmatched_products = []
    
    for idx, row in shopify_df.iterrows():
        key = (
            row['_shopify_product_norm'],
            row['_shopify_variant_norm'],
            store_name_norm
        )
        
        if key in database_lookup:
            # MATCH FOUND - populate values
            shopify_df.loc[idx, 'Delivery Rate'] = database_lookup[key]['delivery_rate']
            shopify_df.loc[idx, 'Product Cost (Input)'] = database_lookup[key]['product_cost']
            matched_count += 1
            logger.debug(f"Matched: {row['Product title']} - {row['Product variant title']}")
        else:
            # NO MATCH - add to unmatched list
            unmatched_products.append({
                'product_title': row['Product title'],
                'variant_title': row['Product variant title'],
                'store_name': store_name,
                'net_items_sold': int(row.get('Net items sold', 0)) if pd.notna(row.get('Net items sold', 0)) else 0,
                'reason': 'Not found in database'
            })
            logger.debug(f"Unmatched: {row['Product title']} - {row['Product variant title']}")
    
    # Clean up temporary columns
    shopify_df = shopify_df.drop(columns=['_shopify_product_norm', '_shopify_variant_norm'], errors='ignore')
    
    logger.info(f"Matching complete: {matched_count} matched, {len(unmatched_products)} unmatched")
    
    return shopify_df, matched_count, unmatched_products


def extract_date_range_from_filename(filename: str) -> str:
    """Extract date range from filename like HCC_shopify_raw_4thOct-16thOct.xlsx"""
    pattern = r'_raw_([^.]+)'
    match = re.search(pattern, filename)
    
    if match:
        return match.group(1)
    
    # Fallback: try to find date pattern directly
    date_pattern = r'(\d{1,2}(?:st|nd|rd|th)?[A-Za-z]{3,9}[-_]\d{1,2}(?:st|nd|rd|th)?[A-Za-z]{3,9})'
    date_match = re.search(date_pattern, filename)
    
    if date_match:
        return date_match.group(1)
    
    # Final fallback: use today's date
    from datetime import datetime
    today = datetime.now()
    day = today.day
    month = today.strftime('%b')
    return f"{day}th{month}"


def generate_processed_filename(store_name: str, file_type: str, raw_filename: str) -> str:
    """
    Generate processed filename with format: STORENAME_filetype_processed_DATERANGE.xlsx
    Example: HCC_shopify_processed_4thOct-16thOct.xlsx
    """
    date_range = extract_date_range_from_filename(raw_filename)
    return f"{store_name}_{file_type}_processed_{date_range}"




def create_unmatched_products_list(shopify_df: pd.DataFrame, store_name: str, reason: str = "Not checked"):
    """Create list of all products as unmatched"""
    unmatched = []
    for _, row in shopify_df.iterrows():
        unmatched.append({
            'product_title': row['Product title'],
            'variant_title': row['Product variant title'],
            'store_name': store_name,
            'net_items_sold': int(row.get('Net items sold', 0)) if pd.notna(row.get('Net items sold', 0)) else 0,
            'reason': reason
        })
    return unmatched




# ---- EXISTING HELPER FUNCTIONS ----
def safe_write(worksheet, row, col, value, cell_format=None):
    """Wrapper around worksheet.write to handle NaN/inf safely"""
    if isinstance(value, (int, float)):
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            value = 0
    else:
        if pd.isna(value):
            value = ""
    worksheet.write(row, col, value, cell_format)

def fuzzy_match_to_campaign(name, choices, cutoff=85):
    if not choices:
        return name
    result = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio, score_cutoff=cutoff)
    return result[0] if result else name

def find_date_column(df):
    """Find date column in dataframe"""
    date_columns = []
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['day', 'date', 'time']):
            date_columns.append(col)
    return date_columns[0] if date_columns else None

def clean_product_name(name: str) -> str:
    text = str(name).strip()
    match = re.split(r"[-/|]|\s[xX]\s", text, maxsplit=1)
    base = match[0] if match else text
    base = base.lower()
    base = re.sub(r'[^a-z0-9 ]', '', base)
    base = re.sub(r'\s+', ' ', base)
    return base.strip().title()

def standardize_campaign_columns(df):
    """Standardize campaign column names and handle currency conversion"""
    df = df.copy()
    info_messages = []
    
    # Find and preserve original date column
    date_col = find_date_column(df)
    if date_col:
        df['Date'] = df[date_col]
        if date_col != 'Date':
            df = df.drop(columns=[date_col])
        info_messages.append(f"Found date column: {date_col}")
    
    # Find and preserve Delivery status column
    delivery_status_col = None
    for col in df.columns:
        col_lower = col.lower()
        if ('delivery' in col_lower and 'status' in col_lower) or col_lower == 'campaign delivery':
            delivery_status_col = col
            break

    if delivery_status_col:
        def normalize_delivery_status(value):
            if pd.isna(value) or str(value).strip() == "":
                return ""
            
            value_lower = str(value).strip().lower()
            
            if "active" in value_lower and "inactive" not in value_lower:
                return "Active"
            else:
                return "Inactive"
        
        df['Delivery status'] = df[delivery_status_col].apply(normalize_delivery_status)
        
        if delivery_status_col != 'Delivery status':
            df = df.drop(columns=[delivery_status_col])
        info_messages.append(f"Found Delivery status column: {delivery_status_col}")
    
    # Find purchases/results column
    purchases_col = None
    for col in df.columns:
        if col.lower() in ['purchases', 'results']:
            purchases_col = col
            break
    
    if purchases_col and purchases_col != 'Purchases':
        df = df.rename(columns={purchases_col: 'Purchases'})
        info_messages.append(f"Renamed '{purchases_col}' to 'Purchases'")
    
    # Find amount spent column and handle currency
    amount_col = None
    is_inr = False
    
    # Check for USD first
    for col in df.columns:
        if 'amount spent' in col.lower() and 'usd' in col.lower():
            amount_col = col
            is_inr = False
            break
    
    if not amount_col:
        for col in df.columns:
            if 'amount spent' in col.lower() and 'inr' in col.lower():
                amount_col = col
                is_inr = True
                break
    
    if not amount_col:
        for col in df.columns:
            if 'amount spent' in col.lower():
                amount_col = col
                is_inr = True
                break
    
    if amount_col:
        if is_inr:
            df['Amount spent (USD)'] = df[amount_col] / 100
            info_messages.append(f"Converted '{amount_col}' from INR to USD")
        else:
            df['Amount spent (USD)'] = df[amount_col]
            if amount_col != 'Amount spent (USD)':
                info_messages.append(f"Renamed '{amount_col}' to 'Amount spent (USD)'")
        
        if amount_col != 'Amount spent (USD)':
            df = df.drop(columns=[amount_col])
    
    return df, info_messages

def merge_campaign_files(files_data):
    """Merge multiple campaign files"""
    if not files_data:
        return None, []
    
    all_campaigns = []
    all_messages = []
    
    for file_dict in files_data:
        df = file_dict['data']
        if df is not None:
            df, messages = standardize_campaign_columns(df)
            all_messages.extend(messages)
            all_campaigns.append(df)
    
    if not all_campaigns:
        return None, all_messages
    
    merged_df = pd.concat(all_campaigns, ignore_index=True)
    
    group_cols = ["Campaign name"]
    if 'Date' in merged_df.columns:
        group_cols.append('Date')
    
    required_cols = group_cols + ["Amount spent (USD)"]
    if all(col in merged_df.columns for col in required_cols):
        has_purchases = "Purchases" in merged_df.columns
        has_delivery_status = "Delivery status" in merged_df.columns
        agg_dict = {"Amount spent (USD)": "sum"}
        if has_purchases:
            agg_dict["Purchases"] = "sum"
        if has_delivery_status:
            agg_dict["Delivery status"] = "first"
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    all_messages.append(f"Successfully merged {len(files_data)} campaign files")
    
    return merged_df, all_messages

def merge_shopify_files(files_data):
    """Merge multiple Shopify files"""
    if not files_data:
        return None, []
    
    all_shopify = []
    messages = []
    
    for file_dict in files_data:
        df = file_dict['data']
        if df is not None:
            date_col = find_date_column(df)
            if date_col:
                df['Date'] = df[date_col]
                if date_col != 'Date':
                    df = df.drop(columns=[date_col])
                messages.append(f"Found Shopify date column: {date_col}")
            
            all_shopify.append(df)
    
    if not all_shopify:
        return None, messages
    
    merged_df = pd.concat(all_shopify, ignore_index=True)
    
    group_cols = ["Product title", "Product variant title"]
    if 'Date' in merged_df.columns:
        group_cols.append('Date')
    
    required_cols = group_cols + ["Net items sold"]
    if all(col in merged_df.columns for col in required_cols):
        agg_dict = {"Net items sold": "sum"}
        if "Product variant price" in merged_df.columns:
            agg_dict["Product variant price"] = "first"
        
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    messages.append(f"Successfully merged {len(files_data)} Shopify files")
    
    return merged_df, messages

def merge_reference_files(files_data):
    """Merge multiple reference files"""
    if not files_data:
        return None, []
    
    all_references = []
    messages = []
    
    for file_dict in files_data:
        df = file_dict['data']
        if df is not None:
            required_old_cols = ["Product title", "Product variant title", "Delivery Rate"]
            if all(col in df.columns for col in required_old_cols):
                current_product = None
                for idx, row in df.iterrows():
                    if pd.notna(row["Product title"]) and row["Product title"].strip() != "":
                        if row["Product variant title"] == "ALL VARIANTS (TOTAL)":
                            current_product = row["Product title"]
                        else:
                            current_product = row["Product title"]
                    else:
                        if current_product:
                            df.loc[idx, "Product title"] = current_product

                df_filtered = df[
                    (df["Product variant title"] != "ALL VARIANTS (TOTAL)") &
                    (df["Product variant title"] != "ALL PRODUCTS") &
                    (df["Delivery Rate"].notna()) & (df["Delivery Rate"] != "")
                ]
                
                if not df_filtered.empty:
                    df_filtered["Product title_norm"] = df_filtered["Product title"].astype(str).str.strip().str.lower()
                    df_filtered["Product variant title_norm"] = df_filtered["Product variant title"].astype(str).str.strip().str.lower()
                    all_references.append(df_filtered)
            else:
                messages.append(f"Reference file {file_dict['filename']} doesn't contain required columns")
    
    if not all_references:
        return None, messages
    
    merged_df = pd.concat(all_references, ignore_index=True)
    
    merged_df = merged_df.drop_duplicates(
        subset=["Product title_norm", "Product variant title_norm"], 
        keep="last"
    )
    
    has_product_cost = "Product Cost (Input)" in merged_df.columns
    messages.append(f"Successfully merged {len(files_data)} reference files")
    
    if has_product_cost:
        product_cost_count = merged_df["Product Cost (Input)"].notna().sum()
        messages.append(f"Product cost records found: {product_cost_count}")
    
    return merged_df, messages








def convert_shopify_to_excel(df, shipping_rate=77, operational_rate=65):
    """Original Shopify Excel conversion function (fallback)"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Shopify Data")
        writer.sheets["Shopify Data"] = worksheet

        # Formats
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11
        })
        variant_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11
        })

        # Header
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Column indexes
        delivered_col = df.columns.get_loc("Delivered Orders")
        sold_col = df.columns.get_loc("Net items sold")
        rate_col = df.columns.get_loc("Delivery Rate")
        revenue_col = df.columns.get_loc("Net Revenue")
        price_col = df.columns.get_loc("Product variant price")
        shipping_col = df.columns.get_loc("Shipping Cost")
        operation_col = df.columns.get_loc("Operational Cost")
        product_cost_col = df.columns.get_loc("Product Cost (Output)")
        product_cost_input_col = df.columns.get_loc("Product Cost (Input)")
        net_profit_col = df.columns.get_loc("Net Profit")
        ad_spend_col = df.columns.get_loc("Ad Spend (USD)")
        net_profit_percent_col = df.columns.get_loc("Net Profit (%)")
        product_title_col = df.columns.get_loc("Product title")
        variant_title_col = df.columns.get_loc("Product variant title")

        cols_to_sum = [
            "Net items sold", "Delivered Orders", "Net Revenue", "Ad Spend (USD)",
            "Shipping Cost", "Operational Cost", "Product Cost (Output)", "Net Profit"
        ]
        cols_to_sum_idx = [df.columns.get_loc(c) for c in cols_to_sum]

        # Grand total row
        grand_total_row_idx = 1
        worksheet.write(grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        worksheet.write(grand_total_row_idx, 1, "ALL PRODUCTS", grand_total_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # Products
        for product, product_df in df.groupby("Product title"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            worksheet.write(product_total_row_idx, 0, product, product_total_format)
            worksheet.write(product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", product_total_format)

            n_variants = len(product_df)
            first_variant_row_idx = product_total_row_idx + 1
            last_variant_row_idx = product_total_row_idx + n_variants

            # Product SUMs
            for col_idx in cols_to_sum_idx:
                col_letter = xl_col_to_name(col_idx)
                excel_first = first_variant_row_idx + 1
                excel_last = last_variant_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, col_idx,
                    f"=SUM({col_letter}{excel_first}:{col_letter}{excel_last})",
                    product_total_format
                )

            # Product weighted avg Delivery Rate
            sold_col_letter = xl_col_to_name(sold_col)
            rate_col_letter = xl_col_to_name(rate_col)
            excel_first = first_variant_row_idx + 1
            excel_last = last_variant_row_idx + 1
            worksheet.write_formula(
                product_total_row_idx, rate_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({rate_col_letter}{excel_first}:{rate_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                product_total_format
            )

            # Product weighted avg Product variant price
            price_col_letter = xl_col_to_name(price_col)
            worksheet.write_formula(
                product_total_row_idx, price_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({price_col_letter}{excel_first}:{price_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                product_total_format
            )

            # Product weighted avg Product Cost (Input)
            pc_input_col_letter = xl_col_to_name(product_cost_input_col)
            worksheet.write_formula(
                product_total_row_idx, product_cost_input_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({pc_input_col_letter}{excel_first}:{pc_input_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                product_total_format
            )

            # Product Net Profit %
            rev_col_letter = xl_col_to_name(revenue_col)
            np_col_letter = xl_col_to_name(net_profit_col)
            excel_row = product_total_row_idx + 1
            worksheet.write_formula(
                product_total_row_idx, net_profit_percent_col,
                f"=IF(N({rev_col_letter}{excel_row})=0,0,"
                f"N({np_col_letter}{excel_row})/N({rev_col_letter}{excel_row})*100)",
                product_total_format
            )

            # Variants
            row += 1
            for _, variant in product_df.iterrows():
                variant_row_idx = row
                excel_row = variant_row_idx + 1

                sold_ref = f"{xl_col_to_name(sold_col)}{excel_row}"
                rate_ref = f"{xl_col_to_name(rate_col)}{excel_row}"
                delivered_ref = f"{xl_col_to_name(delivered_col)}{excel_row}"
                price_ref = f"{xl_col_to_name(price_col)}{excel_row}"
                pc_input_ref = f"{xl_col_to_name(product_cost_input_col)}{excel_row}"
                ad_spend_ref = f"{xl_col_to_name(ad_spend_col)}{excel_row}"
                shipping_ref = f"{xl_col_to_name(shipping_col)}{excel_row}"
                op_ref = f"{xl_col_to_name(operation_col)}{excel_row}"
                pc_output_ref = f"{xl_col_to_name(product_cost_col)}{excel_row}"
                net_profit_ref = f"{xl_col_to_name(net_profit_col)}{excel_row}"
                revenue_ref = f"{xl_col_to_name(revenue_col)}{excel_row}"

                for col_idx, col_name in enumerate(df.columns):
                    if col_idx == product_title_col:
                        worksheet.write(variant_row_idx, col_idx, "", variant_format)
                    elif col_idx == variant_title_col:
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product variant title", ""), variant_format)
                    elif col_name == "Net items sold":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Net items sold", 0), variant_format)
                    elif col_name == "Product variant price":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product variant price", 0), variant_format)
                    elif col_name == "Ad Spend (USD)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Ad Spend (USD)", 0.0), variant_format)
                    elif col_name == "Delivery Rate":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Delivery Rate", ""), variant_format)
                    elif col_name == "Product Cost (Input)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product Cost (Input)", ""), variant_format)
                    elif col_name == "Date":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Date", ""), variant_format)
                    elif col_name == "Delivered Orders":
                        rate_term = f"IF(ISNUMBER({rate_ref}),IF({rate_ref}>1,{rate_ref}/100,{rate_ref}),0)"
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=ROUND(N({sold_ref})*{rate_term},1)",
                            variant_format
                        )
                    elif col_name == "Net Revenue":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({price_ref})*N({delivered_ref})",
                            variant_format
                        )
                    elif col_name == "Shipping Cost":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={shipping_rate}*N({sold_ref})",
                            variant_format
                        )
                    elif col_name == "Operational Cost":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={operational_rate}*N({sold_ref})",
                            variant_format
                        )
                    elif col_name == "Product Cost (Output)":
                        pc_term = f"IF(ISNUMBER({pc_input_ref}),{pc_input_ref},0)"
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={pc_term}*N({delivered_ref})",
                            variant_format
                        )
                    elif col_name == "Net Profit":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({revenue_ref})-N({ad_spend_ref})*100-N({shipping_ref})-N({pc_output_ref})-N({op_ref})",
                            variant_format
                        )
                    elif col_name == "Net Profit (%)":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=IF(N({revenue_ref})=0,0,N({net_profit_ref})/N({revenue_ref})*100)",
                            variant_format
                        )
                    else:
                        worksheet.write(variant_row_idx, col_idx, variant.get(col_name, ""), variant_format)
                row += 1

        # Grand total = sum of product totals
        if product_total_rows:
            for col_idx in cols_to_sum_idx:
                col_letter = xl_col_to_name(col_idx)
                total_refs = [f"{col_letter}{r+1}" for r in product_total_rows]
                worksheet.write_formula(
                    grand_total_row_idx, col_idx,
                    f"=SUM({','.join(total_refs)})",
                    grand_total_format
                )

            # Grand total weighted averages
            sold_col_letter = xl_col_to_name(sold_col)
            rate_col_letter = xl_col_to_name(rate_col)
            product_refs_sold = [f"{sold_col_letter}{r+1}" for r in product_total_rows]
            product_refs_rate = [f"{rate_col_letter}{r+1}" for r in product_total_rows]
            
            # Grand total weighted avg Delivery Rate
            worksheet.write_formula(
                grand_total_row_idx, rate_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_rate)},{','.join(product_refs_sold)})/"
                f"SUM({','.join(product_refs_sold)}))",
                grand_total_format
            )

            # Grand total weighted avg Product variant price
            price_col_letter = xl_col_to_name(price_col)
            product_refs_price = [f"{price_col_letter}{r+1}" for r in product_total_rows]
            worksheet.write_formula(
                grand_total_row_idx, price_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_price)},{','.join(product_refs_sold)})/"
                f"SUM({','.join(product_refs_sold)}))",
                grand_total_format
            )

            # Grand total weighted avg Product Cost (Input)
            pc_input_col_letter = xl_col_to_name(product_cost_input_col)
            product_refs_pc_input = [f"{pc_input_col_letter}{r+1}" for r in product_total_rows]
            worksheet.write_formula(
                grand_total_row_idx, product_cost_input_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_pc_input)},{','.join(product_refs_sold)})/"
                f"SUM({','.join(product_refs_sold)}))",
                grand_total_format
            )

            rev_col_letter = xl_col_to_name(revenue_col)
            np_col_letter = xl_col_to_name(net_profit_col)
            excel_row = grand_total_row_idx + 1
            worksheet.write_formula(
                grand_total_row_idx, net_profit_percent_col,
                f"=IF(N({rev_col_letter}{excel_row})=0,0,N({np_col_letter}{excel_row})/N({rev_col_letter}{excel_row})*100)",
                grand_total_format
            )

        worksheet.freeze_panes(2, 0)
        for i, col in enumerate(df.columns):
            if col in ("Product title", "Product variant title"):
                worksheet.set_column(i, i, 35)
            elif col in ("Product variant price", "Net Revenue", "Ad Spend (USD)", "Shipping Cost", "Operational Cost", "Net Profit"):
                worksheet.set_column(i, i, 15)
            else:
                worksheet.set_column(i, i, 12)

    return output.getvalue()


def convert_shopify_to_excel_with_date_columns_fixed(df, shipping_rate=77, operational_rate=65):
    """Convert Shopify data to Excel with collapsible column groups every 12 columns after base columns"""
    if df is None or df.empty:
        return None
    processed_json_data = []  
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Shopify Data")
        writer.sheets["Shopify Data"] = worksheet

        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to original structure if no dates
            return convert_shopify_to_excel(df)
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        num_days = len(unique_dates)
        
        # Calculate dynamic threshold
        dynamic_threshold = num_days * 1

        # Formats with conditional formatting based on net items sold
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#B4C6E7", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        total_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # Dynamic conditional formats based on calculated threshold (simplified to 2 categories)
        # Format for products with < dynamic_threshold net items sold (Red theme)
        product_total_format_low = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#DC4E23", "font_name": "Calibri", "font_size": 11,  # Red
            "num_format": "#,##0.00"
        })
        variant_format_low = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFCCCB", "font_name": "Calibri", "font_size": 11,  # Light red
            "num_format": "#,##0.00"
        })
        
        # Format for products with >= dynamic_threshold net items sold (Default theme)
        product_total_format_high = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        variant_format_high = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # Define base columns - CHANGED: Cost Per Item to CPI, added BE as 8th column
        base_columns = ["Product title", "Product variant title", "Delivery Rate", "Product Cost (Input)", "Net items sold", "Total Ad Spent", "CPI", "BE"]
        
        # Define metrics that will be repeated for each date (12 metrics = 12 columns per date)
        date_metrics = ["Net items sold", "Avg Price", "Delivery Rate", "Product Cost (Input)", 
                       "Delivered Orders", "Net Revenue", "Ad Spend (USD)", 
                       "Shipping Cost", "Operational Cost", "Product Cost (Output)", 
                       "Net Profit", "Net Profit (%)"]
        
        # Build column structure WITH SEPARATOR COLUMNS
        all_columns = base_columns.copy()
        
        # Add separator column after base columns
        all_columns.append("SEPARATOR_AFTER_BASE")
        
        # Add date-specific columns with separators
        for date in unique_dates:
            for metric in date_metrics:
                all_columns.append(f"{date}_{metric}")
            # Add separator column after each date's columns
            all_columns.append(f"SEPARATOR_AFTER_{date}")
        
        # Add total columns
        for metric in date_metrics:
            all_columns.append(f"Total_{metric}")

        # Write headers (skip separator columns)
        for col_num, col_name in enumerate(all_columns):
            if col_name.startswith("SEPARATOR_"):
                # Leave separator columns empty - don't write any header
                continue
            elif col_name.startswith("Total_"):
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), total_header_format)
            elif "_" in col_name and col_name.split("_")[0] in unique_dates:
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), date_header_format)
            else:
                safe_write(worksheet, 0, col_num, col_name, header_format)

        # SET UP COLUMN GROUPING - ACCOUNT FOR SEPARATOR COLUMNS
        # Base columns are 0, 1, 2, 3, 4, 5, 6, 7 (A, B, C, D, E, F, G, H) - NO GROUPING
        # Separator column after base is column 8 - NO GROUPING
        
        # Start grouping from column 9 (column J) onwards - after base + separator
        start_col = 9  # Column J (after base columns A-H + separator I)
        total_columns = len(all_columns)
        
        # Group every 12 columns + 1 separator = 13 positions starting from column 9
        group_level = 1
        while start_col < total_columns:
            # Skip if this is a separator column
            if start_col < len(all_columns) and all_columns[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
                
            # Find end of this group (12 data columns)
            data_cols_found = 0
            end_col = start_col
            while end_col < total_columns and data_cols_found < 12:
                if not all_columns[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 12:
                    end_col += 1
                    
                    
            
            # Set column grouping only for data columns (skip separators)
            if end_col < total_columns:
                worksheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden':True}  # Start collapsed
                )
            
            # Move to next group - skip the separator column
            start_col = end_col + 1  # +1 to skip separator after this group
        
        # Set base column widths (always visible, NO GROUPING)
        worksheet.set_column(0, 1, 25)  # Product title and variant title
        worksheet.set_column(2, 4, 15)  # Base delivery rate, product cost, net items sold
        worksheet.set_column(5, 5, 18)  # Total Ad Spent
        worksheet.set_column(6, 6, 15)  # CPI
        worksheet.set_column(7, 7, 15)  # BE
        worksheet.set_column(8, 8, 3)   # Separator column after base - narrow width

        # Configure outline settings for better user experience
        worksheet.outline_settings(
            symbols_below=True,    # Show outline symbols below groups
            symbols_right=True,    # Show outline symbols to the right
            auto_style=False       # Don't use automatic styling
        )

        # Grand total row
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "ALL PRODUCTS", grand_total_format)
        
        row = grand_total_row_idx + 1
        product_total_rows = []

        # Group by product and restructure data
        for product, product_df in df.groupby("Product title"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Calculate total net items sold for this product to determine formatting
            total_net_items_for_product = 0
            for _, variant_group in product_df.groupby("Product variant title"):
                for _, row_data in variant_group.iterrows():
                    net_items = row_data.get("Net items sold", 0) or 0
                    total_net_items_for_product += net_items
            
            # Choose formatting based on dynamic threshold (simplified to 2 categories)
            if total_net_items_for_product < dynamic_threshold:
                product_total_format = product_total_format_low
                variant_format = variant_format_low
            else:
                product_total_format = product_total_format_high
                variant_format = variant_format_high

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            safe_write(worksheet, product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", product_total_format)

            # Group variants within product
            variant_rows = []
            row += 1
            
            for (variant_title), variant_group in product_df.groupby("Product variant title"):
                variant_row_idx = row
                variant_rows.append(variant_row_idx)
                
                # Fill base columns for variant
                safe_write(worksheet, variant_row_idx, 0, "", variant_format)  # Empty product title for variant rows
                safe_write(worksheet, variant_row_idx, 1, variant_title, variant_format)
                
                # Calculate simple averages for base delivery rate and product cost
                delivery_rates = []
                product_costs = []
                
                for _, row_data in variant_group.iterrows():
                    delivery_rate = row_data.get("Delivery Rate", 0) or 0
                    product_cost = row_data.get("Product Cost (Input)", 0) or 0
                    
                    if delivery_rate > 0:
                        delivery_rates.append(delivery_rate)
                    if product_cost > 0:
                        product_costs.append(product_cost)
                
                # Use simple averages for base columns
                avg_delivery_rate = sum(delivery_rates) / len(delivery_rates) if delivery_rates else 0
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                variant_total_net_items = variant_group['Net items sold'].sum()
                variant_total_ad_spend = variant_group['Ad Spend (USD)'].sum() if 'Ad Spend (USD)' in variant_group.columns else 0
                
                # Build day-wise data
                variant_day_wise_data = {}
                if 'Date' in variant_group.columns:
                    variant_unique_dates = sorted([str(d) for d in variant_group['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
                    
                    for date in variant_unique_dates:
                        date_data = variant_group[variant_group['Date'].astype(str) == date]
                        
                        if not date_data.empty:
                            row_data = date_data.iloc[0]
                            
                            net_items = row_data.get("Net items sold", 0) or 0
                            avg_price = row_data.get("Product variant price", 0) or 0
                            delivery_rate_val = row_data.get("Delivery Rate", 0) or 0
                            product_cost_input = row_data.get("Product Cost (Input)", 0) or 0
                            ad_spend_usd = row_data.get("Ad Spend (USD)", 0) or 0
                            
                            # Calculate derived values
                            delivery_rate_decimal = delivery_rate_val / 100 if delivery_rate_val > 1 else delivery_rate_val
                            delivered_orders = net_items * delivery_rate_decimal
                            net_revenue = delivered_orders * avg_price
                            shipping_cost = net_items * shipping_rate
                            operational_cost = net_items * operational_rate
                            product_cost_output = delivered_orders * product_cost_input
                            net_profit = net_revenue - (ad_spend_usd * 100) - shipping_cost - operational_cost - product_cost_output
                            net_profit_percent = (net_profit / net_revenue * 100) if net_revenue > 0 else 0
                            
                            variant_day_wise_data[date] = {
                               "net_items_sold": int(net_items),
                               "avg_price": float(round(avg_price, 2)),
                               "delivery_rate": float(round(delivery_rate_val, 2)),
                               "product_cost_input": float(round(product_cost_input, 2)),
                               "delivered_orders": float(round(delivered_orders, 2)),
                               "net_revenue": float(round(net_revenue, 2)),
                               "ad_spend_usd": float(round(ad_spend_usd, 2)),
                               "shipping_cost": float(round(shipping_cost, 2)),
                               "operational_cost": float(round(operational_cost, 2)),
                               "product_cost_output": float(round(product_cost_output, 2)),
                               "net_profit": float(round(net_profit, 2)),
                               "net_profit_percent": float(round(net_profit_percent, 2))
                            }
                
                # ==================== CALCULATE ALL TOTALS ====================
                variant_total_revenue = sum(day["net_revenue"] for day in variant_day_wise_data.values())
                variant_total_profit = sum(day["net_profit"] for day in variant_day_wise_data.values())
                variant_total_delivered_orders = sum(day["delivered_orders"] for day in variant_day_wise_data.values())
                variant_total_shipping_cost = sum(day["shipping_cost"] for day in variant_day_wise_data.values())
                variant_total_operational_cost = sum(day["operational_cost"] for day in variant_day_wise_data.values())
                variant_total_product_cost_output = sum(day["product_cost_output"] for day in variant_day_wise_data.values())

                # Calculate CPI (Cost Per Item) in USD
                variant_cpi = float(round(variant_total_ad_spend / variant_total_net_items, 2)) if variant_total_net_items > 0 else 0.0

                # Calculate BE (Break Even) per item
                variant_be = 0.0
                if variant_total_revenue > 0 and variant_total_delivered_orders > 0:
                    variant_be = float(round(
                        (variant_total_revenue - variant_total_shipping_cost - variant_total_operational_cost - variant_total_product_cost_output) / 100 / variant_total_net_items,
                        2
                    ))

                # Calculate weighted averages for total columns
                total_avg_price = 0.0
                total_delivery_rate = 0.0
                total_product_cost_input = 0.0

                if variant_total_net_items > 0:
                    # Weighted average price
                    price_sum = sum(day["avg_price"] * day["net_items_sold"] for day in variant_day_wise_data.values())
                    total_avg_price = float(round(price_sum / variant_total_net_items, 2))
                    
                    # Weighted average delivery rate
                    delivery_rate_sum = sum(day["delivery_rate"] * day["net_items_sold"] for day in variant_day_wise_data.values())
                    total_delivery_rate = float(round(delivery_rate_sum / variant_total_net_items, 2))
                    
                    # Weighted average product cost
                    cost_sum = sum(day["product_cost_input"] * day["net_items_sold"] for day in variant_day_wise_data.values())
                    total_product_cost_input = float(round(cost_sum / variant_total_net_items, 2))

                # ==================== ADD TO JSON WITH ALL COLUMNS ====================
                processed_json_data.append({
                    # BASE COLUMNS
                    "product_title": str(product),
                    "product_variant": str(variant_title),
                    "delivery_rate": float(round(avg_delivery_rate, 2)),
                    "product_cost": float(round(avg_product_cost, 2)),
                    "net_items_sold": int(variant_total_net_items),
                    "total_ad_spend": float(round(variant_total_ad_spend, 2)),
                    "cpi": variant_cpi,
                    "be": variant_be,
                    
                    # TOTAL COLUMNS
                    "totals": {
                        "total_net_items_sold": int(variant_total_net_items),
                        "total_avg_price": total_avg_price,
                        "total_delivery_rate": total_delivery_rate,
                        "total_product_cost_input": total_product_cost_input,
                        "total_delivered_orders": float(round(variant_total_delivered_orders, 2)),
                        "total_net_revenue": float(round(variant_total_revenue, 2)),
                        "total_ad_spend": float(round(variant_total_ad_spend, 2)),
                        "total_shipping_cost": float(round(variant_total_shipping_cost, 2)),
                        "total_operational_cost": float(round(variant_total_operational_cost, 2)),
                        "total_product_cost_output": float(round(variant_total_product_cost_output, 2)),
                        "total_net_profit": float(round(variant_total_profit, 2)),
                        "total_net_profit_percentage": float(round((variant_total_profit / variant_total_revenue * 100) if variant_total_revenue > 0 else 0, 2))
                    },
                    
                    # DAY-WISE DATA
                    "day_wise_data": variant_day_wise_data
                })
                safe_write(worksheet, variant_row_idx, 2, round(avg_delivery_rate, 2), variant_format)
                safe_write(worksheet, variant_row_idx, 3, round(avg_product_cost, 2), variant_format)
                
                # Leave Net items sold, Total Ad Spent, CPI, and BE columns empty for variants (will be calculated via formulas)
                safe_write(worksheet, variant_row_idx, 4, "", variant_format)
                safe_write(worksheet, variant_row_idx, 5, "", variant_format)
                safe_write(worksheet, variant_row_idx, 6, "", variant_format)
                safe_write(worksheet, variant_row_idx, 7, "", variant_format)  # BE will reference product total
                
                # Cell references for Excel formulas
                excel_row = variant_row_idx + 1
                base_delivery_rate_ref = f"{xl_col_to_name(2)}{excel_row}"
                base_product_cost_ref = f"{xl_col_to_name(3)}{excel_row}"
                
                # Fill date-specific data and formulas
                for date in unique_dates:
                    date_data = variant_group[variant_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date
                    net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                    avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    product_cost_input_col_idx = all_columns.index(f"{date}_Product Cost (Input)")
                    delivered_orders_col_idx = all_columns.index(f"{date}_Delivered Orders")
                    net_revenue_col_idx = all_columns.index(f"{date}_Net Revenue")
                    ad_spend_col_idx = all_columns.index(f"{date}_Ad Spend (USD)")
                    shipping_cost_col_idx = all_columns.index(f"{date}_Shipping Cost")
                    operational_cost_col_idx = all_columns.index(f"{date}_Operational Cost")
                    product_cost_output_col_idx = all_columns.index(f"{date}_Product Cost (Output)")
                    net_profit_col_idx = all_columns.index(f"{date}_Net Profit")
                    net_profit_percent_col_idx = all_columns.index(f"{date}_Net Profit (%)")
                    
                    # Cell references for this date
                    net_items_ref = f"{xl_col_to_name(net_items_col_idx)}{excel_row}"
                    avg_price_ref = f"{xl_col_to_name(avg_price_col_idx)}{excel_row}"
                    delivery_rate_ref = f"{xl_col_to_name(delivery_rate_col_idx)}{excel_row}"
                    product_cost_input_ref = f"{xl_col_to_name(product_cost_input_col_idx)}{excel_row}"
                    delivered_orders_ref = f"{xl_col_to_name(delivered_orders_col_idx)}{excel_row}"
                    net_revenue_ref = f"{xl_col_to_name(net_revenue_col_idx)}{excel_row}"
                    ad_spend_ref = f"{xl_col_to_name(ad_spend_col_idx)}{excel_row}"
                    shipping_cost_ref = f"{xl_col_to_name(shipping_cost_col_idx)}{excel_row}"
                    operational_cost_ref = f"{xl_col_to_name(operational_cost_col_idx)}{excel_row}"
                    product_cost_output_ref = f"{xl_col_to_name(product_cost_output_col_idx)}{excel_row}"
                    net_profit_ref = f"{xl_col_to_name(net_profit_col_idx)}{excel_row}"
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Actual data for this date
                        net_items = row_data.get("Net items sold", 0) or 0
                        
                        avg_price = row_data.get("Product variant price", 0) or 0
                        delivery_rate = row_data.get("Delivery Rate", 0) or 0
                        product_cost_input = row_data.get("Product Cost (Input)", 0) or 0
                        ad_spend_usd = row_data.get("Ad Spend (USD)", 0) or 0
                        
                        safe_write(worksheet, variant_row_idx, net_items_col_idx, int(net_items), variant_format)
                        safe_write(worksheet, variant_row_idx, avg_price_col_idx, round(avg_price, 2), variant_format)
                        safe_write(worksheet, variant_row_idx, ad_spend_col_idx, round(ad_spend_usd, 2), variant_format)
                        
                        # Date-specific Delivery Rate and Product Cost link to base columns
                        if delivery_rate > 0:
                            safe_write(worksheet, variant_row_idx, delivery_rate_col_idx, round(delivery_rate, 2), variant_format)
                        else:
                            worksheet.write_formula(
                                variant_row_idx, delivery_rate_col_idx,
                                f"=ROUND({base_delivery_rate_ref},2)",
                                variant_format
                            )
                        
                        if product_cost_input > 0:
                            safe_write(worksheet, variant_row_idx, product_cost_input_col_idx, round(product_cost_input, 2), variant_format)
                        else:
                            worksheet.write_formula(
                                variant_row_idx, product_cost_input_col_idx,
                                f"=ROUND({base_product_cost_ref},2)",
                                variant_format
                            )
                        
                    else:
                        # No data for this date - link to base columns and fill other fields with zeros
                        safe_write(worksheet, variant_row_idx, net_items_col_idx, 0, variant_format)
                        safe_write(worksheet, variant_row_idx, avg_price_col_idx, 0.00, variant_format)
                        safe_write(worksheet, variant_row_idx, ad_spend_col_idx, 0.00, variant_format)
                        
                        worksheet.write_formula(
                            variant_row_idx, delivery_rate_col_idx,
                            f"=ROUND({base_delivery_rate_ref},2)",
                            variant_format
                        )
                        worksheet.write_formula(
                            variant_row_idx, product_cost_input_col_idx,
                            f"=ROUND({base_product_cost_ref},2)",
                            variant_format
                        )
                    
                    # FORMULAS for calculated fields (with ROUND for 2 decimal places)
                    
                    # Delivered Orders = Net items sold * Delivery Rate
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    worksheet.write_formula(
                        variant_row_idx, delivered_orders_col_idx,
                        f"=ROUND({net_items_ref}*{rate_term},2)",
                        variant_format
                    )
                    
                    # Net Revenue = Delivered Orders * Average Price
                    worksheet.write_formula(
                        variant_row_idx, net_revenue_col_idx,
                        f"=ROUND({delivered_orders_ref}*{avg_price_ref},2)",
                        variant_format
                    )
                    
                    # Shipping Cost = Net items sold * shipping_rate
                    worksheet.write_formula(
                        variant_row_idx, shipping_cost_col_idx,
                        f"=ROUND({shipping_rate}*{net_items_ref},2)",
                        variant_format
                    )
                    
                    # Operational Cost = Net items sold * operational_rate
                    worksheet.write_formula(
                        variant_row_idx, operational_cost_col_idx,
                        f"=ROUND({operational_rate}*{net_items_ref},2)",
                        variant_format
                    )
                    
                    # Product Cost (Output) = Delivered Orders * Product Cost (Input)
                    pc_term = f"IF(ISNUMBER({product_cost_input_ref}),{product_cost_input_ref},0)"
                    worksheet.write_formula(
                        variant_row_idx, product_cost_output_col_idx,
                        f"=ROUND({pc_term}*{delivered_orders_ref},2)",
                        variant_format
                    )
                    
                    # Net Profit = Net Revenue - Ad Spend (USD)*100 - Shipping Cost - Operational Cost - Product Cost (Output)
                    worksheet.write_formula(
                        variant_row_idx, net_profit_col_idx,
                        f"=ROUND({net_revenue_ref}-{ad_spend_ref}*100-{shipping_cost_ref}-{operational_cost_ref}-{product_cost_output_ref},2)",
                        variant_format
                    )
                    
                    # Net Profit (%) = Net Profit / Net Revenue * 100
                    worksheet.write_formula(
                        variant_row_idx, net_profit_percent_col_idx,
                        f"=ROUND(IF({net_revenue_ref}=0,0,{net_profit_ref}/{net_revenue_ref}*100),2)",
                        variant_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR VARIANT (with ROUND for 2 decimal places)
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Net items sold":
                        # SUM: Add all date-specific net items sold (non-contiguous columns)
                        if len(unique_dates) > 1:
                            # Build individual cell references since columns are not contiguous
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={sum_formula}",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                variant_format
                            )
                    
                    elif metric == "Avg Price":
                        # WEIGHTED AVERAGE: (Price1*NetItems1 + Price2*NetItems2 + ...) / TotalNetItems
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            # Build SUMPRODUCT formula for weighted average
                            price_terms = []
                            for date in unique_dates:
                                price_col_idx = all_columns.index(f"{date}_Avg Price")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                price_terms.append(f"{xl_col_to_name(price_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(price_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND(IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref}),2)",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                variant_format
                            )
                    
                    elif metric == "Delivery Rate":
                        # WEIGHTED AVERAGE: Same as Avg Price
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            rate_terms = []
                            for date in unique_dates:
                                rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                rate_terms.append(f"{xl_col_to_name(rate_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(rate_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND(IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref}),2)",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                variant_format
                            )
                    
                    elif metric == "Product Cost (Input)":
                        # WEIGHTED AVERAGE: Same as Avg Price
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            cost_terms = []
                            for date in unique_dates:
                                cost_col_idx = all_columns.index(f"{date}_Product Cost (Input)")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                cost_terms.append(f"{xl_col_to_name(cost_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(cost_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND(IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref}),2)",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                variant_format
                            )
                    
                    elif metric == "Net Profit (%)":
                        # CALCULATED: Total Net Profit / Total Net Revenue * 100
                        total_net_profit_col_idx = all_columns.index("Total_Net Profit")
                        total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
                        total_net_profit_ref = f"{xl_col_to_name(total_net_profit_col_idx)}{excel_row}"
                        total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{excel_row}"
                        
                        worksheet.write_formula(
                            variant_row_idx, total_col_idx,
                            f"=ROUND(IF({total_net_revenue_ref}=0,0,{total_net_profit_ref}/{total_net_revenue_ref}*100),2)",
                            variant_format
                        )
                    
                    else:
                        # SUM: All other metrics (Delivered Orders, Net Revenue, Ad Spend, etc.)
                        if len(unique_dates) > 1:
                            # Build individual cell references since columns are not contiguous
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            if metric == "Net items sold":  # Don't round net items sold
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"={sum_formula}",
                                    variant_format
                                )
                            else:
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"=ROUND({sum_formula},2)",
                                    variant_format
                                )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            if metric == "Net items sold":  # Don't round net items sold
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"={xl_col_to_name(single_date_col)}{excel_row}",
                                    variant_format
                                )
                            else:
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                    variant_format
                                )
                
                # Calculate base columns for variant (link to total columns)
                total_net_items_col_idx = all_columns.index("Total_Net items sold")
                total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                
                worksheet.write_formula(
                    variant_row_idx, 4,
                    f"={xl_col_to_name(total_net_items_col_idx)}{excel_row}",
                    variant_format
                )
                
                worksheet.write_formula(
                    variant_row_idx, 5,
                    f"=ROUND({xl_col_to_name(total_ad_spend_col_idx)}{excel_row},2)",
                    variant_format
                )
                
                # CPI (Cost Per Item) = Total Ad Spent / Net items sold (in USD)
                worksheet.write_formula(
                    variant_row_idx, 6,
                    f"=ROUND(IF({xl_col_to_name(total_net_items_col_idx)}{excel_row}=0,0,{xl_col_to_name(total_ad_spend_col_idx)}{excel_row}/{xl_col_to_name(total_net_items_col_idx)}{excel_row}),2)",
                    variant_format
                )
                
                # BE - REMOVED: Don't calculate BE for individual variants, will reference product total later
                
                row += 1
                # ==================== CALCULATE PRODUCT TOTALS FOR JSON ====================
            # After processing all variants of a product, calculate product totals
            
            # Find all variants for this product in processed_json_data
            product_variants = [item for item in processed_json_data if item["product_title"] == str(product)]
            
            if product_variants:
                # Calculate product totals from all variants
                product_total_net_items = sum(v["net_items_sold"] for v in product_variants)
                product_total_ad_spend = sum(v["total_ad_spend"] for v in product_variants)
                product_total_revenue = sum(v["totals"]["total_net_revenue"] for v in product_variants)
                product_total_profit = sum(v["totals"]["total_net_profit"] for v in product_variants)
                product_total_delivered_orders = sum(v["totals"]["total_delivered_orders"] for v in product_variants)
                product_total_shipping_cost = sum(v["totals"]["total_shipping_cost"] for v in product_variants)
                product_total_operational_cost = sum(v["totals"]["total_operational_cost"] for v in product_variants)
                product_total_product_cost_output = sum(v["totals"]["total_product_cost_output"] for v in product_variants)
                
                # Calculate product CPI
                product_cpi = float(round(product_total_ad_spend / product_total_net_items, 2)) if product_total_net_items > 0 else 0.0
                
                # Calculate product BE
                product_be = 0.0
                if product_total_revenue > 0 and product_total_net_items > 0:
                    product_be = float(round(
                        (product_total_revenue - product_total_shipping_cost - product_total_operational_cost - product_total_product_cost_output) / 100 / product_total_net_items,
                        2
                    ))
                
                # Calculate weighted averages for product totals
                product_avg_price = 0.0
                product_delivery_rate = 0.0
                product_product_cost = 0.0
                
                if product_total_net_items > 0:
                    # Weighted average price
                    price_sum = sum(v["totals"]["total_avg_price"] * v["net_items_sold"] for v in product_variants)
                    product_avg_price = float(round(price_sum / product_total_net_items, 2))
                    
                    # Weighted average delivery rate
                    delivery_sum = sum(v["delivery_rate"] * v["net_items_sold"] for v in product_variants)
                    product_delivery_rate = float(round(delivery_sum / product_total_net_items, 2))
                    
                    # Weighted average product cost
                    cost_sum = sum(v["product_cost"] * v["net_items_sold"] for v in product_variants)
                    product_product_cost = float(round(cost_sum / product_total_net_items, 2))
                
                # Calculate product day-wise data
                product_day_wise_data = {}
                for date in unique_dates:
                    day_net_items = 0
                    day_revenue = 0.0
                    day_profit = 0.0
                    day_delivered_orders = 0.0
                    day_shipping_cost = 0.0
                    day_operational_cost = 0.0
                    day_product_cost_output = 0.0
                    day_ad_spend = 0.0
                    
                    for variant in product_variants:
                        if date in variant["day_wise_data"]:
                            day_data = variant["day_wise_data"][date]
                            day_net_items += day_data["net_items_sold"]
                            day_revenue += day_data["net_revenue"]
                            day_profit += day_data["net_profit"]
                            day_delivered_orders += day_data["delivered_orders"]
                            day_shipping_cost += day_data["shipping_cost"]
                            day_operational_cost += day_data["operational_cost"]
                            day_product_cost_output += day_data["product_cost_output"]
                            day_ad_spend += day_data["ad_spend_usd"]
                    
                    # Calculate day-wise weighted averages
                    day_avg_price = 0.0
                    day_delivery_rate = 0.0
                    day_product_cost = 0.0
                    
                    if day_net_items > 0:
                        price_sum = sum(
                            variant["day_wise_data"][date]["avg_price"] * variant["day_wise_data"][date]["net_items_sold"]
                            for variant in product_variants
                            if date in variant["day_wise_data"]
                        )
                        day_avg_price = float(round(price_sum / day_net_items, 2))
                        
                        delivery_sum = sum(
                            variant["day_wise_data"][date]["delivery_rate"] * variant["day_wise_data"][date]["net_items_sold"]
                            for variant in product_variants
                            if date in variant["day_wise_data"]
                        )
                        day_delivery_rate = float(round(delivery_sum / day_net_items, 2))
                        
                        cost_sum = sum(
                            variant["day_wise_data"][date]["product_cost_input"] * variant["day_wise_data"][date]["net_items_sold"]
                            for variant in product_variants
                            if date in variant["day_wise_data"]
                        )
                        day_product_cost = float(round(cost_sum / day_net_items, 2))
                    
                    day_profit_percent = float(round((day_profit / day_revenue * 100) if day_revenue > 0 else 0, 2))
                    
                    product_day_wise_data[date] = {
                        "net_items_sold": int(day_net_items),
                        "avg_price": day_avg_price,
                        "delivery_rate": day_delivery_rate,
                        "product_cost_input": day_product_cost,
                        "delivered_orders": float(round(day_delivered_orders, 2)),
                        "net_revenue": float(round(day_revenue, 2)),
                        "ad_spend_usd": float(round(day_ad_spend, 2)),
                        "shipping_cost": float(round(day_shipping_cost, 2)),
                        "operational_cost": float(round(day_operational_cost, 2)),
                        "product_cost_output": float(round(day_product_cost_output, 2)),
                        "net_profit": float(round(day_profit, 2)),
                        "net_profit_percent": day_profit_percent
                    }
                
                # Create product total JSON object
                product_total_json = {
                    "product_title": str(product),
                    "product_variant": "ALL VARIANTS (TOTAL)",
                    "delivery_rate": product_delivery_rate,
                    "product_cost": product_product_cost,
                    "net_items_sold": int(product_total_net_items),
                    "total_ad_spend": float(round(product_total_ad_spend, 2)),
                    "cpi": product_cpi,
                    "be": product_be,
                    "totals": {
                        "total_net_items_sold": int(product_total_net_items),
                        "total_avg_price": product_avg_price,
                        "total_delivery_rate": product_delivery_rate,
                        "total_product_cost_input": product_product_cost,
                        "total_delivered_orders": float(round(product_total_delivered_orders, 2)),
                        "total_net_revenue": float(round(product_total_revenue, 2)),
                        "total_ad_spend": float(round(product_total_ad_spend, 2)),
                        "total_shipping_cost": float(round(product_total_shipping_cost, 2)),
                        "total_operational_cost": float(round(product_total_operational_cost, 2)),
                        "total_product_cost_output": float(round(product_total_product_cost_output, 2)),
                        "total_net_profit": float(round(product_total_profit, 2)),
                        "total_net_profit_percentage": float(round((product_total_profit / product_total_revenue * 100) if product_total_revenue > 0 else 0, 2))
                    },
                    "day_wise_data": product_day_wise_data
                }
                
                # Find the index of the first variant of this product
                first_variant_index = next(i for i, item in enumerate(processed_json_data) if item["product_title"] == str(product))
                
                # Insert the product total BEFORE the first variant
                processed_json_data.insert(first_variant_index, product_total_json)
            # Calculate product totals by aggregating variant rows using RANGES (with ROUND for 2 decimal places)
            if variant_rows:
                # Build ranges for product totals
                first_variant_row = min(variant_rows) + 1  # Excel row numbering
                last_variant_row = max(variant_rows) + 1
                
                # Fill Net items sold, Total Ad Spent, CPI, and BE in base columns for product total
                total_net_items_col_idx = all_columns.index("Total_Net items sold")
                total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                
                worksheet.write_formula(
                    product_total_row_idx, 4,
                    f"={xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, 5,
                    f"=ROUND({xl_col_to_name(total_ad_spend_col_idx)}{product_total_row_idx+1},2)",
                    product_total_format
                )
                
                # CPI for product total (in USD)
                worksheet.write_formula(
                    product_total_row_idx, 6,
                    f"=ROUND(IF({xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_ad_spend_col_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}),2)",
                    product_total_format
                )
                
                # BE for product total (per item) - CHANGED: Use Delivered Orders instead of Net items sold for consistency with Campaign
                total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
                total_shipping_cost_col_idx = all_columns.index("Total_Shipping Cost")
                total_operational_cost_col_idx = all_columns.index("Total_Operational Cost")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost (Output)")
                total_delivered_orders_col_idx = all_columns.index("Total_Net items sold")  # CHANGED: Use Delivered Orders
                
                total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{product_total_row_idx+1}"
                total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{product_total_row_idx+1}"
                total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{product_total_row_idx+1}"
                total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1}"
                total_delivered_orders_ref = f"{xl_col_to_name(total_delivered_orders_col_idx)}{product_total_row_idx+1}"
                
                worksheet.write_formula(
                    product_total_row_idx, 7,
                    f"=ROUND(IF(AND({total_net_revenue_ref}>0,{total_delivered_orders_ref}>0),({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_delivered_orders_ref},0),2)",
                    product_total_format
                )
                
                # AFTER calculating product BE, copy this value to ALL variant rows under this product
                product_be_ref = f"H{product_total_row_idx+1}"  # H is column 7 (BE column)
                for variant_row_idx in variant_rows:
                    worksheet.write_formula(
                        variant_row_idx, 7,
                        f"={product_be_ref}",
                        variant_format
                    )
                
                # PRODUCT TOTAL CALCULATIONS (with ROUND for 2 decimal places)
                for date in unique_dates:
                    for metric in date_metrics:
                        col_idx = all_columns.index(f"{date}_{metric}")
                        
                        if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                            # Weighted average based on net items sold for this date using RANGES
                            date_net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                            
                            metric_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                            net_items_range = f"{xl_col_to_name(date_net_items_col_idx)}{first_variant_row}:{xl_col_to_name(date_net_items_col_idx)}{last_variant_row}"
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF(SUM({net_items_range})=0,0,SUMPRODUCT({metric_range},{net_items_range})/SUM({net_items_range})),2)",
                                product_total_format
                            )
                        elif metric == "Net Profit (%)":
                            # Calculate based on net profit and net revenue for this date
                            net_profit_idx = all_columns.index(f"{date}_Net Profit")
                            net_revenue_idx = all_columns.index(f"{date}_Net Revenue")
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF({xl_col_to_name(net_revenue_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(net_profit_idx)}{product_total_row_idx+1}/{xl_col_to_name(net_revenue_idx)}{product_total_row_idx+1}*100),2)",
                                product_total_format
                            )
                        else:
                            # Sum for other metrics using ranges
                            col_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                            if metric == "Net items sold":  # Don't round net items sold
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=SUM({col_range})",
                                    product_total_format
                                )
                            else:
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=ROUND(SUM({col_range}),2)",
                                    product_total_format
                                )
                
                # Calculate product totals for Total columns using RANGES (with ROUND for 2 decimal places)
                for metric in date_metrics:
                    col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                        # Weighted average based on total net items sold using RANGES
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        
                        metric_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                        net_items_range = f"{xl_col_to_name(total_net_items_col_idx)}{first_variant_row}:{xl_col_to_name(total_net_items_col_idx)}{last_variant_row}"
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(IF(SUM({net_items_range})=0,0,SUMPRODUCT({metric_range},{net_items_range})/SUM({net_items_range})),2)",
                            product_total_format
                        )
                    elif metric == "Net Profit (%)":
                        # Calculate based on total net profit and total net revenue
                        total_net_profit_idx = all_columns.index("Total_Net Profit")
                        total_net_revenue_idx = all_columns.index("Total_Net Revenue")
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(IF({xl_col_to_name(total_net_revenue_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_net_profit_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_net_revenue_idx)}{product_total_row_idx+1}*100),2)",
                            product_total_format
                        )
                    else:
                        # Sum for other metrics using ranges
                        col_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                        if metric == "Net items sold":  # Don't round net items sold
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=SUM({col_range})",
                                product_total_format
                            )
                        else:
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(SUM({col_range}),2)",
                                product_total_format
                            )
                
                # Base columns for product totals - reference the Total weighted averages
                base_delivery_rate_col = 2
                base_product_cost_col = 3
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost (Input)")
                
                worksheet.write_formula(
                    product_total_row_idx, base_delivery_rate_col,
                    f"=ROUND({xl_col_to_name(total_delivery_rate_col_idx)}{product_total_row_idx+1},2)",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, base_product_cost_col,
                    f"=ROUND({xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1},2)",
                    product_total_format
                )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY (with ROUND for 2 decimal places)
        if product_total_rows:
            # Base columns for grand total
            base_delivery_rate_col = 2
            base_product_cost_col = 3
            base_net_items_col = 4
            base_total_ad_spent_col = 5
            base_cpi_col = 6
            base_be_col = 7
            total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
            total_product_cost_col_idx = all_columns.index("Total_Product Cost (Input)")
            total_net_items_col_idx = all_columns.index("Total_Net items sold")
            total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
            
            worksheet.write_formula(
                grand_total_row_idx, base_delivery_rate_col,
                f"=ROUND({xl_col_to_name(total_delivery_rate_col_idx)}{grand_total_row_idx+1},2)",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_product_cost_col,
                f"=ROUND({xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1},2)",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_net_items_col,
                f"={xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_total_ad_spent_col,
                f"=ROUND({xl_col_to_name(total_ad_spend_col_idx)}{grand_total_row_idx+1},2)",
                grand_total_format
            )
            
            # CPI for grand total
            worksheet.write_formula(
                grand_total_row_idx, base_cpi_col,
                f"=ROUND(IF({xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_ad_spend_col_idx)}{grand_total_row_idx+1}*100/{xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1})/100,2)",
                grand_total_format
            )
            
            # BE for grand total (per item) - CHANGED: Use Delivered Orders for consistency with Campaign
            total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
            total_shipping_cost_col_idx = all_columns.index("Total_Shipping Cost")
            total_operational_cost_col_idx = all_columns.index("Total_Operational Cost")
            total_product_cost_col_idx = all_columns.index("Total_Product Cost (Output)")
            total_delivered_orders_col_idx = all_columns.index("Total_Net items sold")  # CHANGED: Use Delivered Orders
            
            total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{grand_total_row_idx+1}"
            total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{grand_total_row_idx+1}"
            total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{grand_total_row_idx+1}"
            total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1}"
            total_delivered_orders_ref = f"{xl_col_to_name(total_delivered_orders_col_idx)}{grand_total_row_idx+1}"
            
            worksheet.write_formula(
                grand_total_row_idx, base_be_col,
                f"=ROUND(IF(AND({total_net_revenue_ref}>0,{total_delivered_orders_ref}>0),({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_delivered_orders_ref},0),2)",
                grand_total_format
            )
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                        # Weighted average using individual product total rows
                        date_net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                        
                        # Build individual cell references for PRODUCT TOTAL rows only
                        metric_refs = []
                        net_items_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                            net_items_refs.append(f"{xl_col_to_name(date_net_items_col_idx)}{product_excel_row}")
                        
                        # Build SUMPRODUCT formula for weighted average
                        sumproduct_terms = []
                        for i in range(len(metric_refs)):
                            sumproduct_terms.append(f"{metric_refs[i]}*{net_items_refs[i]}")
                        
                        sumproduct_formula = "+".join(sumproduct_terms)
                        sum_net_items_formula = "+".join(net_items_refs)
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF(({sum_net_items_formula})=0,0,({sumproduct_formula})/({sum_net_items_formula})),2)",
                            grand_total_format
                        )
                    elif metric == "Net Profit (%)":
                        # Calculate based on net profit and net revenue for this date
                        net_profit_idx = all_columns.index(f"{date}_Net Profit")
                        net_revenue_idx = all_columns.index(f"{date}_Net Revenue")
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF({xl_col_to_name(net_revenue_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(net_profit_idx)}{grand_total_row_idx+1}/{xl_col_to_name(net_revenue_idx)}{grand_total_row_idx+1}*100),2)",
                            grand_total_format
                        )
                    else:
                        # Sum using individual product total rows only
                        sum_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        
                        sum_formula = "+".join(sum_refs)
                        if metric == "Net items sold":  # Don't round net items sold
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"={sum_formula}",
                                grand_total_format
                            )
                        else:
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"=ROUND({sum_formula},2)",
                                grand_total_format
                            )
            
            # Total columns for grand total using INDIVIDUAL PRODUCT TOTAL ROWS (with ROUND for 2 decimal places)
            total_net_items_col_idx = all_columns.index("Total_Net items sold")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                    # Weighted average using individual product total rows
                    
                    # Build individual cell references for PRODUCT TOTAL rows only
                    metric_refs = []
                    net_items_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        net_items_refs.append(f"{xl_col_to_name(total_net_items_col_idx)}{product_excel_row}")
                    
                    # Build SUMPRODUCT formula for weighted average
                    sumproduct_terms = []
                    for i in range(len(metric_refs)):
                        sumproduct_terms.append(f"{metric_refs[i]}*{net_items_refs[i]}")
                    
                    sumproduct_formula = "+".join(sumproduct_terms)
                    sum_net_items_formula = "+".join(net_items_refs)
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=ROUND(IF(({sum_net_items_formula})=0,0,({sumproduct_formula})/({sum_net_items_formula})),2)",
                        grand_total_format
                    )
                elif metric == "Net Profit (%)":
                    # Calculate based on total net profit and total net revenue
                    total_net_profit_idx = all_columns.index("Total_Net Profit")
                    total_net_revenue_idx = all_columns.index("Total_Net Revenue")
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=ROUND(IF({xl_col_to_name(total_net_revenue_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_net_profit_idx)}{grand_total_row_idx+1}/{xl_col_to_name(total_net_revenue_idx)}{grand_total_row_idx+1}*100),2)",
                        grand_total_format
                    )
                else:
                    # Sum using individual product total rows only
                    sum_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                    
                    sum_formula = "+".join(sum_refs)
                    if metric == "Net items sold":  # Don't round net items sold
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"={sum_formula}",
                            grand_total_format
                        )
                    else:
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND({sum_formula},2)",
                            grand_total_format
                        )

        # Freeze panes to keep base columns visible when scrolling
        worksheet.freeze_panes(2, len(base_columns))  # Freeze header and base columns
        # ==================== ADD GRAND TOTAL TO JSON ====================
    if product_total_rows:
        # Calculate grand totals from all product totals
        grand_total_net_items = sum(
            item["net_items_sold"] 
            for item in processed_json_data
        )
        
        grand_total_ad_spend = sum(
            item["total_ad_spend"] 
            for item in processed_json_data
        )
        
        grand_total_revenue = sum(
            item["totals"]["total_net_revenue"] 
            for item in processed_json_data
        )
        
        grand_total_profit = sum(
            item["totals"]["total_net_profit"] 
            for item in processed_json_data
        )
        
        grand_total_delivered_orders = sum(
            item["totals"]["total_delivered_orders"] 
            for item in processed_json_data
        )
        
        grand_total_shipping_cost = sum(
            item["totals"]["total_shipping_cost"] 
            for item in processed_json_data
        )
        
        grand_total_operational_cost = sum(
            item["totals"]["total_operational_cost"] 
            for item in processed_json_data
        )
        
        grand_total_product_cost_output = sum(
            item["totals"]["total_product_cost_output"] 
            for item in processed_json_data
        )
        
        # Calculate grand CPI
        grand_cpi = float(round(grand_total_ad_spend / grand_total_net_items, 2)) if grand_total_net_items > 0 else 0.0
        
        # Calculate grand BE
        grand_be = 0.0
        if grand_total_revenue > 0 and grand_total_delivered_orders > 0:
            grand_be = float(round(
                (grand_total_revenue - grand_total_shipping_cost - grand_total_operational_cost - grand_total_product_cost_output) / 100 / grand_total_net_items,
                2
            ))
        
        # Calculate weighted averages for grand totals
        grand_avg_price = 0.0
        grand_delivery_rate = 0.0
        grand_product_cost = 0.0
        
        if grand_total_net_items > 0:
            # Weighted average price
            price_sum = sum(
                item["totals"]["total_avg_price"] * item["net_items_sold"]
                for item in processed_json_data
            )
            grand_avg_price = float(round(price_sum / grand_total_net_items, 2))
            
            # Weighted average delivery rate
            delivery_sum = sum(
                item["delivery_rate"] * item["net_items_sold"]
                for item in processed_json_data
            )
            grand_delivery_rate = float(round(delivery_sum / grand_total_net_items, 2))
            
            # Weighted average product cost
            cost_sum = sum(
                item["product_cost"] * item["net_items_sold"]
                for item in processed_json_data
            )
            grand_product_cost = float(round(cost_sum / grand_total_net_items, 2))
        
        # Calculate grand totals day-wise data
        grand_day_wise_data = {}
        for date in unique_dates:
            day_net_items = 0
            day_revenue = 0.0
            day_profit = 0.0
            day_delivered_orders = 0.0
            day_shipping_cost = 0.0
            day_operational_cost = 0.0
            day_product_cost_output = 0.0
            day_ad_spend = 0.0
            
            for item in processed_json_data:
                if date in item["day_wise_data"]:
                    day_data = item["day_wise_data"][date]
                    day_net_items += day_data["net_items_sold"]
                    day_revenue += day_data["net_revenue"]
                    day_profit += day_data["net_profit"]
                    day_delivered_orders += day_data["delivered_orders"]
                    day_shipping_cost += day_data["shipping_cost"]
                    day_operational_cost += day_data["operational_cost"]
                    day_product_cost_output += day_data["product_cost_output"]
                    day_ad_spend += day_data["ad_spend_usd"]
            
            # Calculate day-wise weighted averages
            day_avg_price = 0.0
            day_delivery_rate = 0.0
            day_product_cost = 0.0
            
            if day_net_items > 0:
                price_sum = sum(
                    item["day_wise_data"][date]["avg_price"] * item["day_wise_data"][date]["net_items_sold"]
                    for item in processed_json_data
                    if date in item["day_wise_data"]
                )
                day_avg_price = float(round(price_sum / day_net_items, 2))
                
                delivery_sum = sum(
                    item["day_wise_data"][date]["delivery_rate"] * item["day_wise_data"][date]["net_items_sold"]
                    for item in processed_json_data
                    if date in item["day_wise_data"]
                )
                day_delivery_rate = float(round(delivery_sum / day_net_items, 2))
                
                cost_sum = sum(
                    item["day_wise_data"][date]["product_cost_input"] * item["day_wise_data"][date]["net_items_sold"]
                    for item in processed_json_data
                    if date in item["day_wise_data"]
                )
                day_product_cost = float(round(cost_sum / day_net_items, 2))
            
            day_profit_percent = float(round((day_profit / day_revenue * 100) if day_revenue > 0 else 0, 2))
            
            grand_day_wise_data[date] = {
                "net_items_sold": int(day_net_items),
                "avg_price": day_avg_price,
                "delivery_rate": day_delivery_rate,
                "product_cost_input": day_product_cost,
                "delivered_orders": float(round(day_delivered_orders, 2)),
                "net_revenue": float(round(day_revenue, 2)),
                "ad_spend_usd": float(round(day_ad_spend, 2)),
                "shipping_cost": float(round(day_shipping_cost, 2)),
                "operational_cost": float(round(day_operational_cost, 2)),
                "product_cost_output": float(round(day_product_cost_output, 2)),
                "net_profit": float(round(day_profit, 2)),
                "net_profit_percent": day_profit_percent
            }
        
        # Insert grand total at the beginning of the JSON array
        grand_total_json = {
            "product_title": "GRAND TOTAL",
            "product_variant": "ALL PRODUCTS",
            "delivery_rate": grand_delivery_rate,
            "product_cost": grand_product_cost,
            "net_items_sold": int(grand_total_net_items),
            "total_ad_spend": float(round(grand_total_ad_spend, 2)),
            "cpi": grand_cpi,
            "be": grand_be,
            "totals": {
                "total_net_items_sold": int(grand_total_net_items),
                "total_avg_price": grand_avg_price,
                "total_delivery_rate": grand_delivery_rate,
                "total_product_cost_input": grand_product_cost,
                "total_delivered_orders": float(round(grand_total_delivered_orders, 2)),
                "total_net_revenue": float(round(grand_total_revenue, 2)),
                "total_ad_spend": float(round(grand_total_ad_spend, 2)),
                "total_shipping_cost": float(round(grand_total_shipping_cost, 2)),
                "total_operational_cost": float(round(grand_total_operational_cost, 2)),
                "total_product_cost_output": float(round(grand_total_product_cost_output, 2)),
                "total_net_profit": float(round(grand_total_profit, 2)),
                "total_net_profit_percentage": float(round((grand_total_profit / grand_total_revenue * 100) if grand_total_revenue > 0 else 0, 2))
            },
            "day_wise_data": grand_day_wise_data
        }
        
        # Insert at the beginning of the list
        processed_json_data.insert(0, grand_total_json)
    return output.getvalue(), processed_json_data




def convert_final_campaign_to_excel_with_date_columns_fixed(df, shopify_df=None, selected_days=None, shipping_rate=77, operational_rate=65, product_date_avg_prices=None, product_date_delivery_rates=None, product_date_cost_inputs=None
      ):
    """Convert Campaign data to Excel with day-wise lookups integrated and unmatched campaigns sheet"""
    if df.empty:
        return None
    if product_date_avg_prices is None:
        product_date_avg_prices = {}
    if product_date_delivery_rates is None:
        product_date_delivery_rates = {}
    if product_date_cost_inputs is None:
        product_date_cost_inputs = {}
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        
        # ==== MAIN SHEET: Campaign Data ====
        worksheet = workbook.add_worksheet("Campaign Data")
        writer.sheets["Campaign Data"] = worksheet

        # Formats
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11
        })
        date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#B4C6E7", "font_name": "Calibri", "font_size": 11
        })
        total_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11
        })
        campaign_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11
        })
        # NEW: Exclusion table formats
        exclusion_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF9999", "font_name": "Calibri", "font_size": 11
        })
        exclusion_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to original structure if no dates
            return convert_final_campaign_to_excel(df, shopify_df)
        
        # Get unique dates and sort them
        # Get unique dates and sort them CHRONOLOGICALLY
        # First collect all dates
        all_dates = [d for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != '']
        
        # Convert to datetime objects for proper sorting, then back to strings
        from datetime import datetime
        def parse_date(date_str):
            """Parse date string to datetime object for sorting"""
            date_str = str(date_str).strip()
            # Try multiple date formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            # If no format works, return the string as-is (fallback)
            return date_str
        
        # Sort dates chronologically
        try:
            sorted_date_objects = sorted(all_dates, key=parse_date)
            unique_dates = [str(d) for d in sorted_date_objects]
        except:
            # Fallback to string sorting if datetime parsing fails
            unique_dates = sorted([str(d) for d in all_dates])
        if selected_days is None:
            if len(unique_dates) > 0:
                n_days = len(unique_dates)
                selected_days = n_days // 2 if n_days % 2 == 0 else (n_days + 1) // 2
            else:
                selected_days = 1
        
        # Define base columns - CHANGED: Cost Per Purchase to CPP, Amount Spent (Zero Net Profit %) to BE
        base_columns = ["Product Name", "Campaign Name", "Total Amount Spent (USD)", "Total Purchases", "CPP", "BE"]
        
        # Define metrics that will be repeated for each date (13 metrics = 13 columns per date)
        date_metrics = ["Delivery status","Avg Price", "Delivery Rate", "Product Cost Input", "Amount Spent (USD)", "Purchases", "Cost Per Purchase (USD)", 
                       "Delivered Orders", "Net Revenue", "Total Product Cost", "Total Shipping Cost", 
                       "Total Operational Cost", "Net Profit", "Net Profit (%)"]
        
        # Build column structure WITH SEPARATOR COLUMNS
        all_columns = base_columns.copy()
        all_columns.append("SEPARATOR_AFTER_BASE")
        
        # Add date-specific columns with separators
        for date in unique_dates:
            for metric in date_metrics:
                all_columns.append(f"{date}_{metric}")
            all_columns.append(f"SEPARATOR_AFTER_{date}")
        
        # Add total columns
        for metric in date_metrics:
            all_columns.append(f"Total_{metric}")
        
        # Add remark column at the end
        all_columns.append("Remark")

        # FIRST: Identify matched and unmatched campaigns BEFORE processing main sheet
        matched_campaigns = []
        unmatched_campaigns = []
        
        # Check each campaign for Shopify data availability
        for product, product_df in df.groupby("Product"):
            # Check if this product has Shopify data (day-wise lookups)
            has_shopify_data = (
                  (product in product_date_avg_prices and product_date_avg_prices[product] > 0) or
                  (product in product_date_delivery_rates and product_date_delivery_rates[product] > 0) or
                  (product in product_date_cost_inputs and product_date_cost_inputs[product] > 0)
             )
            
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_amount_spent_inr = campaign_group.get("Amount Spent (INR)", 0).sum() if "Amount Spent (INR)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                campaign_info = {
                    'Product': str(product) if pd.notna(product) else '',
                    'Campaign Name': str(campaign_name) if pd.notna(campaign_name) else '',
                    'Amount Spent (USD)': round(float(total_amount_spent_usd), 2) if pd.notna(total_amount_spent_usd) else 0.0,
                    'Amount Spent (INR)': round(float(total_amount_spent_inr), 2) if pd.notna(total_amount_spent_inr) else 0.0,
                    'Purchases': int(total_purchases) if pd.notna(total_purchases) else 0,
                    'Has Shopify Data': has_shopify_data,
                    'Dates': sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d)])
                }
                
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    unmatched_campaigns.append(campaign_info)
        
        # FILTER: Create a filtered DataFrame that ONLY contains matched campaigns
        unmatched_campaign_keys = set()
        for campaign in unmatched_campaigns:
            unmatched_campaign_keys.add((campaign['Product'], campaign['Campaign Name']))
        
        # Filter the main DataFrame to exclude unmatched campaigns
        filtered_df_rows = []
        for _, row in df.iterrows():
            campaign_key = (str(row['Product']) if pd.notna(row['Product']) else '', 
                           str(row['Campaign Name']) if pd.notna(row['Campaign Name']) else '')
            if campaign_key not in unmatched_campaign_keys:
                filtered_df_rows.append(row)
        
        # Create filtered DataFrame with only matched campaigns
        if filtered_df_rows:
            filtered_df = pd.DataFrame(filtered_df_rows)
        else:
            # If no matched campaigns, create empty DataFrame with same structure
            filtered_df = df.iloc[0:0].copy()
        
        # Use filtered_df for all main sheet calculations
        df_main = filtered_df

        # NEW: Check for products with zero product cost input and zero delivery rate
        excluded_products = []
        valid_products = []
        
        for product, product_df in df_main.groupby("Product"):
            # Check if product has zero cost input and zero delivery rate across all dates
            has_valid_cost = False
            has_valid_delivery_rate = False
            
            for date in unique_dates:
                date_cost = safe_lookup_get(product_date_cost_inputs, product, 0.0)
                date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
                
                if date_cost > 0:
                    has_valid_cost = True
                if date_delivery_rate > 0:
                    has_valid_delivery_rate = True
            
            # If both cost input and delivery rate are zero, exclude from main calculations
            if not has_valid_cost and not has_valid_delivery_rate:
                total_amount_spent = product_df["Amount Spent (USD)"].sum()
                total_purchases = product_df["Purchases"].sum()
                campaign_count = len(product_df.groupby("Campaign Name"))
                
                excluded_products.append({
                    'Product': str(product),
                    'Campaign Count': campaign_count,
                    'Total Amount Spent (USD)': round(total_amount_spent, 2),
                    'Total Purchases': int(total_purchases),
                    'Reason': 'Product cost input = 0 and delivery rate = 0'
                })
            else:
                valid_products.append((product, product_df))

        # Write headers (skip separator columns)
        for col_num, col_name in enumerate(all_columns):
            if col_name.startswith("SEPARATOR_"):
                continue
            elif col_name.startswith("Total_"):
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), total_header_format)
            elif "_" in col_name and col_name.split("_")[0] in unique_dates:
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), date_header_format)
            else:
                safe_write(worksheet, 0, col_num, col_name, header_format)

        # SET UP COLUMN GROUPING
        start_col = 7  # Column H (after base columns A, B, C, D, E, F + separator G)
        total_columns = len(all_columns)
        
        group_level = 1
        while start_col < total_columns:
            if start_col < len(all_columns) and all_columns[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
                
            data_cols_found = 0
            end_col = start_col
            while end_col < total_columns and data_cols_found < 14:
                if not all_columns[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 14:
                    end_col += 1
            
            if end_col < total_columns:
                worksheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden':True}
                )
            
            start_col = end_col + 1
        
        # Set base column widths
        worksheet.set_column(0, 0, 25)  # Product Name
        worksheet.set_column(1, 1, 30)  # Campaign Name
        worksheet.set_column(2, 2, 20)  # Total Amount Spent (USD)
        worksheet.set_column(3, 3, 15)  # Total Purchases
        worksheet.set_column(4, 4, 18)  # CPP
        worksheet.set_column(5, 5, 25)  # BE
        worksheet.set_column(6, 6, 3)   # Separator column
        # Set width for remark column
        remark_col_idx = all_columns.index("Remark")
        worksheet.set_column(remark_col_idx, remark_col_idx, 30)  # Remark column

        # Configure outline settings
        worksheet.outline_settings(
            symbols_below=True,
            symbols_right=True,
            auto_style=False
        )

        # Grand total row
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "ALL VALID PRODUCTS", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "GRAND TOTAL", grand_total_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # NEW: Pre-calculate product-level delivery rates AND average prices for Total columns (ONLY FOR VALID PRODUCTS)
        product_total_delivery_rates = {}
        product_total_avg_prices = {}
        
        # STORE PRODUCT BE VALUES - This will be populated after main sheet calculation
        product_be_values = {}
        
        # STORE PRODUCT NET PROFIT VALUES - for Profit and Loss Products sheet
        product_net_profit_values = {}
        
        # NEW: STORE PRODUCT COST INPUT VALUES - for Profit and Loss Products sheet
        product_cost_input_values = {}
        
        # CHANGED: Calculate total purchases per product for sorting AND pre-calculate other values (ONLY FOR VALID PRODUCTS)
        product_purchase_totals = []
        for product, product_df in valid_products:
            total_purchases = product_df.get("Purchases", 0).sum() if "Purchases" in product_df.columns else 0
            
            # Calculate weighted average delivery rate for this product across all dates
            total_purchases_delivery = 0
            weighted_delivery_rate_sum = 0
            
            # Calculate weighted average price for this product across all dates
            total_purchases_price = 0
            weighted_avg_price_sum = 0
            
            for date in unique_dates:
                date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
                date_avg_price = safe_lookup_get(product_date_avg_prices, product, 0.0)
                date_purchases = product_df[product_df['Date'].astype(str) == date]['Purchases'].sum() if 'Purchases' in product_df.columns else 0
                
                # For delivery rate calculation
                total_purchases_delivery += date_purchases
                weighted_delivery_rate_sum += date_delivery_rate * date_purchases
                
                # For average price calculation
                total_purchases_price += date_purchases
                weighted_avg_price_sum += date_avg_price * date_purchases
            
            # Calculate weighted average delivery rate for this product
            if total_purchases_delivery > 0:
                product_total_delivery_rates[product] = weighted_delivery_rate_sum / total_purchases_delivery
            else:
                product_total_delivery_rates[product] = 0
            
            # Calculate weighted average price for this product
            if total_purchases_price > 0:
                product_total_avg_prices[product] = weighted_avg_price_sum / total_purchases_price
            else:
                product_total_avg_prices[product] = 0
            
            # Store for sorting
            product_purchase_totals.append((product, product_df, total_purchases))

        # CHANGED: Sort products by total purchases in descending order (highest purchases first)
        product_purchase_totals.sort(key=lambda x: x[2], reverse=True)

        # CHANGED: Group by product and restructure data - SORT BY TOTAL PURCHASES DESCENDING (ONLY VALID PRODUCTS)
        for product, product_df, total_purchases_for_product in product_purchase_totals:
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            safe_write(worksheet, product_total_row_idx, 1, "ALL CAMPAIGNS (TOTAL)", product_total_format)
            
            # Leave base columns empty for product total (will be calculated via formulas)
            safe_write(worksheet, product_total_row_idx, 2, "", product_total_format)
            safe_write(worksheet, product_total_row_idx, 3, "", product_total_format)
            safe_write(worksheet, product_total_row_idx, 4, "", product_total_format)
            safe_write(worksheet, product_total_row_idx, 5, "", product_total_format)
            
            # Check if product has total amount spent USD = 0 for remark
            product_total_amount_spent = product_df.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in product_df.columns else 0
            if product_total_amount_spent == 0:
                safe_write(worksheet, product_total_row_idx, all_columns.index("Remark"), "Total Amount Spent USD = 0", product_total_format)
            else:
                safe_write(worksheet, product_total_row_idx, all_columns.index("Remark"), "", product_total_format)

            # Group campaigns within product and calculate CPP for sorting
            campaign_groups = []
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # MODIFIED CPP CALCULATION: Use 1 for purchases if amount > 0 and purchases = 0
                cpp = 0
                if total_amount_spent_usd > 0 and total_purchases == 0:
                    cpp = total_amount_spent_usd / 1  # Use 1 for formula purposes
                elif total_purchases > 0:
                    cpp = total_amount_spent_usd / total_purchases
                
                campaign_groups.append((cpp, campaign_name, campaign_group))
            
            # Sort campaigns by CPP in ascending order
            campaign_groups.sort(key=lambda x: x[0])
            
            campaign_rows = []
            row += 1
            
            for cpp, campaign_name, campaign_group in campaign_groups:
                campaign_row_idx = row
                campaign_rows.append(campaign_row_idx)
                
                # Fill base columns for campaign
                safe_write(worksheet, campaign_row_idx, 0, product, campaign_format)
                safe_write(worksheet, campaign_row_idx, 1, campaign_name, campaign_format)
                # Leave base columns empty for campaigns (will be calculated via formulas)
                safe_write(worksheet, campaign_row_idx, 2, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 3, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 4, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 5, "", campaign_format)  # BE will reference product total
                
                # Add remark for campaigns with total amount spent USD = 0
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                if total_amount_spent_usd == 0:
                    safe_write(worksheet, campaign_row_idx, all_columns.index("Remark"), "Total Amount Spent USD = 0", campaign_format)
                else:
                    safe_write(worksheet, campaign_row_idx, all_columns.index("Remark"), "", campaign_format)
                
                # Cell references for Excel formulas
                excel_row = campaign_row_idx + 1
                
                # Fill date-specific data and formulas
                for date in unique_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date
                    avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    product_cost_input_col_idx = all_columns.index(f"{date}_Product Cost Input")
                    amount_spent_col_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                    purchases_col_idx = all_columns.index(f"{date}_Purchases")
                    cost_per_purchase_col_idx = all_columns.index(f"{date}_Cost Per Purchase (USD)")
                    delivered_orders_col_idx = all_columns.index(f"{date}_Delivered Orders")
                    net_revenue_col_idx = all_columns.index(f"{date}_Net Revenue")
                    total_product_cost_col_idx = all_columns.index(f"{date}_Total Product Cost")
                    total_shipping_cost_col_idx = all_columns.index(f"{date}_Total Shipping Cost")
                    total_operational_cost_col_idx = all_columns.index(f"{date}_Total Operational Cost")
                    net_profit_col_idx = all_columns.index(f"{date}_Net Profit")
                    net_profit_percent_col_idx = all_columns.index(f"{date}_Net Profit (%)")
                    delivery_status_col_idx = all_columns.index(f"{date}_Delivery status")
                    # Cell references for this date
                    avg_price_ref = f"{xl_col_to_name(avg_price_col_idx)}{excel_row}"
                    delivery_rate_ref = f"{xl_col_to_name(delivery_rate_col_idx)}{excel_row}"
                    product_cost_input_ref = f"{xl_col_to_name(product_cost_input_col_idx)}{excel_row}"
                    amount_spent_ref = f"{xl_col_to_name(amount_spent_col_idx)}{excel_row}"
                    purchases_ref = f"{xl_col_to_name(purchases_col_idx)}{excel_row}"
                    delivered_orders_ref = f"{xl_col_to_name(delivered_orders_col_idx)}{excel_row}"
                    net_revenue_ref = f"{xl_col_to_name(net_revenue_col_idx)}{excel_row}"
                    total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{excel_row}"
                    total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{excel_row}"
                    total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{excel_row}"
                    net_profit_ref = f"{xl_col_to_name(net_profit_col_idx)}{excel_row}"
                    
                    # VALUES FROM DAY-WISE LOOKUPS - Apply to ALL campaigns of this product for this date
                    
                    # Average Price - from day-wise lookup for this product and date
                    date_avg_price = safe_lookup_get(product_date_avg_prices, product, 0.0)
                    safe_write(worksheet, campaign_row_idx, avg_price_col_idx, round(float(date_avg_price), 2), campaign_format)
                    
                    # Delivery Rate - from day-wise lookup for this product and date
                    date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
                    safe_write(worksheet, campaign_row_idx, delivery_rate_col_idx, round(float(date_delivery_rate), 2), campaign_format)
                    
                    # Product Cost Input - from day-wise lookup for this product and date
                    date_cost_input = safe_lookup_get(product_date_cost_inputs, product, 0.0)
                    safe_write(worksheet, campaign_row_idx, product_cost_input_col_idx, round(float(date_cost_input), 2), campaign_format)
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Amount Spent (USD) - from campaign data
                        amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, round(float(amount_spent), 2), campaign_format)
                        
                        # Purchases - from campaign data  
                        purchases = row_data.get("Purchases", 0) or 0
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, purchases, campaign_format)
                        
                        # Delivery status - from campaign data
                        delivery_status_raw = row_data.get("Delivery status", "")
                        if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                                delivery_status_normalized = str(delivery_status_raw).strip().lower()
        # Consider "recently completed" and "inactive" as the same (Inactive)
                                if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                     delivery_status = "Active"
                                else:
                                     delivery_status = "Inactive"
                        else:
                                delivery_status = ""
                        safe_write(worksheet, campaign_row_idx, delivery_status_col_idx, delivery_status, campaign_format)
                        
                    else:
                        # No data for this date
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, 0, campaign_format)
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, 0, campaign_format)
                        safe_write(worksheet, campaign_row_idx, delivery_status_col_idx, "", campaign_format)  # ADD THIS LINE
                    
                    # FORMULAS for calculated fields
                    
                    # MODIFIED Cost Per Purchase (USD) formula: Use MAX(Purchases, 1) when Amount > 0
                    worksheet.write_formula(
                        campaign_row_idx, cost_per_purchase_col_idx,
                        f"=ROUND(IF({amount_spent_ref}>0,{amount_spent_ref}/MAX({purchases_ref},1),IF({purchases_ref}=0,0,{amount_spent_ref}/{purchases_ref})),2)",
                        campaign_format
                    )
                    
                    # Delivered Orders = Purchases * Delivery Rate
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    worksheet.write_formula(
                        campaign_row_idx, delivered_orders_col_idx,
                        f"=ROUND({purchases_ref}*{rate_term},2)",
                        campaign_format
                    )
                    
                    # Net Revenue = Delivered Orders * Average Price
                    worksheet.write_formula(
                        campaign_row_idx, net_revenue_col_idx,
                        f"=ROUND({delivered_orders_ref}*{avg_price_ref},2)",
                        campaign_format
                    )
                    
                    # Total Product Cost = Delivered Orders * Product Cost Input
                    worksheet.write_formula(
                        campaign_row_idx, total_product_cost_col_idx,
                        f"=ROUND({delivered_orders_ref}*{product_cost_input_ref},2)",
                        campaign_format
                    )
                    
                    # Total Shipping Cost = Purchases * shipping_rate
                    worksheet.write_formula(
                        campaign_row_idx, total_shipping_cost_col_idx,
                        f"=ROUND({purchases_ref}*{shipping_rate},2)",
                        campaign_format
                    )
                    
                    # Total Operational Cost = Purchases * operational_rate
                    worksheet.write_formula(
                        campaign_row_idx, total_operational_cost_col_idx,
                        f"=ROUND({purchases_ref}*{operational_rate},2)",
                        campaign_format
                    )
                    
                    # Net Profit = Net Revenue - Amount Spent (USD)*100 - Total Shipping Cost - Total Operational Cost - Total Product Cost
                    worksheet.write_formula(
                        campaign_row_idx, net_profit_col_idx,
                        f"=ROUND({net_revenue_ref}-{amount_spent_ref}*100-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref},2)",
                        campaign_format
                    )
                    
                    # MODIFIED Net Profit (%) = Net Profit / (Avg Price * Delivery Rate * Purchases) * 100
                    # Use MAX(Purchases, 1) when Amount Spent > 0
                    rate_term_for_profit = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    denominator_formula = f"({avg_price_ref}*{rate_term_for_profit}*IF({amount_spent_ref}>0,MAX({purchases_ref},1),{purchases_ref}))"
                    worksheet.write_formula(
                        campaign_row_idx, net_profit_percent_col_idx,
                        f"=ROUND(IF({denominator_formula}=0,0,{net_profit_ref}/{denominator_formula}*100),2)",
                        campaign_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR CAMPAIGN (FIXED: Use product-level delivery rate AND average price)
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Avg Price":
                        # FIXED: Use the pre-calculated product-level average price for ALL campaigns of this product
                        product_avg_price = product_total_avg_prices.get(product, 0)
                        safe_write(worksheet, campaign_row_idx, total_col_idx, round(float(product_avg_price), 2), campaign_format)
                    
                    elif metric == "Delivery Rate":
                        # FIXED: Use the pre-calculated product-level delivery rate for ALL campaigns of this product
                        product_delivery_rate = product_total_delivery_rates.get(product, 0)
                        safe_write(worksheet, campaign_row_idx, total_col_idx, round(float(product_delivery_rate), 2), campaign_format)
                    
                    elif metric == "Product Cost Input":
                        # WEIGHTED AVERAGE
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            metric_terms = []
                            for date in unique_dates:
                                metric_col_idx = all_columns.index(f"{date}_{metric}")
                                purchases_col_idx = all_columns.index(f"{date}_Purchases")
                                metric_terms.append(f"{xl_col_to_name(metric_col_idx)}{excel_row}*{xl_col_to_name(purchases_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(metric_terms)
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND(IF({total_purchases_ref}=0,0,({sumproduct_formula})/{total_purchases_ref}),2)",
                                campaign_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                campaign_format
                            )
                    
                    elif metric == "Cost Per Purchase (USD)":
                        # MODIFIED CALCULATED: Total Amount Spent / MAX(Total Purchases, 1) when Amount > 0
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=ROUND(IF({total_amount_spent_ref}>0,{total_amount_spent_ref}/MAX({total_purchases_ref},1),IF({total_purchases_ref}=0,0,{total_amount_spent_ref}/{total_purchases_ref})),2)",
                            campaign_format
                        )
                    
                    elif metric == "Net Profit (%)":
                        # MODIFIED CALCULATED: Net Profit / (Avg Price * Delivery Rate * Purchases) * 100
                        # Use MAX(Purchases, 1) when Amount Spent > 0
                        total_net_profit_col_idx = all_columns.index("Total_Net Profit")
                        total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                        total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_net_profit_ref = f"{xl_col_to_name(total_net_profit_col_idx)}{excel_row}"
                        total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                        total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        rate_term_total = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                        denominator_formula_total = f"({total_avg_price_ref}*{rate_term_total}*IF({total_amount_spent_ref}>0,MAX({total_purchases_ref},1),{total_purchases_ref}))"
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=ROUND(IF({denominator_formula_total}=0,0,{total_net_profit_ref}/{denominator_formula_total}*100),2)",
                            campaign_format
                        )
                    
                    else:
                        # SUM: All other metrics
                        if len(unique_dates) > 1:
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND({sum_formula},2)",
                                campaign_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                campaign_format
                            )
                
                # Calculate base columns for campaign (link to total columns)
                total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                total_purchases_col_idx = all_columns.index("Total_Purchases")
                total_cost_per_purchase_col_idx = all_columns.index("Total_Cost Per Purchase (USD)")
                
                worksheet.write_formula(
                    campaign_row_idx, 2,
                    f"={xl_col_to_name(total_amount_spent_col_idx)}{excel_row}",
                    campaign_format
                )
                
                worksheet.write_formula(
                    campaign_row_idx, 3,
                    f"={xl_col_to_name(total_purchases_col_idx)}{excel_row}",
                    campaign_format
                )
                
                # CPP (Cost Per Purchase) - link to total cost per purchase column
                worksheet.write_formula(
                    campaign_row_idx, 4,
                    f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{excel_row}",
                    campaign_format
                )
                
                # BE - CHANGED: Reference the product total BE value instead of calculating individually
                # This will be filled after product total BE is calculated
                
                row += 1
            
            # Calculate product totals by aggregating campaign rows using RANGES
            if campaign_rows:
                first_campaign_row = min(campaign_rows) + 1
                last_campaign_row = max(campaign_rows) + 1
                
                # Calculate base columns for product total (link to total columns)
                total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                total_purchases_col_idx = all_columns.index("Total_Purchases")
                total_cost_per_purchase_col_idx = all_columns.index("Total_Cost Per Purchase (USD)")
                
                worksheet.write_formula(
                    product_total_row_idx, 2,
                    f"={xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, 3,
                    f"={xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                # CPP for product total
                worksheet.write_formula(
                    product_total_row_idx, 4,
                    f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                # BE (Amount Spent Zero Net Profit % per purchases) for product total - FIXED: Use Total_Purchases (correct)
                total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
                total_shipping_cost_col_idx = all_columns.index("Total_Total Shipping Cost")
                total_operational_cost_col_idx = all_columns.index("Total_Total Operational Cost")
                total_product_cost_col_idx = all_columns.index("Total_Total Product Cost")
                total_purchases_col_idx = all_columns.index("Total_Purchases")  # FIXED: Use purchases (correct)
                
                total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{product_total_row_idx+1}"
                total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{product_total_row_idx+1}"
                total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{product_total_row_idx+1}"
                total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1}"
                total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}"  # FIXED: Use purchases
                
                zero_net_profit_formula = f'''=ROUND(IF(AND({total_net_revenue_ref}>0,{total_purchases_ref}>0),
                    ({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_purchases_ref},0),2)'''
                
                worksheet.write_formula(
                    product_total_row_idx, 5,
                    zero_net_profit_formula,
                    product_total_format
                )
                
                # STORE THE BE VALUE FOR LOOKUP - Calculate using PURCHASES (FIXED)
                total_net_revenue = 0
                total_shipping_cost = 0
                total_operational_cost = 0
                total_product_cost = 0
                total_purchases = 0  # FIXED: Use purchases instead of delivered orders
                
                for date in unique_dates:
                    date_data = product_df[product_df['Date'].astype(str) == date]
                    if not date_data.empty:
                        date_purchases = date_data['Purchases'].sum() if 'Purchases' in date_data.columns else 0
                        
                        # Get day-wise lookup data
                        date_avg_price = safe_lookup_get(product_date_avg_prices, product, 0.0)
                        date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
                        date_product_cost = safe_lookup_get(product_date_cost_inputs, product, 0.0)
                        
                        # Calculate components
                        delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                        delivered_orders = date_purchases * delivery_rate
                        net_revenue = delivered_orders * date_avg_price
                        product_cost = delivered_orders * date_product_cost
                        shipping_cost = date_purchases * shipping_rate
                        operational_cost = date_purchases * operational_rate
                        
                        total_purchases += date_purchases  # FIXED: Sum purchases, not delivered orders
                        total_net_revenue += net_revenue
                        total_shipping_cost += shipping_cost
                        total_operational_cost += operational_cost
                        total_product_cost += product_cost
                
                # Calculate BE for this product using PURCHASES (FIXED)
                be = 0
                if total_net_revenue > 0 and total_purchases > 0:
                    be = (total_net_revenue - total_shipping_cost - total_operational_cost - total_product_cost) / 100 / total_purchases
                
                product_be_values[product] = round(be, 2)
                
                # NEW: Calculate and store Net Profit for this product (for Profit and Loss Products sheet)
                total_net_profit_for_product = 0                
                for date in unique_dates:                    
                    date_net_profit_for_date = 0
                    date_data = product_df[product_df['Date'].astype(str) == date]                    
                    for _, campaign_row in date_data.iterrows():
                        date_purchases = round(campaign_row.get('Purchases', 0) if pd.notna(campaign_row.get('Purchases')) else 0, 2)
                        date_amount_spent = round(campaign_row.get("Amount Spent (USD)", 0) if pd.notna(campaign_row.get("Amount Spent (USD)")) else 0, 2)
                        
                        # Get day-wise lookup data
                        date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                        date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                        date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
        
                         # Calculate with rounding at each step (matching Excel exactly)
                        delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                        delivered_orders = round(date_purchases * delivery_rate, 2)
                        net_revenue = round(delivered_orders * date_avg_price, 2)
                        product_cost = round(delivered_orders * date_product_cost, 2)
                        shipping_cost = round(date_purchases * shipping_rate, 2)
                        operational_cost = round(date_purchases * operational_rate, 2)
        
        # Net Profit for this campaign on this date
                        campaign_date_net_profit = round(net_revenue - (date_amount_spent * 100) - shipping_cost - operational_cost - product_cost, 2)
                        date_net_profit_for_date += campaign_date_net_profit
    
    # Round the sum for this date (matching Excel's date-column rounding)
                    date_net_profit_for_date = round(date_net_profit_for_date, 2)
                    total_net_profit_for_product += date_net_profit_for_date
                
                product_net_profit_values[product] = round(total_net_profit_for_product, 2)
                # NEW: Calculate and store Total Product Cost Input for this product (for Profit and Loss Products sheet)
                # This should be the weighted average of product cost input across all dates for this product
                weighted_cost_input_sum = 0
                total_purchases_for_cost = 0
                
                for date in unique_dates:
                    date_data = product_df[product_df['Date'].astype(str) == date]
                    if not date_data.empty:
                        date_purchases = date_data['Purchases'].sum() if 'Purchases' in date_data.columns else 0
                        date_cost_input = safe_lookup_get(product_date_cost_inputs, product, 0.0)
                        
                        weighted_cost_input_sum += date_cost_input * date_purchases
                        total_purchases_for_cost += date_purchases
                
                # Calculate weighted average product cost input
                if total_purchases_for_cost > 0:
                    product_cost_input_avg = weighted_cost_input_sum / total_purchases_for_cost
                else:
                    product_cost_input_avg = 0
                
                product_cost_input_values[product] = round(product_cost_input_avg, 2)
                
                # AFTER calculating product BE, copy this value to ALL campaign rows under this product
                product_be_ref = f"F{product_total_row_idx+1}"  # F is column 5 (BE column)
                for campaign_row_idx in campaign_rows:
                    worksheet.write_formula(
                        campaign_row_idx, 5,
                        f"={product_be_ref}",
                        campaign_format
                    )
                
                # Continue with product total calculations...
                # [Rest of the product total calculations remain the same as your original code]
                
                # PRODUCT TOTAL CALCULATIONS (similar to existing logic but with day-wise data)
                for date in unique_dates:
                    for metric in date_metrics:
                        col_idx = all_columns.index(f"{date}_{metric}")
                        
                        if metric == "Avg Price":
                            # FIXED: Use the same product-level average price for product total row
                            date_avg_price = safe_lookup_get(product_date_avg_prices, product, 0.0)
                            safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_avg_price), 2), product_total_format)
                        elif metric == "Delivery Rate":
                            # FIXED: Use the same product-level delivery rate for product total row
                            date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
                            safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_delivery_rate), 2), product_total_format)
                        elif metric == "Product Cost Input":
                            # Weighted average based on purchases for this date using RANGES
                            date_purchases_col_idx = all_columns.index(f"{date}_Purchases")
                            
                            metric_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                            purchases_range = f"{xl_col_to_name(date_purchases_col_idx)}{first_campaign_row}:{xl_col_to_name(date_purchases_col_idx)}{last_campaign_row}"
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF(SUM({purchases_range})=0,0,SUMPRODUCT({metric_range},{purchases_range})/SUM({purchases_range})),2)",
                                product_total_format
                            )
                        elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                            # Calculate based on totals for this date
                            if metric == "Cost Per Purchase (USD)":
                                amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                                purchases_idx = all_columns.index(f"{date}_Purchases")
                                # MODIFIED CPP formula for product totals
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=ROUND(IF({xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}>0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}/MAX({xl_col_to_name(purchases_idx)}{product_total_row_idx+1},1),IF({xl_col_to_name(purchases_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}/{xl_col_to_name(purchases_idx)}{product_total_row_idx+1})),2)",
                                    product_total_format
                                )
                            else: # Net Profit (%)
                                net_profit_idx = all_columns.index(f"{date}_Net Profit")
                                avg_price_idx = all_columns.index(f"{date}_Avg Price")
                                delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                                amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                                # MODIFIED Net Profit (%) formula for product totals
                                rate_term_product = f"IF(ISNUMBER({xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}),IF({xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}>1,{xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}/100,{xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}),0)"
                                denominator_formula_product = f"({xl_col_to_name(avg_price_idx)}{product_total_row_idx+1}*{rate_term_product}*IF({xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}>0,MAX({xl_col_to_name(purchases_idx)}{product_total_row_idx+1},1),{xl_col_to_name(purchases_idx)}{product_total_row_idx+1}))"
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=ROUND(IF({denominator_formula_product}=0,0,{xl_col_to_name(net_profit_idx)}{product_total_row_idx+1}/{denominator_formula_product}*100),2)",
                                    product_total_format
                                )
                        else:
                            # Sum for other metrics using ranges
                            col_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=SUM({col_range})",
                                product_total_format
                            )
                
                # Calculate product totals for Total columns using RANGES (FIXED: Use product-level delivery rate AND average price)
                for metric in date_metrics:
                    col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Avg Price":
                        # FIXED: Use the pre-calculated product-level average price for product total
                        product_avg_price = product_total_avg_prices.get(product, 0)
                        safe_write(worksheet, product_total_row_idx, col_idx, round(float(product_avg_price), 2), product_total_format)
                    elif metric == "Delivery Rate":
                        # FIXED: Use the pre-calculated product-level delivery rate for product total
                        product_delivery_rate = product_total_delivery_rates.get(product, 0)
                        safe_write(worksheet, product_total_row_idx, col_idx, round(float(product_delivery_rate), 2), product_total_format)
                    elif metric == "Product Cost Input":
                        # Weighted average based on total purchases using RANGES
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        
                        metric_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                        purchases_range = f"{xl_col_to_name(total_purchases_col_idx)}{first_campaign_row}:{xl_col_to_name(total_purchases_col_idx)}{last_campaign_row}"
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(IF(SUM({purchases_range})=0,0,SUMPRODUCT({metric_range},{purchases_range})/SUM({purchases_range})),2)",
                            product_total_format
                        )
                    elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                        # Calculate based on totals
                        if metric == "Cost Per Purchase (USD)":
                            total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                            total_purchases_idx = all_columns.index("Total_Purchases")
                            # MODIFIED CPP formula for product total in Total columns
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF({xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}>0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}/MAX({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1},1),IF({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1})),2)",
                                product_total_format
                            )
                        else: # Net Profit (%)
                            total_net_profit_idx = all_columns.index("Total_Net Profit")
                            total_avg_price_idx = all_columns.index("Total_Avg Price")
                            total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                            total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                            # MODIFIED Net Profit (%) formula for product total in Total columns
                            rate_term_product_total = f"IF(ISNUMBER({xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}),IF({xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}>1,{xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}/100,{xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}),0)"
                            denominator_formula_product_total = f"({xl_col_to_name(total_avg_price_idx)}{product_total_row_idx+1}*{rate_term_product_total}*IF({xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}>0,MAX({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1},1),{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}))"
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF({denominator_formula_product_total}=0,0,{xl_col_to_name(total_net_profit_idx)}{product_total_row_idx+1}/{denominator_formula_product_total}*100),2)",
                                product_total_format
                            )
                    else:
                        # Sum for other metrics using ranges
                        col_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(SUM({col_range}),2)",
                            product_total_format
                        )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY (FIXED: Use weighted average for delivery rate AND average price) (ONLY VALID PRODUCTS)
        if product_total_rows:
            # Base columns for grand total
            total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            total_cost_per_purchase_col_idx = all_columns.index("Total_Cost Per Purchase (USD)")
            
            worksheet.write_formula(
                grand_total_row_idx, 2,
                f"={xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, 3,
                f"={xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            # CPP for grand total
            worksheet.write_formula(
                grand_total_row_idx, 4,
                f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            # BE (Amount Spent Zero Net Profit % per purchases) for grand total - FIXED: Use Purchases (ONLY VALID PRODUCTS)
            total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
            total_shipping_cost_col_idx = all_columns.index("Total_Total Shipping Cost")
            total_operational_cost_col_idx = all_columns.index("Total_Total Operational Cost")
            total_product_cost_col_idx = all_columns.index("Total_Total Product Cost")
            total_purchases_col_idx = all_columns.index("Total_Purchases")  # FIXED: Use purchases
            
            total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{grand_total_row_idx+1}"
            total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{grand_total_row_idx+1}"
            total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{grand_total_row_idx+1}"
            total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1}"
            total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}"  # FIXED: Use purchases
            
            zero_net_profit_formula = f'''=ROUND(IF(AND({total_net_revenue_ref}>0,{total_purchases_ref}>0),
                ({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_purchases_ref},0),2)'''
            
            worksheet.write_formula(
                grand_total_row_idx, 5,
                zero_net_profit_formula,
                grand_total_format
            )
            
            # Add remark for grand total if total amount spent USD = 0 (using filtered data for valid products only)
            grand_total_amount_spent = sum([product_df["Amount Spent (USD)"].sum() for _, product_df in valid_products])
            if grand_total_amount_spent == 0:
                safe_write(worksheet, grand_total_row_idx, all_columns.index("Remark"), "Total Amount Spent USD = 0", grand_total_format)
            else:
                safe_write(worksheet, grand_total_row_idx, all_columns.index("Remark"), "", grand_total_format)
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS (FIXED: Weighted average for delivery rate AND average price) (ONLY VALID PRODUCTS)
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                        # Weighted average using individual product total rows
                        date_purchases_col_idx = all_columns.index(f"{date}_Purchases")
                        
                        metric_refs = []
                        purchases_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                            purchases_refs.append(f"{xl_col_to_name(date_purchases_col_idx)}{product_excel_row}")
                        
                        # Build SUMPRODUCT formula for weighted average
                        sumproduct_terms = []
                        for i in range(len(metric_refs)):
                            sumproduct_terms.append(f"{metric_refs[i]}*{purchases_refs[i]}")
                        
                        sumproduct_formula = "+".join(sumproduct_terms)
                        sum_purchases_formula = "+".join(purchases_refs)
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula})),2)",
                            grand_total_format
                        )
                    elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                        # Calculate based on totals for this date
                        if metric == "Cost Per Purchase (USD)":
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            purchases_idx = all_columns.index(f"{date}_Purchases")
                            # MODIFIED CPP formula for grand total
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"=ROUND(IF({xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}>0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}/MAX({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1},1),IF({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}/{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1})),2)",
                                grand_total_format
                            )
                        else: # Net Profit (%)
                            net_profit_idx = all_columns.index(f"{date}_Net Profit")
                            avg_price_idx = all_columns.index(f"{date}_Avg Price")
                            delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            # MODIFIED CPP formula for grand total
                            rate_term_grand = f"IF(ISNUMBER({xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}),IF({xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}>1,{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}/100,{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}),0)"
                            denominator_formula_grand = f"({xl_col_to_name(avg_price_idx)}{grand_total_row_idx+1}*{rate_term_grand}*IF({xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}>0,MAX({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1},1),{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}))"
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"=ROUND(IF({denominator_formula_grand}=0,0,{xl_col_to_name(net_profit_idx)}{grand_total_row_idx+1}/{denominator_formula_grand}*100),2)",
                                grand_total_format
                            )
                    else:
                        # Sum using individual product total rows only
                        sum_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        
                        sum_formula = "+".join(sum_refs)
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"={sum_formula}",
                            grand_total_format
                        )
            
            # Total columns for grand total using INDIVIDUAL PRODUCT TOTAL ROWS (FIXED: Weighted average for delivery rate AND average price) (ONLY VALID PRODUCTS)
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                    # Weighted average using individual product total rows
                    metric_refs = []
                    purchases_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        purchases_refs.append(f"{xl_col_to_name(total_purchases_col_idx)}{product_excel_row}")
                    
                    # Build SUMPRODUCT formula for weighted average
                    sumproduct_terms = []
                    for i in range(len(metric_refs)):
                        sumproduct_terms.append(f"{metric_refs[i]}*{purchases_refs[i]}")
                    
                    sumproduct_formula = "+".join(sumproduct_terms)
                    sum_purchases_formula = "+".join(purchases_refs)
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=ROUND(IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula})),2)",
                        grand_total_format
                    )
                elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                    # Calculate based on totals
                    if metric == "Cost Per Purchase (USD)":
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        # MODIFIED CPP formula for grand total in Total columns
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF({xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}>0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}/MAX({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1},1),IF({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}/{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1})),2)",
                            grand_total_format
                        )
                    else: # Net Profit (%)
                        total_net_profit_idx = all_columns.index("Total_Net Profit")
                        total_avg_price_idx = all_columns.index("Total_Avg Price")
                        total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        # MODIFIED Net Profit (%) formula for grand total in Total columns
                        rate_term_grand_total = f"IF(ISNUMBER({xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}),IF({xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}>1,{xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}/100,{xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}),0)"
                        denominator_formula_grand_total = f"({xl_col_to_name(total_avg_price_idx)}{grand_total_row_idx+1}*{rate_term_grand_total}*IF({xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}>0,MAX({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1},1),{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}))"
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF({denominator_formula_grand_total}=0,0,{xl_col_to_name(total_net_profit_idx)}{grand_total_row_idx+1}/{denominator_formula_grand_total}*100),2)",
                            grand_total_format
                        )
                else:
                    # Sum using individual product total rows only
                    sum_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                    
                    sum_formula = "+".join(sum_refs)
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"={sum_formula}",
                        grand_total_format
                    )

        # NEW: Add excluded products table at the end of the sheet - RESTRUCTURED with campaigns
        # SPLIT INTO TWO TABLES: All Active vs Has Inactive
        if excluded_products:
            # Add some spacing
            exclusion_start_row = row + 3
            
            # Title for exclusion table
            safe_write(worksheet, exclusion_start_row, 0, "PRODUCTS EXCLUDED FROM CALCULATIONS", exclusion_header_format)
            safe_write(worksheet, exclusion_start_row + 1, 0, "These products have product cost input = 0 and delivery rate = 0", exclusion_data_format)
            
            current_exclusion_row = exclusion_start_row + 3
            
            # NEW FORMAT: Product-level header formats
            excluded_product_header_format = workbook.add_format({
                "bold": True, "align": "left", "valign": "vcenter",
                "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
            })
            excluded_campaign_format = workbook.add_format({
                "align": "left", "valign": "vcenter",
                "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
                "num_format": "#,##0.00"
            })
            
            # NEW: Active product header format (different color)
            active_product_header_format = workbook.add_format({
                "bold": True, "align": "left", "valign": "vcenter",
                "fg_color": "#90EE90", "font_name": "Calibri", "font_size": 11
            })
            active_campaign_format = workbook.add_format({
                "align": "left", "valign": "vcenter",
                "fg_color": "#E6FFE6", "font_name": "Calibri", "font_size": 11,
                "num_format": "#,##0.00"
            })
            
            # STEP 1: Categorize products based on last day delivery status
            all_active_products = []
            has_inactive_products = []
            
            for excluded_product_info in excluded_products:
                product_name = excluded_product_info['Product']
                product_df = df[df['Product'] == product_name]
                
                # Check all campaigns for this product
                all_campaigns_active = True
                
                for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                    # Get last day delivery status
                    campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() 
                                           if pd.notna(d) and str(d).strip() != ''])
                    
                    last_date = campaign_dates[-1] if campaign_dates else None
                    last_day_delivery_status = ""
                    
                    if last_date:
                        last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                        if not last_date_data.empty:
                            row_data = last_date_data.iloc[0]
                            delivery_status_raw = row_data.get("Delivery status", "")
                            if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                                delivery_status_normalized = str(delivery_status_raw).strip().lower()
                                if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                    last_day_delivery_status = "Active"
                                else:
                                    last_day_delivery_status = "Inactive"
                                    all_campaigns_active = False
                            else:
                                all_campaigns_active = False
                        else:
                            all_campaigns_active = False
                    else:
                        all_campaigns_active = False
                    
                    # If we found any non-active campaign, no need to check further
                    if not all_campaigns_active:
                        break
                
                # Categorize the product
                if all_campaigns_active:
                    all_active_products.append(excluded_product_info)
                else:
                    has_inactive_products.append(excluded_product_info)
            
            # STEP 2: TABLE 1 - Products with ALL campaigns active
            if all_active_products:
                safe_write(worksheet, current_exclusion_row, 0, 
                          "TABLE 1: PRODUCTS WITH ALL CAMPAIGNS ACTIVE (LAST DAY)", 
                          active_product_header_format)
                current_exclusion_row += 2
                
                for excluded_product_info in all_active_products:
                    product_name = excluded_product_info['Product']
                    product_df = df[df['Product'] == product_name]
                    
                    # PRODUCT HEADER ROW
                    safe_write(worksheet, current_exclusion_row, 0, product_name, active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 1, "ALL CAMPAIGNS", active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 2, "", active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 3, "", active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 4, "", active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 5, excluded_product_info['Reason'], 
                              active_product_header_format)
                    current_exclusion_row += 1
                    
                    # CAMPAIGN HEADERS
                    campaign_headers = ["Product Name", "Campaign Name", "Amount Spent (USD)", "Purchases", 
                                       "Last Day Delivery Status", "Reason"]
                    for col_num, header in enumerate(campaign_headers):
                        safe_write(worksheet, current_exclusion_row, col_num, header, exclusion_header_format)
                    current_exclusion_row += 1
                    
                    # Get all campaigns for this product
                    campaign_count = 0
                    product_total_amount_spent = 0
                    product_total_purchases = 0
                    
                    for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                        campaign_count += 1
                        
                        # Calculate campaign totals
                        total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() \
                            if "Amount Spent (USD)" in campaign_group.columns else 0
                        total_purchases = campaign_group.get("Purchases", 0).sum() \
                            if "Purchases" in campaign_group.columns else 0
                        
                        product_total_amount_spent += total_amount_spent_usd
                        product_total_purchases += total_purchases
                        
                        # Get last day delivery status
                        campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() 
                                               if pd.notna(d) and str(d).strip() != ''])
                        
                        last_date = campaign_dates[-1] if campaign_dates else None
                        last_day_delivery_status = ""
                        
                        if last_date:
                            last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                            if not last_date_data.empty:
                                row_data = last_date_data.iloc[0]
                                delivery_status_raw = row_data.get("Delivery status", "")
                                if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                                    delivery_status_normalized = str(delivery_status_raw).strip().lower()
                                    if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                        last_day_delivery_status = "Active"
                                    else:
                                        last_day_delivery_status = "Inactive"
                                else:
                                    last_day_delivery_status = "Unknown"
                            else:
                                last_day_delivery_status = "No Data"
                        else:
                            last_day_delivery_status = "No Dates"
                        
                        # Write campaign row
                        safe_write(worksheet, current_exclusion_row, 0, product_name, active_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 1, str(campaign_name), active_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 2, round(total_amount_spent_usd, 2), 
                                  active_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 3, int(total_purchases), 
                                  active_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 4, last_day_delivery_status, 
                                  active_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 5, 
                                  "Product cost input = 0 and delivery rate = 0", active_campaign_format)
                        current_exclusion_row += 1
                    
                    # PRODUCT SUMMARY ROW
                    safe_write(worksheet, current_exclusion_row, 0, f"{product_name} - SUMMARY", 
                              active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 1, f"Total Campaigns: {campaign_count}", 
                              active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 2, round(product_total_amount_spent, 2), 
                              active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 3, int(product_total_purchases), 
                              active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 4, "", active_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 5, "", active_product_header_format)
                    current_exclusion_row += 1
                    
                    # Add spacing between products
                    current_exclusion_row += 1
                
                # Summary for all active products
                current_exclusion_row += 1
                safe_write(worksheet, current_exclusion_row, 0, "SUMMARY - ALL ACTIVE PRODUCTS", 
                          exclusion_header_format)
                current_exclusion_row += 1
                
                total_active_products = len(all_active_products)
                total_active_campaigns = sum(p['Campaign Count'] for p in all_active_products)
                total_active_amount = sum(p['Total Amount Spent (USD)'] for p in all_active_products)
                total_active_purchases = sum(p['Total Purchases'] for p in all_active_products)
                
                safe_write(worksheet, current_exclusion_row, 0, 
                          f"Products with all campaigns active: {total_active_products}", 
                          exclusion_data_format)
                safe_write(worksheet, current_exclusion_row + 1, 0, 
                          f"Total campaigns: {total_active_campaigns}", exclusion_data_format)
                safe_write(worksheet, current_exclusion_row + 2, 0, 
                          f"Total amount spent: ${total_active_amount:,.2f}", exclusion_data_format)
                safe_write(worksheet, current_exclusion_row + 3, 0, 
                          f"Total purchases: {total_active_purchases:,}", exclusion_data_format)
                
                current_exclusion_row += 6
            
            # STEP 3: TABLE 2 - Products with at least one inactive campaign
            if has_inactive_products:
                safe_write(worksheet, current_exclusion_row, 0, 
                          "TABLE 2: PRODUCTS WITH AT LEAST ONE INACTIVE CAMPAIGN (LAST DAY)", 
                          excluded_product_header_format)
                current_exclusion_row += 2
                
                for excluded_product_info in has_inactive_products:
                    product_name = excluded_product_info['Product']
                    product_df = df[df['Product'] == product_name]
                    
                    # PRODUCT HEADER ROW
                    safe_write(worksheet, current_exclusion_row, 0, product_name, excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 1, "ALL CAMPAIGNS", excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 2, "", excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 3, "", excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 4, "", excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 5, excluded_product_info['Reason'], 
                              excluded_product_header_format)
                    current_exclusion_row += 1
                    
                    # CAMPAIGN HEADERS
                    campaign_headers = ["Product Name", "Campaign Name", "Amount Spent (USD)", "Purchases", 
                                       "Last Day Delivery Status", "Reason"]
                    for col_num, header in enumerate(campaign_headers):
                        safe_write(worksheet, current_exclusion_row, col_num, header, exclusion_header_format)
                    current_exclusion_row += 1
                    
                    # Get all campaigns for this product
                    campaign_count = 0
                    product_total_amount_spent = 0
                    product_total_purchases = 0
                    
                    for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                        campaign_count += 1
                        
                        # Calculate campaign totals
                        total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() \
                            if "Amount Spent (USD)" in campaign_group.columns else 0
                        total_purchases = campaign_group.get("Purchases", 0).sum() \
                            if "Purchases" in campaign_group.columns else 0
                        
                        product_total_amount_spent += total_amount_spent_usd
                        product_total_purchases += total_purchases
                        
                        # Get last day delivery status
                        campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() 
                                               if pd.notna(d) and str(d).strip() != ''])
                        
                        last_date = campaign_dates[-1] if campaign_dates else None
                        last_day_delivery_status = ""
                        
                        if last_date:
                            last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                            if not last_date_data.empty:
                                row_data = last_date_data.iloc[0]
                                delivery_status_raw = row_data.get("Delivery status", "")
                                if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                                    delivery_status_normalized = str(delivery_status_raw).strip().lower()
                                    if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                        last_day_delivery_status = "Active"
                                    else:
                                        last_day_delivery_status = "Inactive"
                                else:
                                    last_day_delivery_status = "Unknown"
                            else:
                                last_day_delivery_status = "No Data"
                        else:
                            last_day_delivery_status = "No Dates"
                        
                        # Write campaign row
                        safe_write(worksheet, current_exclusion_row, 0, product_name, excluded_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 1, str(campaign_name), excluded_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 2, round(total_amount_spent_usd, 2), 
                                  excluded_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 3, int(total_purchases), 
                                  excluded_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 4, last_day_delivery_status, 
                                  excluded_campaign_format)
                        safe_write(worksheet, current_exclusion_row, 5, 
                                  "Product cost input = 0 and delivery rate = 0", excluded_campaign_format)
                        current_exclusion_row += 1
                    
                    # PRODUCT SUMMARY ROW
                    safe_write(worksheet, current_exclusion_row, 0, f"{product_name} - SUMMARY", 
                              excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 1, f"Total Campaigns: {campaign_count}", 
                              excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 2, round(product_total_amount_spent, 2), 
                              excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 3, int(product_total_purchases), 
                              excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 4, "", excluded_product_header_format)
                    safe_write(worksheet, current_exclusion_row, 5, "", excluded_product_header_format)
                    current_exclusion_row += 1
                    
                    # Add spacing between products
                    current_exclusion_row += 1
                
                # Summary for products with inactive campaigns
                current_exclusion_row += 1
                safe_write(worksheet, current_exclusion_row, 0, "SUMMARY - PRODUCTS WITH INACTIVE CAMPAIGNS", 
                          exclusion_header_format)
                current_exclusion_row += 1
                
                total_inactive_products = len(has_inactive_products)
                total_inactive_campaigns = sum(p['Campaign Count'] for p in has_inactive_products)
                total_inactive_amount = sum(p['Total Amount Spent (USD)'] for p in has_inactive_products)
                total_inactive_purchases = sum(p['Total Purchases'] for p in has_inactive_products)
                
                safe_write(worksheet, current_exclusion_row, 0, 
                          f"Products with at least one inactive campaign: {total_inactive_products}", 
                          exclusion_data_format)
                safe_write(worksheet, current_exclusion_row + 1, 0, 
                          f"Total campaigns: {total_inactive_campaigns}", exclusion_data_format)
                safe_write(worksheet, current_exclusion_row + 2, 0, 
                          f"Total amount spent: ${total_inactive_amount:,.2f}", exclusion_data_format)
                safe_write(worksheet, current_exclusion_row + 3, 0, 
                          f"Total purchases: {total_inactive_purchases:,}", exclusion_data_format)
                
                current_exclusion_row += 6
            
            # OVERALL EXCLUSION SUMMARY
            safe_write(worksheet, current_exclusion_row, 0, "OVERALL EXCLUSION SUMMARY", 
                      exclusion_header_format)
            current_exclusion_row += 1
            
            total_excluded_amount = sum(p['Total Amount Spent (USD)'] for p in excluded_products)
            total_excluded_purchases = sum(p['Total Purchases'] for p in excluded_products)
            total_excluded_campaigns = sum(p['Campaign Count'] for p in excluded_products)
            
            safe_write(worksheet, current_exclusion_row, 0, f"Total excluded products: {len(excluded_products)}", 
                      exclusion_data_format)
            safe_write(worksheet, current_exclusion_row + 1, 0, 
                      f"  • All campaigns active: {len(all_active_products)}", exclusion_data_format)
            safe_write(worksheet, current_exclusion_row + 2, 0, 
                      f"  • Has inactive campaigns: {len(has_inactive_products)}", exclusion_data_format)
            safe_write(worksheet, current_exclusion_row + 3, 0, 
                      f"Total excluded campaigns: {total_excluded_campaigns}", exclusion_data_format)
            safe_write(worksheet, current_exclusion_row + 4, 0, 
                      f"Total excluded amount spent: ${total_excluded_amount:,.2f}", exclusion_data_format)
            safe_write(worksheet, current_exclusion_row + 5, 0, 
                      f"Total excluded purchases: {total_excluded_purchases:,}", exclusion_data_format)
            
            
            
        # Freeze panes to keep base columns visible when scrolling
        worksheet.freeze_panes(2, len(base_columns))
        
        # ==== NEW SHEET: Unmatched Campaigns ====
        unmatched_sheet = workbook.add_worksheet("Unmatched Campaigns")
        
        # Formats for unmatched sheet
        unmatched_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF9999", "font_name": "Calibri", "font_size": 11
        })
        unmatched_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })
        matched_summary_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E6FFE6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })
        
        # Headers for unmatched sheet
        unmatched_headers = ["Status", "Product", "Campaign Name", "Amount Spent (USD)", 
                           "Amount Spent (INR)", "Purchases", "Cost Per Purchase (USD)", "Last Day Delivery Status", "Dates Covered", "Reason"]
        
        for col_num, header in enumerate(unmatched_headers):
            safe_write(unmatched_sheet, 0, col_num, header, unmatched_header_format)
        
        # Write summary first
        summary_row = 1
        safe_write(unmatched_sheet, summary_row, 0, "SUMMARY", unmatched_header_format)
        safe_write(unmatched_sheet, summary_row + 1, 0, f"Total Campaigns: {len(matched_campaigns) + len(unmatched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 2, 0, f"Matched with Shopify: {len(matched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 3, 0, f"Unmatched with Shopify: {len(unmatched_campaigns)}", unmatched_data_format)
        safe_write(unmatched_sheet, summary_row + 4, 0, f"Date Range: {min(unique_dates)} to {max(unique_dates)}" if unique_dates else "No dates found", matched_summary_format)
        
        # Write unmatched campaigns
        # Write unmatched campaigns
        current_row = summary_row + 6
        
        if unmatched_campaigns:
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITHOUT SHOPIFY DATA", unmatched_header_format)
            current_row += 1
            
            for campaign in unmatched_campaigns:
                # MODIFIED CPP calculation for unmatched campaigns sheet
                cost_per_purchase_usd = 0
                if campaign['Amount Spent (USD)'] > 0 and campaign['Purchases'] == 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / 1, 2)  # Use 1 when no purchases but has spending
                elif campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                dates_str = ", ".join(campaign['Dates']) if campaign['Dates'] else "No dates"
                
                # Get last day delivery status
                product = campaign['Product']
                campaign_name = campaign['Campaign Name']
                product_df = df[df['Product'] == product]
                campaign_group = product_df[product_df['Campaign Name'] == campaign_name]
                
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() 
                                       if pd.notna(d) and str(d).strip() != ''])
                
                last_date = campaign_dates[-1] if campaign_dates else None
                last_day_delivery_status = ""
                
                if last_date:
                    last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                    if not last_date_data.empty:
                        row_data = last_date_data.iloc[0]
                        delivery_status_raw = row_data.get("Delivery status", "")
                        if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                            delivery_status_normalized = str(delivery_status_raw).strip().lower()
                            if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                last_day_delivery_status = "Active"
                            else:
                                last_day_delivery_status = "Inactive"
                        else:
                            last_day_delivery_status = "Unknown"
                    else:
                        last_day_delivery_status = "No Data"
                else:
                    last_day_delivery_status = "No Dates"
                
                safe_write(unmatched_sheet, current_row, 0, "UNMATCHED", unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Amount Spent (INR)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 5, campaign['Purchases'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 6, cost_per_purchase_usd, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 7, last_day_delivery_status, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 8, dates_str, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 9, "No matching Shopify day-wise data found", unmatched_data_format)
                current_row += 1
        
        # Write matched campaigns summary
        if matched_campaigns:
            current_row += 2
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITH SHOPIFY DATA (FOR REFERENCE)", unmatched_header_format)
            current_row += 1
            
            for campaign in matched_campaigns[:10]:  # Show only first 10 to save space
                # MODIFIED CPP calculation for matched campaigns sheet
                cost_per_purchase_usd = 0
                if campaign['Amount Spent (USD)'] > 0 and campaign['Purchases'] == 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / 1, 2)  # Use 1 when no purchases but has spending
                elif campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                dates_str = ", ".join(campaign['Dates']) if campaign['Dates'] else "No dates"
                
                safe_write(unmatched_sheet, current_row, 0, "MATCHED", matched_summary_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Amount Spent (INR)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 5, campaign['Purchases'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 6, cost_per_purchase_usd, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 7, dates_str, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 8, "Successfully matched with Shopify day-wise data", matched_summary_format)
                current_row += 1
            
            if len(matched_campaigns) > 10:
                safe_write(unmatched_sheet, current_row, 0, f"... and {len(matched_campaigns) - 10} more matched campaigns", matched_summary_format)
        
        # Set column widths for unmatched sheet
        unmatched_sheet.set_column(0, 0, 12)  # Status
        unmatched_sheet.set_column(1, 1, 25)  # Product
        unmatched_sheet.set_column(2, 2, 35)  # Campaign Name
        unmatched_sheet.set_column(3, 3, 18)  # Amount USD
        unmatched_sheet.set_column(4, 4, 18)  # Amount INR
        unmatched_sheet.set_column(5, 5, 12)  # Purchases
        unmatched_sheet.set_column(6, 6, 20)  # Cost Per Purchase USD
        unmatched_sheet.set_column(7, 7, 22)  # Last Day Delivery Status
        unmatched_sheet.set_column(8, 8, 25)  # Dates Covered
        unmatched_sheet.set_column(9, 9, 40)  # Reason
       

        # ==== SHEET: Negative Net Profit Campaigns ====
        negative_profit_sheet = workbook.add_worksheet("Negative Net Profit Campaigns")

        # Formats
        negative_profit_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
        })
        negative_profit_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        moderate_negative_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFA500", "font_name": "Calibri", "font_size": 11
        })
        moderate_negative_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE4B5", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        positive_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#ABEA53", "font_name": "Calibri", "font_size": 11
        })
        positive_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#F0FFF0", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        last_date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#559BD8", "font_name": "Calibri", "font_size": 11
        })
        last_date_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E6E6FA", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        # NEW: Format for complete analysis table
        analysis_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#4472C4", "font_name": "Calibri", "font_size": 11,
            "border": 1
        })
        analysis_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E2F3", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00",
            "border": 1
        })

        # Helper function to format dates
        def format_date_readable(date_str):
            """Convert date string to readable format like '9th September 2025'"""
            try:
                from datetime import datetime
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        day = date_obj.day
                        if 4 <= day <= 20 or 24 <= day <= 30:
                            suffix = "th"
                        else:
                            suffix = ["st", "nd", "rd"][day % 10 - 1]
                        return f"{day}{suffix} {date_obj.strftime('%B %Y')}"
                    except ValueError:
                        continue
                return date_str
            except:
                return date_str

        # ==== NEW: COMPLETE ANALYSIS TABLE FOR ALL VALID CAMPAIGNS ====
        current_row = 0
        
        # Title for complete analysis
        safe_write(negative_profit_sheet, current_row, 0, 
                  "COMPLETE CAMPAIGN ANALYSIS - ALL VALID PRODUCTS", 
                  analysis_header_format)
        current_row += 2
        
        # Build headers - Product, Campaign, Day-wise columns for each date, Total Net Profit %, CPP, BE
        analysis_headers = ["Product", "Campaign Name"]
        
        # Add separator after Campaign Name
        analysis_headers.append("SEPARATOR_AFTER_CAMPAIGN")
        
        # Add day-wise columns for each date (all metrics from Campaign Data sheet) WITH SEPARATORS
        for date in unique_dates:
            analysis_headers.extend([
                f"{date} Avg Price",
                f"{date} Delivery Rate", 
                f"{date} Product Cost Input",
                f"{date} Amount Spent (USD)",
                f"{date} Purchases",
                f"{date} Delivered Orders",
                f"{date} Net Revenue",
                f"{date} Total Product Cost",
                f"{date} Total Shipping Cost",
                f"{date} Total Operational Cost",
                f"{date} Net Profit",
                f"{date} Net Profit %"
            ])
            # Add separator after each date's columns
            analysis_headers.append(f"SEPARATOR_AFTER_{date}")
        
        # Add summary columns
        analysis_headers.extend(["Total Net Profit %", "CPP", "BE"])
        
        # Write headers (skip separator columns)
        col_num = 0
        for header in analysis_headers:
            if header.startswith("SEPARATOR_"):
                col_num += 1
                continue
            safe_write(negative_profit_sheet, current_row, col_num, header, analysis_header_format)
            col_num += 1
        current_row += 1
        
        # FIXED: Create a set of valid product names for filtering
        valid_product_names = set([product for product, _ in valid_products])
        
        # Collect all campaign analysis data - ONLY FROM VALID PRODUCTS
        all_campaigns_complete_analysis = []
        
        for product, product_df in df_main.groupby("Product"):
            # SKIP if product is not in valid_products list
            if product not in valid_product_names:
                continue
                
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                # Get campaign dates
                campaign_dates = []
                for date in sorted([str(d) for d in campaign_group['Date'].unique() 
                     if pd.notna(d) and str(d).strip() != '']):
                   date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                   if not date_data.empty:
                          date_amount_spent = date_data.iloc[0].get("Amount Spent (USD)", 0)
                          if pd.notna(date_amount_spent) and float(date_amount_spent) > 0:
                               campaign_dates.append(date)
                
                # Calculate totals
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() \
                    if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() \
                    if "Purchases" in campaign_group.columns else 0
                
                # Calculate CPP
                cpp = 0
                if total_amount_spent_usd > 0 and total_purchases == 0:
                    cpp = total_amount_spent_usd / 1
                elif total_purchases > 0:
                    cpp = total_amount_spent_usd / total_purchases
                
                # Get BE value
                be = product_be_values.get(product, 0)
                
                # Get product-level values for total calculation
                product_avg_price = round(product_total_avg_prices.get(product, 0), 2)
                product_delivery_rate = round(product_total_delivery_rates.get(product, 0), 2)
                
                # CALCULATE DAY-WISE ALL METRICS for ALL dates
                day_wise_metrics_dict = {}
                
                for date in unique_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Get day-specific data
                        date_amount_spent = round(row_data.get("Amount Spent (USD)", 0) 
                                                if pd.notna(row_data.get("Amount Spent (USD)")) else 0, 2)
                        date_purchases = round(row_data.get("Purchases", 0) 
                                             if pd.notna(row_data.get("Purchases")) else 0, 2)
                        
                        # Get day-wise lookup data
                        date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                        date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                        date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
                        
                        # Calculate all metrics for this day
                        if date_avg_price > 0 and (date_purchases > 0 or 
                            (date_purchases == 0 and date_amount_spent > 0)):
                            
                            # Use actual purchases for calculations
                            delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                            
                            # Calculate intermediate values
                            delivered_orders = round(date_purchases * delivery_rate, 2)
                            net_revenue = round(delivered_orders * date_avg_price, 2)
                            total_product_cost = round(delivered_orders * date_product_cost, 2)
                            total_shipping_cost = round(date_purchases * shipping_rate, 2)
                            total_operational_cost = round(date_purchases * operational_rate, 2)
                            net_profit = round(net_revenue - (date_amount_spent * 100) - 
                                             total_shipping_cost - total_operational_cost - total_product_cost, 2)
                            
                            # FIXED: For denominator, use MAX(purchases, 1) when amount_spent > 0
                            purchases_for_denominator = max(date_purchases, 1) if date_amount_spent > 0 else date_purchases
                            denominator = date_avg_price * delivery_rate * purchases_for_denominator
                            day_net_profit_pct = round((net_profit / denominator * 100), 2) if denominator > 0 else 0
                            
                            # Store all metrics for this date
                            day_wise_metrics_dict[date] = {
                                'avg_price': date_avg_price,
                                'delivery_rate': date_delivery_rate,
                                'product_cost_input': date_product_cost,
                                'amount_spent': date_amount_spent,
                                'purchases': date_purchases,
                                'delivered_orders': delivered_orders,
                                'net_revenue': net_revenue,
                                'total_product_cost': total_product_cost,
                                'total_shipping_cost': total_shipping_cost,
                                'total_operational_cost': total_operational_cost,
                                'net_profit': net_profit,
                                'net_profit_pct': day_net_profit_pct
                            }
                        else:
                            # No valid data for this date
                            day_wise_metrics_dict[date] = {
                                'avg_price': date_avg_price,
                                'delivery_rate': date_delivery_rate,
                                'product_cost_input': date_product_cost,
                                'amount_spent': date_amount_spent,
                                'purchases': date_purchases,
                                'delivered_orders': 0,
                                'net_revenue': 0,
                                'total_product_cost': 0,
                                'total_shipping_cost': 0,
                                'total_operational_cost': 0,
                                'net_profit': 0,
                                'net_profit_pct': 0
                            }
                    else:
                        # No data for this date
                        day_wise_metrics_dict[date] = {
                            'avg_price': 0,
                            'delivery_rate': 0,
                            'product_cost_input': 0,
                            'amount_spent': 0,
                            'purchases': 0,
                            'delivered_orders': 0,
                            'net_revenue': 0,
                            'total_product_cost': 0,
                            'total_shipping_cost': 0,
                            'total_operational_cost': 0,
                            'net_profit': 0,
                            'net_profit_pct': 0
                        }
                
                # CALCULATE TOTAL NET PROFIT % (day-by-day sum method)
                total_net_profit_sum = 0
                
                if product_avg_price > 0:
                    for date in campaign_dates:
                        date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                        if not date_data.empty:
                            row_data = date_data.iloc[0]
                            
                            date_amount_spent = round(row_data.get("Amount Spent (USD)", 0) 
                                                    if pd.notna(row_data.get("Amount Spent (USD)")) else 0, 2)
                            date_purchases = round(row_data.get("Purchases", 0) 
                                                 if pd.notna(row_data.get("Purchases")) else 0, 2)
                            
                            date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                            date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                            date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
                            
                            calc_purchases_date = round(date_purchases, 2)
                            delivery_rate_date = round(date_delivery_rate / 100 if date_delivery_rate > 1 
                                                     else date_delivery_rate, 2)
                            
                            delivered_orders = round(calc_purchases_date * delivery_rate_date, 2)
                            net_revenue = round(delivered_orders * date_avg_price, 2)
                            total_product_cost_date = round(delivered_orders * date_product_cost, 2)
                            total_shipping_cost_date = round(calc_purchases_date * shipping_rate, 2)
                            total_operational_cost_date = round(calc_purchases_date * operational_rate, 2)
                            
                            date_net_profit = round(net_revenue - (date_amount_spent * 100) - 
                                                  total_shipping_cost_date - total_operational_cost_date - 
                                                  total_product_cost_date, 2)
                            
                            total_net_profit_sum += round(date_net_profit, 2)
                    
                    # Calculate Total Net Profit %
                    calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) \
                        else total_purchases
                    delivery_rate_total = round(product_delivery_rate / 100 if product_delivery_rate > 1 
                                              else product_delivery_rate, 2)
                    
                    numerator_total = round(total_net_profit_sum, 2)
                    denominator_total = round(product_avg_price * calc_purchases_total * delivery_rate_total, 2)
                    total_net_profit_pct = round((numerator_total / denominator_total * 100), 2) \
                        if denominator_total > 0 else 0
                else:
                    total_net_profit_pct = 0
                
                # Store complete analysis
                campaign_complete_analysis = {
                    'Product': str(product),
                    'Campaign Name': str(campaign_name),
                    'day_wise_metrics': day_wise_metrics_dict,
                    'Total Net Profit %': round(total_net_profit_pct, 2),
                    'CPP': round(cpp, 2),
                    'BE': be
                }
                
                all_campaigns_complete_analysis.append(campaign_complete_analysis)
        
        # Write all campaigns to the complete analysis table
        for campaign_data in all_campaigns_complete_analysis:
            col_num = 0
            
            # Write Product and Campaign Name
            safe_write(negative_profit_sheet, current_row, col_num, 
                      campaign_data['Product'], analysis_data_format)
            col_num += 1
            safe_write(negative_profit_sheet, current_row, col_num, 
                      campaign_data['Campaign Name'], analysis_data_format)
            col_num += 1
            
            # Skip separator column after Campaign Name
            col_num += 1
            
            # Write day-wise ALL METRICS for each date
            for date in unique_dates:
                day_metrics = campaign_data['day_wise_metrics'].get(date, {})
                
                # Write all 12 metrics for this date
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('avg_price', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('delivery_rate', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('product_cost_input', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('amount_spent', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('purchases', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('delivered_orders', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('net_revenue', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('total_product_cost', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('total_shipping_cost', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('total_operational_cost', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('net_profit', 0), analysis_data_format)
                col_num += 1
                safe_write(negative_profit_sheet, current_row, col_num, 
                          day_metrics.get('net_profit_pct', 0), analysis_data_format)
                col_num += 1
                
                # Skip separator column after each date
                col_num += 1
            
            # Write summary columns
            safe_write(negative_profit_sheet, current_row, col_num, 
                      campaign_data['Total Net Profit %'], analysis_data_format)
            col_num += 1
            safe_write(negative_profit_sheet, current_row, col_num, 
                      campaign_data['CPP'], analysis_data_format)
            col_num += 1
            safe_write(negative_profit_sheet, current_row, col_num, 
                      campaign_data['BE'], analysis_data_format)
            
            current_row += 1
        
        # Add summary for complete analysis table
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, 
                  f"TOTAL CAMPAIGNS ANALYZED: {len(all_campaigns_complete_analysis)}", 
                  analysis_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, 
                  f"TOTAL UNIQUE DATES: {len(unique_dates)}", 
                  analysis_header_format)
        safe_write(negative_profit_sheet, current_row + 2, 0, 
                  f"DATE RANGE: {min(unique_dates)} to {max(unique_dates)}" if unique_dates else "No dates found", 
                  analysis_header_format)
        
        # Add spacing before filtered tables
        current_row += 5
        # HIDE THE COMPLETE ANALYSIS TABLE (rows 0 to current_row - 6)
 # We keep it for calculations but hide it from view
        analysis_table_end_row = current_row - 6  # The row where analysis table ends
        for row_idx in range(0, analysis_table_end_row + 1):
              negative_profit_sheet.set_row(row_idx, None, None, {'hidden': True})

 # Add a visible header for the filtered tables section
        safe_write(negative_profit_sheet, current_row, 0, 
          "FILTERED CAMPAIGN ANALYSIS TABLES", 
          negative_profit_header_format)
        current_row += 2
        # SET UP COLUMN GROUPING for Complete Analysis Table
        # Count total columns (including separators)
        total_analysis_columns = len(analysis_headers)
        
        # Start grouping from column 3 (after Product, Campaign Name, and first separator)
        start_col = 3
        
        group_level = 1
        while start_col < total_analysis_columns:
            # Skip if this is a separator column
            if start_col < len(analysis_headers) and analysis_headers[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
            
            # Count data columns (12 metrics per date)
            data_cols_found = 0
            end_col = start_col
            while end_col < total_analysis_columns and data_cols_found < 12:
                if not analysis_headers[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 12:
                    end_col += 1
            
            # Set column grouping (collapsed and hidden initially)
            if end_col < total_analysis_columns:
                negative_profit_sheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden': True}
                )
            
            # Move to next group (skip the separator column)
            start_col = end_col + 1
        
        # Configure outline settings for the sheet
        negative_profit_sheet.outline_settings(
            symbols_below=True,
            symbols_right=True,
            auto_style=False
        )
        
        # Set column widths for complete analysis table
        negative_profit_sheet.set_column(0, 0, 25)  # Product
        negative_profit_sheet.set_column(1, 1, 35)  # Campaign Name
        negative_profit_sheet.set_column(2, 2, 3)   # Separator after Campaign Name
        
        # Set widths for day-wise columns (12 metrics per date) and separators
        col_index = 3
        for i in range(len(unique_dates)):
            negative_profit_sheet.set_column(col_index, col_index, 15)      # Avg Price
            negative_profit_sheet.set_column(col_index + 1, col_index + 1, 15)  # Delivery Rate
            negative_profit_sheet.set_column(col_index + 2, col_index + 2, 18)  # Product Cost Input
            negative_profit_sheet.set_column(col_index + 3, col_index + 3, 18)  # Amount Spent
            negative_profit_sheet.set_column(col_index + 4, col_index + 4, 12)  # Purchases
            negative_profit_sheet.set_column(col_index + 5, col_index + 5, 18)  # Delivered Orders
            negative_profit_sheet.set_column(col_index + 6, col_index + 6, 18)  # Net Revenue
            negative_profit_sheet.set_column(col_index + 7, col_index + 7, 20)  # Total Product Cost
            negative_profit_sheet.set_column(col_index + 8, col_index + 8, 20)  # Total Shipping Cost
            negative_profit_sheet.set_column(col_index + 9, col_index + 9, 22)  # Total Operational Cost
            negative_profit_sheet.set_column(col_index + 10, col_index + 10, 15) # Net Profit
            negative_profit_sheet.set_column(col_index + 11, col_index + 11, 18) # Net Profit %
            col_index += 12
            
            # Separator column after each date
            negative_profit_sheet.set_column(col_index, col_index, 3)
            col_index += 1
        
        # Summary columns
        negative_profit_sheet.set_column(col_index, col_index, 20)      # Total Net Profit %
        negative_profit_sheet.set_column(col_index + 1, col_index + 1, 15)  # CPP
        negative_profit_sheet.set_column(col_index + 2, col_index + 2, 15)  # BE
        negative_profit_sheet.set_column(1, 1, 35)  # Campaign Name
        
        # Day-wise columns
        for i in range(len(unique_dates)):
            negative_profit_sheet.set_column(2 + i, 2 + i, 18)
        # Summary columns
        col_offset = 2 + len(unique_dates)
        negative_profit_sheet.set_column(col_offset, col_offset, 20)      # Total Net Profit %
        negative_profit_sheet.set_column(col_offset + 1, col_offset + 1, 15)  # CPP
        negative_profit_sheet.set_column(col_offset + 2, col_offset + 2, 15)  # BE

        # ==== NOW CONTINUE WITH FILTERED TABLES (EXISTING LOGIC) ====
        # STEP 1: Filter campaigns from the complete analysis table based on threshold
        # Instead of recalculating, use the data from all_campaigns_complete_analysis
        
        all_campaign_analysis = []
        
        # Filter campaigns that meet the threshold from the complete analysis table
        for campaign_complete in all_campaigns_complete_analysis:
            # Count negative days from day_wise_metrics
            negative_days_count = 0
            negative_dates_list = []
            
            for date, metrics in campaign_complete['day_wise_metrics'].items():
                if metrics.get('net_profit_pct', 0) < 0:
                    negative_days_count += 1
                    negative_dates_list.append(date)
            
            # Check if campaign meets threshold
            total_dates = len(campaign_complete['day_wise_metrics'])
            
            # Skip campaigns with fewer dates than threshold
            if total_dates < selected_days:
                continue
            
            # Only include campaigns with AT LEAST selected_days number of negative days
            if negative_days_count >= selected_days:
                # Format negative dates for display
                formatted_negative_dates = [format_date_readable(date) for date in negative_dates_list[:10]]
                formatted_dates_str = ", ".join(formatted_negative_dates)
                if len(negative_dates_list) > 10:
                    formatted_dates_str += "..."
                
                # Get last day delivery status
                product = campaign_complete['Product']
                campaign_name = campaign_complete['Campaign Name']
                
                # Get campaign group from df_main
                product_df = df_main[df_main['Product'] == product]
                campaign_group = product_df[product_df['Campaign Name'] == campaign_name]
                
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() 
                                       if pd.notna(d) and str(d).strip() != ''])
                
                last_date = campaign_dates[-1] if campaign_dates else None
                last_day_delivery_status = ""
                
                if last_date:
                    last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                    if not last_date_data.empty:
                        row_data = last_date_data.iloc[0]
                        delivery_status_raw = row_data.get("Delivery status", "")
                        if pd.notna(delivery_status_raw) and str(delivery_status_raw).strip() != "":
                            delivery_status_normalized = str(delivery_status_raw).strip().lower()
                            if "active" in delivery_status_normalized and "inactive" not in delivery_status_normalized:
                                last_day_delivery_status = "Active"
                            else:
                                last_day_delivery_status = "Inactive"
                
                # Get total amount spent and purchases from df_main
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() \
                    if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() \
                    if "Purchases" in campaign_group.columns else 0
                
                # Build campaign analysis entry
                campaign_analysis = {
                    'Product': campaign_complete['Product'],
                    'Campaign Name': campaign_complete['Campaign Name'],
                    'Total Dates': len(campaign_dates), 
                    'Days Checked': selected_days,
                    'Days with Negative Net Profit %': negative_days_count,
                    'CPP': campaign_complete['CPP'],
                    'BE': campaign_complete['BE'],
                    'Amount Spent (USD)': round(total_amount_spent_usd, 2),
                    'Total Purchases': int(total_purchases),
                    'Total Net Profit %': campaign_complete['Total Net Profit %'],
                    'Last Day Delivery Status': last_day_delivery_status,
                    'Negative Net Profit Dates': formatted_dates_str,
                    'Reason': f"Has {negative_days_count} negative net profit % days out of {total_dates} total days (threshold: {selected_days})"
                }
                
                all_campaign_analysis.append(campaign_analysis)
        
        # STEP 2: Filter campaigns based on threshold
        # Only include campaigns with AT LEAST selected_days number of negative days
        filtered_campaigns = all_campaign_analysis  # Already filtered above
        
        # STEP 3: Split into categories based on Total Net Profit %

        # STEP 3: Split into categories based on Total Net Profit %
        severe_negative_campaigns = [c for c in filtered_campaigns if c['Total Net Profit %'] <= -10]
        moderate_negative_campaigns = [c for c in filtered_campaigns 
                                      if -10 < c['Total Net Profit %'] < 0]
        positive_campaigns = [c for c in filtered_campaigns if c['Total Net Profit %'] >= 0]

        # Sort all groups
        severe_negative_campaigns.sort(key=lambda x: x['Total Net Profit %'])
        moderate_negative_campaigns.sort(key=lambda x: x['Total Net Profit %'])
        positive_campaigns.sort(key=lambda x: x['Total Net Profit %'], reverse=True)

        # STEP 4: Write to Excel - TABLE 1: SEVERE NEGATIVE (-100% to -10%)
        safe_write(negative_profit_sheet, current_row, 0, 
                  "CAMPAIGNS WITH SEVERE NEGATIVE NET PROFIT % (-100% TO -10%)", 
                  negative_profit_header_format)
        current_row += 1

        severe_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", 
                         "Net Profit %", "Last Day Delivery Status", "Comment", "Total Dates", 
                         "Days Checked", "Days with Negative Net Profit %", 
                         "Negative Net Profit Dates", "Reason"]

        for col_num, header in enumerate(severe_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, 
                      negative_profit_header_format)
        current_row += 1

        if severe_negative_campaigns:
            for campaign in severe_negative_campaigns:
                daily_spend = campaign['Amount Spent (USD)'] / campaign['Total Dates'] \
                    if campaign['Total Dates'] > 0 else 0
                comment = "Turn it off" if daily_spend < 20 else \
                    "Change the bid and keep BE value as add cost"
                
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Total Net Profit %'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Last Day Delivery Status'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 7, comment, 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 8, campaign['Total Dates'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Days Checked'], 
                          negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 10, 
                          campaign['Days with Negative Net Profit %'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 11, 
                          campaign['Negative Net Profit Dates'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 12, campaign['Reason'], 
                          negative_profit_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, 
                      "No campaigns found with severe negative net profit % (-100% to -10%)", 
                      negative_profit_data_format)
            current_row += 1

        # Summary for severe
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - SEVERE NEGATIVE CAMPAIGNS", 
                  negative_profit_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, 
                  f"Campaigns with severe negative net profit % (-100% to -10%): {len(severe_negative_campaigns)}", 
                  negative_profit_data_format)
        current_row += 3

        # [Continue with remaining tables - TABLE 2, 3, 4, and summaries following
        # TABLE 2: MODERATE NEGATIVE (-10% to 0%, excluding 0%)
        current_row += 2

        moderate_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", 
                           "Net Profit %", "Total Dates", "Days Checked", 
                           "Days with Negative Net Profit %", "Negative Net Profit Dates", "Reason"]

        safe_write(negative_profit_sheet, current_row, 0, 
                  "CAMPAIGNS WITH MODERATE NEGATIVE NET PROFIT % (-10% TO 0%, EXCLUDING 0%)", 
                  moderate_negative_header_format)
        current_row += 1

        for col_num, header in enumerate(moderate_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, 
                      moderate_negative_header_format)
        current_row += 1

        if moderate_negative_campaigns:
            for campaign in moderate_negative_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Total Net Profit %'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Total Dates'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Days Checked'], 
                          moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 8, 
                          campaign['Days with Negative Net Profit %'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 9, 
                          campaign['Negative Net Profit Dates'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], 
                          moderate_negative_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, 
                      "No campaigns found with moderate negative net profit % (-10% to 0%, excluding 0%)", 
                      moderate_negative_data_format)
            current_row += 1

        # Summary for moderate
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - MODERATE NEGATIVE CAMPAIGNS", 
                  moderate_negative_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, 
                  f"Campaigns with moderate negative net profit % (-10% to 0%, excluding 0%): {len(moderate_negative_campaigns)}", 
                  moderate_negative_data_format)

        # TABLE 3: POSITIVE (0% and above)
        current_row += 5

        positive_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", 
                           "Net Profit %", "Total Dates", "Days Checked", 
                           "Days with Negative Net Profit %", "Negative Net Profit Dates", "Reason"]

        safe_write(negative_profit_sheet, current_row, 0, 
                  "CAMPAIGNS WITH POSITIVE NET PROFIT % (0% AND ABOVE)", positive_header_format)
        current_row += 1

        for col_num, header in enumerate(positive_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, positive_header_format)
        current_row += 1

        if positive_campaigns:
            for campaign in positive_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Total Net Profit %'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Total Dates'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Days Checked'], 
                          positive_data_format)
                safe_write(negative_profit_sheet, current_row, 8, 
                          campaign['Days with Negative Net Profit %'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 9, 
                          campaign['Negative Net Profit Dates'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], 
                          positive_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, 
                      "No campaigns found with positive net profit % (0% and above)", 
                      positive_data_format)
            current_row += 1

        # Summary for positive
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - POSITIVE CAMPAIGNS", 
                  positive_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, 
                  f"Campaigns with positive net profit % (0% and above): {len(positive_campaigns)}", 
                  positive_data_format)

        # TABLE 4: LAST DATE NEGATIVE (separate analysis)
        current_row += 5

        safe_write(negative_profit_sheet, current_row, 0, 
                  "CAMPAIGNS WITH NEGATIVE NET PROFIT % ON LAST DATE", last_date_header_format)
        current_row += 1

        last_date_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", 
                            "Net Profit %", "Last Date", "Last Date Net Profit %", 
                            "Last Date Amount Spent (USD)", "Last Date Purchases", "Reason"]

        for col_num, header in enumerate(last_date_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, 
                      last_date_header_format)
        current_row += 1

        # Get campaigns already in first three tables
        already_processed = set((c['Product'], c['Campaign Name']) for c in filtered_campaigns)

        # DO NOT skip any campaigns - get ALL campaigns with negative net profit on last date
        # Remove the already_processed logic
        # Analyze last date
        last_date = unique_dates[-1] if unique_dates else None
        last_date_negative_campaigns = []
        
        if last_date:
            # Use the complete analysis table data we already have
            for campaign_complete in all_campaigns_complete_analysis:
                product = campaign_complete['Product']
                campaign_name = campaign_complete['Campaign Name']
                # SKIP if this campaign is already in any of the first three tables
                if (str(product), str(campaign_name)) in already_processed:
                    continue
                # Get the last date's net profit % from the day_wise_metrics
                last_date_metrics = campaign_complete['day_wise_metrics'].get(last_date, {})
                last_date_net_profit_pct = last_date_metrics.get('net_profit_pct', 0)
                
                # Only include campaigns with negative net profit % on last date
                if last_date_net_profit_pct < 0:
                    # Get additional data from df_main
                    product_df = df_main[df_main['Product'] == product]
                    campaign_group = product_df[product_df['Campaign Name'] == campaign_name]
                    
                    # Get total amount spent and purchases
                    total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() \
                        if "Amount Spent (USD)" in campaign_group.columns else 0
                    total_purchases = campaign_group.get("Purchases", 0).sum() \
                        if "Purchases" in campaign_group.columns else 0
                    
                    # Get last date specific data
                    last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                    last_date_amount_spent = 0
                    last_date_purchases = 0
                    
                    if not last_date_data.empty:
                        last_date_row = last_date_data.iloc[0]
                        last_date_amount_spent = round(last_date_row.get("Amount Spent (USD)", 0) or 0, 2)
                        last_date_purchases = int(last_date_row.get("Purchases", 0) or 0)
                    
                    last_date_campaign = {
                        'Product': str(product),
                        'Campaign Name': str(campaign_name),
                        'CPP': campaign_complete['CPP'],
                        'BE': campaign_complete['BE'],
                        'Amount Spent (USD)': round(total_amount_spent_usd, 2),
                        'Net Profit %': campaign_complete['Total Net Profit %'],
                        'Last Date': format_date_readable(last_date),
                        'Last Date Net Profit %': round(last_date_net_profit_pct, 2),
                        'Last Date Amount Spent (USD)': last_date_amount_spent,
                        'Last Date Purchases': last_date_purchases,
                        'Reason': f"Negative net profit % ({round(last_date_net_profit_pct, 2)}%) on last date ({format_date_readable(last_date)})"
                    }
                    
                    last_date_negative_campaigns.append(last_date_campaign)

        # Sort last date campaigns by last date net profit %
        last_date_negative_campaigns.sort(key=lambda x: x['Last Date Net Profit %'])

        # Write last date campaigns
        if last_date_negative_campaigns:
            for campaign in last_date_negative_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Net Profit %'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Last Date'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Last Date Net Profit %'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 8, 
                          campaign['Last Date Amount Spent (USD)'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Last Date Purchases'], 
                          last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], 
                          last_date_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, 
                      f"No campaigns found with negative net profit % on last date ({format_date_readable(last_date) if last_date else 'N/A'})", 
                      last_date_data_format)
            current_row += 1

        # Summary for last date
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - LAST DATE TABLE", 
                  last_date_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, 
                  f"Last date analyzed: {format_date_readable(last_date) if last_date else 'N/A'}", 
                  last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 2, 0, 
                  f"Campaigns with negative net profit % on last date: {len(last_date_negative_campaigns)}", 
                  last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 3, 0, 
                  "Note: Campaigns already in Tables 1-3 are excluded from this table", 
                  last_date_data_format)
        # OVERALL SUMMARY
        current_row += 5
        safe_write(negative_profit_sheet, current_row, 0, "OVERALL SUMMARY", 
                  negative_profit_header_format)

        # Count total campaigns analyzed
        total_campaigns = 0
        for product, product_df in df_main.groupby("Product"):
            total_campaigns += len(product_df.groupby("Campaign Name"))

        safe_write(negative_profit_sheet, current_row + 1, 0, 
                  f"Total campaigns analyzed: {total_campaigns}", negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 2, 0, 
                  f"Total campaigns meeting threshold (≥{selected_days} negative days): {len(filtered_campaigns)}", 
                  negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 3, 0, 
                  f"Severe negative campaigns (-100% to -10%): {len(severe_negative_campaigns)}", 
                  negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 4, 0, 
                  f"Moderate negative campaigns (-10% to 0%, excluding 0%): {len(moderate_negative_campaigns)}", 
                  moderate_negative_data_format)
        safe_write(negative_profit_sheet, current_row + 5, 0, 
                  f"Positive campaigns (0% and above): {len(positive_campaigns)}", 
                  positive_data_format)
        safe_write(negative_profit_sheet, current_row + 6, 0, 
                  f"Last date negative campaigns: {len(last_date_negative_campaigns)}", 
                  last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 7, 0, 
                  f"Days threshold used: {selected_days} out of {len(unique_dates)} total unique dates", 
                  negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 8, 0, 
                  f"Date range analyzed: {min(unique_dates)} to {max(unique_dates)}" 
                  if unique_dates else "No dates found", negative_profit_data_format)
        
        
        
        
        # ==== MODIFIED SHEET: Profit and Loss Products ====
        profit_loss_sheet = workbook.add_worksheet("Profit and Loss Products")
        
        # Formats for combined profit and loss sheet
        positive_profit_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#4CAF50", "font_name": "Calibri", "font_size": 11
        })
        positive_profit_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E8F5E8", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # NEW: Formats for negative net profit products table (top right)
        negative_profit_header_format_top = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
        })
        negative_profit_data_format_top = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        moderate_negative_format_combined = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
        })
        moderate_negative_data_format_combined  = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFEBEE", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00",
            "font_color": "#D32F2F"
        })
        negative_profit_header_format_combined = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFA500", "font_name": "Calibri", "font_size": 11
        })
        negative_profit_data_format_combined = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFF3E0", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00",
            "font_color": "#F57C00"
        })
        overall_summary_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#E0E0E0", "font_name": "Calibri", "font_size": 11
        })
        
        current_row = 0
        
        # ==== SECTION 1: SIDE-BY-SIDE TABLES ====
        # LEFT SIDE: POSITIVE NET PROFIT PRODUCTS
        # RIGHT SIDE: NEGATIVE NET PROFIT PRODUCTS (NEW)
        
        # LEFT TABLE: POSITIVE NET PROFIT PRODUCTS
        safe_write(profit_loss_sheet, current_row, 0, "POSITIVE NET PROFIT PRODUCTS", positive_profit_header_format)
        
        # RIGHT TABLE: NEGATIVE NET PROFIT PRODUCTS (NEW)
        safe_write(profit_loss_sheet, current_row, 5, "NEGATIVE NET PROFIT PRODUCTS", negative_profit_header_format_top)
        current_row += 1
        
        # Headers for both tables - UPDATED: Added CPP and BE columns
        positive_headers = ["Product Name", "CPP", "BE", "Total Net Profit %", "Total Net Profit"]
        negative_headers = ["Product Name", "CPP", "BE", "Total Net Profit %", "Total Net Profit"]
        
       # Write headers for positive table (left side)
        for col_num, header in enumerate(positive_headers):
            safe_write(profit_loss_sheet, current_row, col_num, header, positive_profit_header_format)
        
        # Write headers for negative table (right side) - starting from column 7 (was 5, now 7 due to 2 extra columns)
        for col_num, header in enumerate(negative_headers):
            safe_write(profit_loss_sheet, current_row, col_num + 7, header, negative_profit_header_format_top)
        current_row += 1
        
        # Filter and sort positive products by net profit (highest to lowest)
        
        # Filter and sort positive products by net profit (highest to lowest)
        positive_products = [(product, net_profit) for product, net_profit in product_net_profit_values.items() if net_profit >= 0]
        positive_products.sort(key=lambda x: x[1], reverse=True)
        
        # Filter and sort negative products by net profit (worst to best, i.e., most negative first)
        negative_products = [(product, net_profit) for product, net_profit in product_net_profit_values.items() if net_profit < 0]
        negative_products.sort(key=lambda x: x[1])  # Sort ascending (most negative first)
        
        # Determine the maximum number of rows needed for both tables
        max_rows = max(len(positive_products), len(negative_products))
        
        # Write data for both tables side by side
        for i in range(max_rows):
            # LEFT TABLE: Positive products
            if i < len(positive_products):
                product, net_profit = positive_products[i]
                # Calculate Net Profit % for this product
                product_data = df_main[df_main['Product'] == product]
                total_purchases = product_data['Purchases'].sum() if 'Purchases' in product_data.columns else 0
                total_amount_spent = product_data['Amount Spent (USD)'].sum() if 'Amount Spent (USD)' in product_data.columns else 0
                
                # Use the pre-calculated product-level values
                product_avg_price = product_total_avg_prices.get(product, 0)
                product_delivery_rate = product_total_delivery_rates.get(product, 0)
                
                # Calculate CPP
                cpp = 0
                if total_amount_spent > 0 and total_purchases == 0:
                    cpp = total_amount_spent / 1
                elif total_purchases > 0:
                    cpp = total_amount_spent / total_purchases
                
                # Get BE value
                be = product_be_values.get(product, 0)
                
                # Calculate Net Profit %
                if product_avg_price > 0 and total_purchases > 0:
                    delivery_rate = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                    denominator = product_avg_price * delivery_rate * total_purchases
                    net_profit_percent = (net_profit / denominator * 100) if denominator > 0 else 0
                else:
                    net_profit_percent = 0
                
                safe_write(profit_loss_sheet, current_row, 0, str(product), positive_profit_data_format)
                safe_write(profit_loss_sheet, current_row, 1, round(cpp, 2), positive_profit_data_format)
                safe_write(profit_loss_sheet, current_row, 2, round(be, 2), positive_profit_data_format)
                safe_write(profit_loss_sheet, current_row, 3, round(net_profit_percent, 2), positive_profit_data_format)
                safe_write(profit_loss_sheet, current_row, 4, net_profit, positive_profit_data_format)
            
            # RIGHT TABLE: Negative products
            if i < len(negative_products):
                product, net_profit = negative_products[i]
                # Calculate Net Profit % for this product
                product_data = df_main[df_main['Product'] == product]
                total_purchases = product_data['Purchases'].sum() if 'Purchases' in product_data.columns else 0
                total_amount_spent = product_data['Amount Spent (USD)'].sum() if 'Amount Spent (USD)' in product_data.columns else 0
                
                # Use the pre-calculated product-level values
                product_avg_price = product_total_avg_prices.get(product, 0)
                product_delivery_rate = product_total_delivery_rates.get(product, 0)
                
                # Calculate CPP
                cpp = 0
                if total_amount_spent > 0 and total_purchases == 0:
                    cpp = total_amount_spent / 1
                elif total_purchases > 0:
                    cpp = total_amount_spent / total_purchases
                
                # Get BE value
                be = product_be_values.get(product, 0)
                
                # Calculate Net Profit %
                if product_avg_price > 0 and total_purchases > 0:
                    delivery_rate = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                    denominator = product_avg_price * delivery_rate * total_purchases
                    net_profit_percent = (net_profit / denominator * 100) if denominator > 0 else 0
                else:
                    net_profit_percent = 0
                
                safe_write(profit_loss_sheet, current_row, 7, str(product), negative_profit_data_format_top)
                safe_write(profit_loss_sheet, current_row, 8, round(cpp, 2), negative_profit_data_format_top)
                safe_write(profit_loss_sheet, current_row, 9, round(be, 2), negative_profit_data_format_top)
                safe_write(profit_loss_sheet, current_row, 10, round(net_profit_percent, 2), negative_profit_data_format_top)
                safe_write(profit_loss_sheet, current_row, 11, net_profit, negative_profit_data_format_top)
            
            current_row += 1

        # Add empty rows if one table is shorter
        if len(positive_products) == 0:
            safe_write(profit_loss_sheet, current_row, 0, "No products with positive net profit found", positive_profit_data_format)
            current_row += 1
        
        if len(negative_products) == 0:
            safe_write(profit_loss_sheet, current_row, 7, "No products with negative net profit found", negative_profit_data_format_top)
            current_row += 1

        # Add summaries for both tables side by side
        current_row += 2
        safe_write(profit_loss_sheet, current_row, 0, "SUMMARY - POSITIVE NET PROFIT PRODUCTS", positive_profit_header_format)
        safe_write(profit_loss_sheet, current_row, 7, "SUMMARY - NEGATIVE NET PROFIT PRODUCTS", negative_profit_header_format_top)
        current_row += 1
        
        total_positive_products = len(positive_products)
        total_positive_net_profit = sum([profit for _, profit in positive_products])
        avg_positive_net_profit = total_positive_net_profit / total_positive_products if total_positive_products > 0 else 0
        
        total_negative_products = len(negative_products)
        total_negative_net_profit = sum([profit for _, profit in negative_products])
        avg_negative_net_profit = total_negative_net_profit / total_negative_products if total_negative_products > 0 else 0
        
        safe_write(profit_loss_sheet, current_row, 0, f"Total Positive Products: {total_positive_products}", positive_profit_data_format)
        safe_write(profit_loss_sheet, current_row, 7, f"Total Negative Products: {total_negative_products}", negative_profit_data_format_top)
        current_row += 1
        
        safe_write(profit_loss_sheet, current_row, 0, f"Total Net Profit (Positive): {round(total_positive_net_profit, 2)}", positive_profit_data_format)
        safe_write(profit_loss_sheet, current_row, 7, f"Total Net Loss (Negative): {round(total_negative_net_profit, 2)}", negative_profit_data_format_top)
        current_row += 1
        
        safe_write(profit_loss_sheet, current_row, 0, f"Average Net Profit per Product: {round(avg_positive_net_profit, 2)}", positive_profit_data_format)
        safe_write(profit_loss_sheet, current_row, 7, f"Average Net Loss per Product: {round(avg_negative_net_profit, 2)}", negative_profit_data_format_top)
        
        current_row += 8  # Add gap between sections
        
        # ==== SECTION 2: NEGATIVE NET PROFIT PRODUCTS (DETAILED ANALYSIS) ====
        
        # Filter negative products and calculate ratio
        negative_products_with_ratio = []
        for product, net_profit in product_net_profit_values.items():
            if net_profit < 0:
                # Calculate Total Total Product Cost for this product
                total_product_cost = 0
                product_data = df_main[df_main['Product'] == product]
                
                for date in unique_dates:
                    date_data = product_data[product_data['Date'].astype(str) == date]
                    if not date_data.empty:
                        date_purchases = date_data['Purchases'].sum() if 'Purchases' in date_data.columns else 0
                        date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
                        date_product_cost_input = safe_lookup_get(product_date_cost_inputs, product, 0.0)
                        
                        delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                        delivered_orders = round(date_purchases * delivery_rate, 2)
                        product_cost = round(delivered_orders * date_product_cost_input, 2)
                        total_product_cost += product_cost
                
                # Calculate ratio: Total Net Profit / Total Total Product Cost
                if total_product_cost != 0:
                    ratio = (1 + ( net_profit/total_product_cost )) * 100
                else:
                    ratio = 0  # Handle division by zero
                
                negative_products_with_ratio.append({
                    'product': product,
                    'total_product_cost': round(total_product_cost, 2),
                    'net_profit': net_profit,
                    'ratio': ratio
                })
        
        # Sort by ratio (worst first - most negative ratios first)
        negative_products_with_ratio.sort(key=lambda x: x['ratio'])
        
        # Split into two groups based on ratio threshold (20)
        ratio_less_than_20 = [p for p in negative_products_with_ratio if abs(p['ratio']) < 20]
        ratio_greater_equal_20 = [p for p in negative_products_with_ratio if abs(p['ratio']) >= 20]
        
        # SUBSECTION 2A: Products with ratio < 20 (moderate)
        safe_write(profit_loss_sheet, current_row, 0, "NEGATIVE NET PROFIT PRODUCTS - RATIO < 20 (MODERATE)", negative_profit_header_format_combined)
        current_row += 1
        
        # Headers with ratio column
        negative_headers_with_ratio = ["Product Name", "Total Total Product Cost", "Total Net Loss", "Net Profit / Total Product Cost Ratio"]
        
        for col_num, header in enumerate(negative_headers_with_ratio):
            safe_write(profit_loss_sheet, current_row, col_num, header, negative_profit_header_format_combined)
        current_row += 1
        
        # Write products with ratio < 20
        if ratio_less_than_20:
            for product_data in ratio_less_than_20:
                safe_write(profit_loss_sheet, current_row, 0, str(product_data['product']), negative_profit_data_format_combined)
                safe_write(profit_loss_sheet, current_row, 1, product_data['total_product_cost'], negative_profit_data_format_combined)
                safe_write(profit_loss_sheet, current_row, 2, product_data['net_profit'], negative_profit_data_format_combined)
                safe_write(profit_loss_sheet, current_row, 3, round(product_data['ratio'], 4), negative_profit_data_format_combined)
                current_row += 1
        else:
            safe_write(profit_loss_sheet, current_row, 0, "No products found with ratio < 20", negative_profit_data_format_combined)
            current_row += 1
        
        # Add summary for ratio < 20
        current_row += 2
        safe_write(profit_loss_sheet, current_row, 0, "SUMMARY - RATIO < 20 (MODERATE)", negative_profit_header_format_combined)
        current_row += 1
        
        total_critical_products = len(ratio_less_than_20)
        total_critical_net_profit = sum([p['net_profit'] for p in ratio_less_than_20])
        total_critical_cost_input = sum([p['total_product_cost'] for p in ratio_less_than_20])
        avg_critical_ratio = sum([p['ratio'] for p in ratio_less_than_20]) / total_critical_products if total_critical_products > 0 else 0
        
        safe_write(profit_loss_sheet, current_row, 0, f"Products with ratio < 20: {total_critical_products}", negative_profit_data_format_combined)
        safe_write(profit_loss_sheet, current_row + 1, 0, f"Total Net Loss (MODERATE): {round(total_critical_net_profit, 2)}", negative_profit_data_format_combined)
        safe_write(profit_loss_sheet, current_row + 2, 0, f"Total Product Cost Input (MODERATE): {round(total_critical_cost_input, 2)}", negative_profit_data_format_combined)
        safe_write(profit_loss_sheet, current_row + 3, 0, f"Average Ratio (MODERATE): {round(avg_critical_ratio, 4)}", negative_profit_data_format_combined)
        
        current_row += 7  # Add gap between subsections
        
        # SUBSECTION 2B: Products with ratio >= 20 (critical)
        safe_write(profit_loss_sheet, current_row, 0, "NEGATIVE NET PROFIT PRODUCTS - RATIO >= 20 (CRITICAL)", moderate_negative_format_combined)
        current_row += 1
        
        # Headers for second subsection
        for col_num, header in enumerate(negative_headers_with_ratio):
            safe_write(profit_loss_sheet, current_row, col_num, header, moderate_negative_format_combined)
        current_row += 1
        
        # Write products with ratio >= 20
        if ratio_greater_equal_20:
            for product_data in ratio_greater_equal_20:
                safe_write(profit_loss_sheet, current_row, 0, str(product_data['product']), moderate_negative_data_format_combined)
                safe_write(profit_loss_sheet, current_row, 1, product_data['total_product_cost'], moderate_negative_data_format_combined)
                safe_write(profit_loss_sheet, current_row, 2, product_data['net_profit'], moderate_negative_data_format_combined)
                safe_write(profit_loss_sheet, current_row, 3, round(product_data['ratio'], 4), moderate_negative_data_format_combined)
                current_row += 1
        else:
            safe_write(profit_loss_sheet, current_row, 0, "No products found with ratio >= 20", moderate_negative_data_format_combined)
            current_row += 1
        
        # Add summary for ratio >= 20
        current_row += 2
        safe_write(profit_loss_sheet, current_row, 0, "SUMMARY - RATIO >= 20 (CRITICAL)", moderate_negative_format_combined)
        current_row += 1
        
        total_moderate_products = len(ratio_greater_equal_20)
        total_moderate_net_profit = sum([p['net_profit'] for p in ratio_greater_equal_20])
        total_moderate_cost_input = sum([p['total_product_cost'] for p in ratio_greater_equal_20])
        avg_moderate_ratio = sum([p['ratio'] for p in ratio_greater_equal_20]) / total_moderate_products if total_moderate_products > 0 else 0
        
        safe_write(profit_loss_sheet, current_row, 0, f"Products with ratio >= 20: {total_moderate_products}", moderate_negative_data_format_combined)
        safe_write(profit_loss_sheet, current_row + 1, 0, f"Total Net Loss (CRITICAL): {round(total_moderate_net_profit, 2)}", moderate_negative_data_format_combined)
        safe_write(profit_loss_sheet, current_row + 2, 0, f"Total Product Cost Input (CRITICAL): {round(total_moderate_cost_input, 2)}", moderate_negative_data_format_combined)
        safe_write(profit_loss_sheet, current_row + 3, 0, f"Average Ratio (CRITICAL): {round(avg_moderate_ratio, 4)}", moderate_negative_data_format_combined)
        
        current_row += 7  # Add gap before overall summary
        
        # ==== SECTION 3: OVERALL SUMMARY ====
        safe_write(profit_loss_sheet, current_row, 0, "OVERALL SUMMARY - ALL PRODUCTS", overall_summary_format)
        current_row += 1
        
        # Overall summary headers
        summary_headers = ["Category", "Count", "Total Net Profit", "Average Net Profit"]
        for col_num, header in enumerate(summary_headers):
            safe_write(profit_loss_sheet, current_row, col_num, header, overall_summary_format)
        current_row += 1
        
        # Calculate overall totals
        total_all_products = len(product_net_profit_values)
        total_all_net_profit = sum(product_net_profit_values.values())
        avg_all_net_profit = total_all_net_profit / total_all_products if total_all_products > 0 else 0
        
        # Write overall summary data
        summary_data = [
            ("Positive Products", total_positive_products, total_positive_net_profit, avg_positive_net_profit, positive_profit_data_format),
            ("Negative Products", total_negative_products, total_negative_net_profit, avg_negative_net_profit, negative_profit_data_format_top),
            ("Moderate Negative (ratio < 20)", total_critical_products, total_critical_net_profit, avg_critical_ratio, negative_profit_data_format_combined),
            ("Critical Negative (ratio >= 20)", total_moderate_products, total_moderate_net_profit, avg_moderate_ratio, moderate_negative_data_format_combined),
            ("ALL PRODUCTS", total_all_products, total_all_net_profit, avg_all_net_profit, overall_summary_format)
        ]
        
        for category, count, net_profit, avg_net_profit, format_style in summary_data:
            safe_write(profit_loss_sheet, current_row, 0, category, format_style)
            safe_write(profit_loss_sheet, current_row, 1, count, format_style)
            safe_write(profit_loss_sheet, current_row, 2, round(net_profit, 2), format_style)
            safe_write(profit_loss_sheet, current_row, 3, round(avg_net_profit, 2), format_style)
            current_row += 1
        
        # Set column widths for combined profit and loss sheet
        profit_loss_sheet.set_column(0, 0, 30)  # Product Name
        profit_loss_sheet.set_column(1, 1, 15)  # CPP
        profit_loss_sheet.set_column(2, 2, 15)  # BE
        profit_loss_sheet.set_column(3, 3, 20)  # Total Net Profit %
        profit_loss_sheet.set_column(4, 4, 20)  # Total Net Profit
        profit_loss_sheet.set_column(5, 5, 3)   # Separator column
        profit_loss_sheet.set_column(6, 6, 3)   # Separator column
        profit_loss_sheet.set_column(7, 7, 30)  # Right table Product Name
        profit_loss_sheet.set_column(8, 8, 15)  # Right table CPP
        profit_loss_sheet.set_column(9, 9, 15)  # Right table BE
        profit_loss_sheet.set_column(10, 10, 20) # Right table Total Net Profit %
        profit_loss_sheet.set_column(11, 11, 20) # Right table Total Net Profit
        
        # ==== NEW SHEET: Scalable Campaigns ====
        # ==== NEW SHEET: Scalable Campaigns ====
        scalable_sheet = workbook.add_worksheet("Scalable Campaigns")
        
        # Formats for scalable campaigns sheet
        scalable_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#4CAF50", "font_name": "Calibri", "font_size": 11
        })
        moderate_scalable_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#8BC34A", "font_name": "Calibri", "font_size": 11
        })
        high_scalable_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#2E7D32", "font_name": "Calibri", "font_size": 11
        })
        moderate_scalable_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#F1F8E9", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        high_scalable_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#C8E6C9", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        current_row = 0
        
        # FIXED: Build a lookup of Net Profit % values from the ACTUAL product-campaign data
        # We need to match the EXACT calculation used in the main sheet
        # FIXED: Build a lookup of Net Profit % values using DAY-BY-DAY calculation
        # This matches the approach in Negative Net Profit Campaigns sheet
        campaign_data_lookup = {}
        
        for product, product_df in df_main.groupby("Product"):
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                # Calculate campaign totals
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # Get product-level values
                product_avg_price = round(product_total_avg_prices.get(product, 0), 2)
                product_delivery_rate = round(product_total_delivery_rates.get(product, 0), 2)
                
                # Calculate Net Profit % using DAY-BY-DAY approach (matching Negative Net Profit Campaigns)
                campaign_net_profit_percentage = 0
                total_net_profit_sum = 0  # Sum of day-by-day net profits
                
                # Get all dates for this campaign
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
                
                if product_avg_price > 0:
                    # Calculate net profit by summing day-by-day (matching Negative Net Profit Campaigns sheet)
                    for date in campaign_dates:
                        date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                        if not date_data.empty:
                            row_data = date_data.iloc[0]
                            
                            # Get day-specific data
                            date_amount_spent = round(row_data.get("Amount Spent (USD)", 0) if pd.notna(row_data.get("Amount Spent (USD)")) else 0, 2)
                            date_purchases = round(row_data.get("Purchases", 0) if pd.notna(row_data.get("Purchases")) else 0, 2)
                            
                            # Get day-wise lookup data
                            date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                            date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                            date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
                            
                            # Calculate for this specific date (SAME AS NEGATIVE NET PROFIT CAMPAIGNS)
                            calc_purchases_date = round(date_purchases, 2)  # No special handling for zero here
                            delivery_rate_date = round(date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate, 2)
                            
                            delivered_orders = round(calc_purchases_date * delivery_rate_date, 2)
                            net_revenue = round(delivered_orders * date_avg_price, 2)
                            total_product_cost_date = round(delivered_orders * date_product_cost, 2)
                            total_shipping_cost_date = round(calc_purchases_date * shipping_rate, 2)
                            total_operational_cost_date = round(calc_purchases_date * operational_rate, 2)
                            
                            # Net profit for THIS DATE
                            date_net_profit = round(net_revenue - (date_amount_spent * 100) - total_shipping_cost_date - total_operational_cost_date - total_product_cost_date, 2)
                            
                            # ADD to total
                            total_net_profit_sum += round(date_net_profit, 2)
                    
                    # Now calculate Net Profit % = Total Net Profit / (Avg Price * Total Purchases * Delivery Rate) * 100
                    calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                    delivery_rate_total = round(product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate, 2)
                    
                    numerator_total = round(total_net_profit_sum, 2)
                    denominator_total = round(product_avg_price * calc_purchases_total * delivery_rate_total, 2)
                    campaign_net_profit_percentage = round((numerator_total / denominator_total * 100), 2) if denominator_total > 0 else 0
                
                # Get last date amount spent
                last_date = unique_dates[-1] if unique_dates else None
                last_date_amount_spent = 0
                
                if last_date:
                    last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                    if not last_date_data.empty:
                        last_date_row = last_date_data.iloc[0]
                        last_date_amount_spent = round(last_date_row.get("Amount Spent (USD)", 0) if pd.notna(last_date_row.get("Amount Spent (USD)")) else 0, 2)
                
                # Store in lookup
                campaign_key = (str(product), str(campaign_name))
                campaign_data_lookup[campaign_key] = {
                    'net_profit_pct': campaign_net_profit_percentage,
                    'total_amount_spent': round(total_amount_spent_usd, 2),
                    'total_purchases': int(total_purchases),
                    'cpp': round(total_amount_spent_usd / max(total_purchases, 1), 2) if (total_amount_spent_usd > 0 and total_purchases == 0) or total_purchases > 0 else 0,
                    'be': product_be_values.get(product, 0),
                    'total_dates': len([d for d in campaign_group['Date'].unique() 
                                   if pd.notna(d) and 
                                   campaign_group[campaign_group['Date'].astype(str) == str(d)].get('Amount Spent (USD)', pd.Series([0])).iloc[0] > 0]),
                    'last_date_amount_spent': last_date_amount_spent
                                }
        
        # Collect scalable campaigns (Net Profit % > 10)
        scalable_campaigns = []
        
        for campaign_key, campaign_data in campaign_data_lookup.items():
            if campaign_data['net_profit_pct'] > 10:
                scalable_campaign = {
                    'Product': campaign_key[0],
                    'Campaign Name': campaign_key[1],
                    'CPP': campaign_data['cpp'],
                    'BE': campaign_data['be'],
                    'Total Amount Spent (USD)': campaign_data['total_amount_spent'],
                    'Total Purchases': campaign_data['total_purchases'],
                    'Net Profit %': campaign_data['net_profit_pct'],
                    'Total Dates': campaign_data['total_dates'],
                    'Last Date Amount Spent (USD)': campaign_data['last_date_amount_spent']
                }
                scalable_campaigns.append(scalable_campaign)
        
        # Split into two groups
        # Split into FOUR groups based on Net Profit % AND Amount Spent
        moderate_scalable_high_spend = [c for c in scalable_campaigns if 10 < c['Net Profit %'] <= 20 and c['Total Amount Spent (USD)'] >= 10]
        moderate_scalable_low_spend = [c for c in scalable_campaigns if 10 < c['Net Profit %'] <= 20 and c['Total Amount Spent (USD)'] < 10]
        high_scalable_high_spend = [c for c in scalable_campaigns if c['Net Profit %'] > 20 and c['Total Amount Spent (USD)'] >= 10]
        high_scalable_low_spend = [c for c in scalable_campaigns if c['Net Profit %'] > 20 and c['Total Amount Spent (USD)'] < 10]
        
        # Sort all four groups by Net Profit % (highest first)
        moderate_scalable_high_spend.sort(key=lambda x: x['Net Profit %'], reverse=True)
        moderate_scalable_low_spend.sort(key=lambda x: x['Net Profit %'], reverse=True)
        high_scalable_high_spend.sort(key=lambda x: x['Net Profit %'], reverse=True)
        high_scalable_low_spend.sort(key=lambda x: x['Net Profit %'], reverse=True)
        
        # ==== TABLE 1A: MODERATE SCALABLE - HIGH SPEND (Amount >= $10) ====
        safe_write(scalable_sheet, current_row, 0, "MODERATE SCALABLE CAMPAIGNS (10% < NET PROFIT % ≤ 20%) - AMOUNT SPENT ≥ $10", moderate_scalable_header_format)
        current_row += 1
        
        # Headers
        scalable_headers = ["Product", "Campaign Name", "CPP", "BE", "Total Amount Spent (USD)", 
                           "Total Purchases", "Net Profit %", "Total Dates", "Last Date Amount Spent (USD)"]
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, moderate_scalable_header_format)
        current_row += 1
        
        # Write moderate scalable campaigns - HIGH SPEND
        if moderate_scalable_high_spend:
            for campaign in moderate_scalable_high_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['CPP'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['BE'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Net Profit %'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], moderate_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Net Profit % between 10% and 20% and Amount Spent >= $10", moderate_scalable_data_format)
            current_row += 1
        
        # Summary for moderate scalable - HIGH SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - MODERATE SCALABLE (HIGH SPEND ≥ $10)", moderate_scalable_header_format)
        current_row += 1
        
        total_moderate_high_campaigns = len(moderate_scalable_high_spend)
        total_moderate_high_spend = sum([c['Total Amount Spent (USD)'] for c in moderate_scalable_high_spend])
        total_moderate_high_purchases = sum([c['Total Purchases'] for c in moderate_scalable_high_spend])
        avg_moderate_high_net_profit_pct = sum([c['Net Profit %'] for c in moderate_scalable_high_spend]) / total_moderate_high_campaigns if total_moderate_high_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_moderate_high_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_moderate_high_spend:,.2f}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_moderate_high_purchases:,}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Net Profit %: {round(avg_moderate_high_net_profit_pct, 2)}%", moderate_scalable_data_format)
        
        current_row += 7  # Add gap between tables
        
        # ==== TABLE 1B: MODERATE SCALABLE - LOW SPEND (Amount < $10) ====
        safe_write(scalable_sheet, current_row, 0, "MODERATE SCALABLE CAMPAIGNS (10% < NET PROFIT % ≤ 20%) - AMOUNT SPENT < $10", moderate_scalable_header_format)
        current_row += 1
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, moderate_scalable_header_format)
        current_row += 1
        
        # Write moderate scalable campaigns - LOW SPEND
        if moderate_scalable_low_spend:
            for campaign in moderate_scalable_low_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['CPP'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['BE'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Net Profit %'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], moderate_scalable_data_format)
                safe_write(scalable_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], moderate_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Net Profit % between 10% and 20% and Amount Spent < $10", moderate_scalable_data_format)
            current_row += 1
        
        # Summary for moderate scalable - LOW SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - MODERATE SCALABLE (LOW SPEND < $10)", moderate_scalable_header_format)
        current_row += 1
        
        total_moderate_low_campaigns = len(moderate_scalable_low_spend)
        total_moderate_low_spend = sum([c['Total Amount Spent (USD)'] for c in moderate_scalable_low_spend])
        total_moderate_low_purchases = sum([c['Total Purchases'] for c in moderate_scalable_low_spend])
        avg_moderate_low_net_profit_pct = sum([c['Net Profit %'] for c in moderate_scalable_low_spend]) / total_moderate_low_campaigns if total_moderate_low_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_moderate_low_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_moderate_low_spend:,.2f}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_moderate_low_purchases:,}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Net Profit %: {round(avg_moderate_low_net_profit_pct, 2)}%", moderate_scalable_data_format)
        
        current_row += 7  # Add gap between major sections
        
        # ==== TABLE 2A: HIGH SCALABLE - HIGH SPEND (Amount >= $10) ====
        safe_write(scalable_sheet, current_row, 0, "HIGH SCALABLE CAMPAIGNS (NET PROFIT % > 20%) - AMOUNT SPENT ≥ $10", high_scalable_header_format)
        current_row += 1
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, high_scalable_header_format)
        current_row += 1
        
        # Write high scalable campaigns - HIGH SPEND
        if high_scalable_high_spend:
            for campaign in high_scalable_high_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['CPP'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['BE'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Net Profit %'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], high_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Net Profit % > 20% and Amount Spent >= $10", high_scalable_data_format)
            current_row += 1
        
        # Summary for high scalable - HIGH SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - HIGH SCALABLE (HIGH SPEND ≥ $10)", high_scalable_header_format)
        current_row += 1
        
        total_high_high_campaigns = len(high_scalable_high_spend)
        total_high_high_spend = sum([c['Total Amount Spent (USD)'] for c in high_scalable_high_spend])
        total_high_high_purchases = sum([c['Total Purchases'] for c in high_scalable_high_spend])
        avg_high_high_net_profit_pct = sum([c['Net Profit %'] for c in high_scalable_high_spend]) / total_high_high_campaigns if total_high_high_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_high_high_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_high_high_spend:,.2f}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_high_high_purchases:,}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Net Profit %: {round(avg_high_high_net_profit_pct, 2)}%", high_scalable_data_format)
        
        current_row += 7  # Add gap between tables
        
        # ==== TABLE 2B: HIGH SCALABLE - LOW SPEND (Amount < $10) ====
        safe_write(scalable_sheet, current_row, 0, "HIGH SCALABLE CAMPAIGNS (NET PROFIT % > 20%) - AMOUNT SPENT < $10", high_scalable_header_format)
        current_row += 1
        
        for col_num, header in enumerate(scalable_headers):
            safe_write(scalable_sheet, current_row, col_num, header, high_scalable_header_format)
        current_row += 1
        
        # Write high scalable campaigns - LOW SPEND
        if high_scalable_low_spend:
            for campaign in high_scalable_low_spend:
                safe_write(scalable_sheet, current_row, 0, campaign['Product'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 1, campaign['Campaign Name'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 2, campaign['CPP'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 3, campaign['BE'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 4, campaign['Total Amount Spent (USD)'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 5, campaign['Total Purchases'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 6, campaign['Net Profit %'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 7, campaign['Total Dates'], high_scalable_data_format)
                safe_write(scalable_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], high_scalable_data_format)
                current_row += 1
        else:
            safe_write(scalable_sheet, current_row, 0, "No campaigns found with Net Profit % > 20% and Amount Spent < $10", high_scalable_data_format)
            current_row += 1
        
        # Summary for high scalable - LOW SPEND
        current_row += 2
        safe_write(scalable_sheet, current_row, 0, "SUMMARY - HIGH SCALABLE (LOW SPEND < $10)", high_scalable_header_format)
        current_row += 1
        
        total_high_low_campaigns = len(high_scalable_low_spend)
        total_high_low_spend = sum([c['Total Amount Spent (USD)'] for c in high_scalable_low_spend])
        total_high_low_purchases = sum([c['Total Purchases'] for c in high_scalable_low_spend])
        avg_high_low_net_profit_pct = sum([c['Net Profit %'] for c in high_scalable_low_spend]) / total_high_low_campaigns if total_high_low_campaigns > 0 else 0
        
        safe_write(scalable_sheet, current_row, 0, f"Total Campaigns: {total_high_low_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"Total Amount Spent (USD): ${total_high_low_spend:,.2f}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"Total Purchases: {total_high_low_purchases:,}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"Average Net Profit %: {round(avg_high_low_net_profit_pct, 2)}%", high_scalable_data_format)
        
        # Overall summary
        current_row += 7
        safe_write(scalable_sheet, current_row, 0, "OVERALL SUMMARY - ALL SCALABLE CAMPAIGNS", scalable_header_format)
        current_row += 1
        
        total_scalable = len(scalable_campaigns)
        total_scalable_spend = sum([c['Total Amount Spent (USD)'] for c in scalable_campaigns])
        total_scalable_purchases = sum([c['Total Purchases'] for c in scalable_campaigns])
        
        # Calculate combined totals for each category
        total_moderate_campaigns = total_moderate_high_campaigns + total_moderate_low_campaigns
        total_high_campaigns = total_high_high_campaigns + total_high_low_campaigns
        
        safe_write(scalable_sheet, current_row, 0, f"Total Scalable Campaigns (Net Profit % > 10%): {total_scalable}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 1, 0, f"  • Moderate (10% < Net Profit % ≤ 20%): {total_moderate_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 2, 0, f"    - High Spend (≥ $10): {total_moderate_high_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 3, 0, f"    - Low Spend (< $10): {total_moderate_low_campaigns}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 4, 0, f"  • High (Net Profit % > 20%): {total_high_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 5, 0, f"    - High Spend (≥ $10): {total_high_high_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 6, 0, f"    - Low Spend (< $10): {total_high_low_campaigns}", high_scalable_data_format)
        safe_write(scalable_sheet, current_row + 7, 0, f"Total Amount Spent (USD): ${total_scalable_spend:,.2f}", moderate_scalable_data_format)
        safe_write(scalable_sheet, current_row + 8, 0, f"Total Purchases: {total_scalable_purchases:,}", moderate_scalable_data_format)
        
        # Set column widths for scalable campaigns sheet
        scalable_sheet.set_column(0, 0, 25)  # Product
        scalable_sheet.set_column(1, 1, 40)  # Campaign Name
        scalable_sheet.set_column(2, 2, 15)  # CPP
        scalable_sheet.set_column(3, 3, 15)  # BE
        scalable_sheet.set_column(4, 4, 25)  # Total Amount Spent (USD)
        scalable_sheet.set_column(5, 5, 18)  # Total Purchases
        scalable_sheet.set_column(6, 6, 18)  # Net Profit %
        scalable_sheet.set_column(7, 7, 15)  # Total Dates
        scalable_sheet.set_column(8, 8, 25)  # Last Date Amount Spent (USD)
    
        
   
        
    # Initialize JSON structure
    campaign_json_output = {
        "campaign_data": {
            "grand_total": None,
            "main_data": [],
            "excluded_products": []
        },
        "unmatched_campaigns": [],
        "negative_profit_campaigns": {
            "complete_analysis": [],
            "severe_negative": [],
            "moderate_negative": [],
            "positive_campaigns": [],
            "last_date_negative": []
        },
        "profit_loss_products": {
            "positive_products": [],
            "negative_products": [],
            "negative_by_ratio": {
                "moderate": [],
                "critical": []
            }
        },
        "scalable_campaigns": {
            "moderate_high_spend": [],
            "moderate_low_spend": [],
            "high_high_spend": [],
            "high_low_spend": []
        },
        "metadata": {
            "shipping_rate": shipping_rate,
            "operational_rate": operational_rate,
            "selected_days": selected_days,
            "generation_date": datetime.now().isoformat()
        }
    }
    
    if df.empty:
        return None, campaign_json_output
    
    # [Keep all your existing Excel generation code exactly as is]
    # ... [ALL EXISTING CODE FOR EXCEL GENERATION] ...
    
    # =================================================================
    # NEW: GENERATE JSON DATA FOR ALL SHEETS
    # =================================================================
    
    # Get unique dates
    has_dates = 'Date' in df.columns
    if has_dates:
        unique_dates = sorted([str(d) for d in df['Date'].unique() 
                              if pd.notna(d) and str(d).strip() != ''])
    else:
        unique_dates = []
    
    campaign_json_output["metadata"]["unique_dates"] = unique_dates
    campaign_json_output["metadata"]["total_dates"] = len(unique_dates)
    
    # Initialize lookups
    if product_date_avg_prices is None:
        product_date_avg_prices = {}
    if product_date_delivery_rates is None:
        product_date_delivery_rates = {}
    if product_date_cost_inputs is None:
        product_date_cost_inputs = {}
    
    # -----------------------------------------------------------------
    # 1. CAMPAIGN DATA JSON (Main + Excluded)
    # -----------------------------------------------------------------
    logger.info("📊 Generating Campaign Data JSON...")
    
    def calculate_all_totals(day_wise_data_dict, product, total_purchases_override=None):
        """
        Calculate ALL total column values from day-wise data
        Returns a dict with all 13 total metrics + BE
        """
        # Get product-level values
        product_avg_price = round(product_total_avg_prices.get(product, 0), 2)
        product_delivery_rate = round(product_total_delivery_rates.get(product, 0), 2)
        
        # Calculate weighted average product cost input
        total_purchases_for_cost = 0
        weighted_cost_input_sum = 0
        
        for date, day_data in day_wise_data_dict.items():
            date_purchases = day_data.get("purchases", 0)
            date_product_cost = day_data.get("product_cost_input", 0)
            
            weighted_cost_input_sum += date_product_cost * date_purchases
            total_purchases_for_cost += date_purchases
        
        total_product_cost_input = weighted_cost_input_sum / total_purchases_for_cost if total_purchases_for_cost > 0 else 0
        
        # Sum up all other metrics
        total_amount_spent = sum([d.get("amount_spent_usd", 0) for d in day_wise_data_dict.values()])
        total_purchases = sum([d.get("purchases", 0) for d in day_wise_data_dict.values()])
        total_delivered_orders = sum([d.get("delivered_orders", 0) for d in day_wise_data_dict.values()])
        total_net_revenue = sum([d.get("net_revenue", 0) for d in day_wise_data_dict.values()])
        total_product_cost = sum([d.get("total_product_cost", 0) for d in day_wise_data_dict.values()])
        total_shipping_cost = sum([d.get("total_shipping_cost", 0) for d in day_wise_data_dict.values()])
        total_operational_cost = sum([d.get("total_operational_cost", 0) for d in day_wise_data_dict.values()])
        total_net_profit = sum([d.get("net_profit", 0) for d in day_wise_data_dict.values()])
        
        # Calculate Cost Per Purchase
        if total_amount_spent > 0 and total_purchases == 0:
            total_cpp = total_amount_spent / 1
        elif total_purchases > 0:
            total_cpp = total_amount_spent / total_purchases
        else:
            total_cpp = 0
        
        # Calculate Net Profit %
        calc_purchases = total_purchases_override if total_purchases_override is not None else total_purchases
        if calc_purchases == 0 and total_amount_spent > 0:
            calc_purchases = 1
        
        delivery_rate_decimal = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
        denominator = product_avg_price * delivery_rate_decimal * calc_purchases
        total_net_profit_pct = round((total_net_profit / denominator * 100), 2) if denominator > 0 else 0
        
        return {
            "avg_price": round(float(product_avg_price), 2),
            "delivery_rate": round(float(product_delivery_rate), 2),
            "product_cost_input": round(float(total_product_cost_input), 2),
            "amount_spent_usd": round(float(total_amount_spent), 2),
            "purchases": int(total_purchases),
            "cost_per_purchase_usd": round(float(total_cpp), 2),
            "delivered_orders": round(float(total_delivered_orders), 2),
            "net_revenue": round(float(total_net_revenue), 2),
            "total_product_cost": round(float(total_product_cost), 2),
            "total_shipping_cost": round(float(total_shipping_cost), 2),
            "total_operational_cost": round(float(total_operational_cost), 2),
            "net_profit": round(float(total_net_profit), 2),
            "net_profit_pct": round(float(total_net_profit_pct), 2)
        }

    
    
    # -----------------------------------------------------------------
    # STEP 1: Determine valid vs excluded products
    # -----------------------------------------------------------------
    valid_product_names = set()
    excluded_products_list = []
    
    for product, product_df in df.groupby("Product"):
        has_valid_cost = False
        has_valid_delivery_rate = False
        
        for date in unique_dates:
            date_cost = safe_lookup_get(product_date_cost_inputs, product, 0.0)
            date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
            
            if date_cost > 0:
                has_valid_cost = True
            if date_delivery_rate > 0:
                has_valid_delivery_rate = True
        
        if not has_valid_cost and not has_valid_delivery_rate:
            # Excluded product
            total_amount_spent = product_df["Amount Spent (USD)"].sum()
            total_purchases = product_df["Purchases"].sum()
            campaign_count = len(product_df.groupby("Campaign Name"))
            
            excluded_products_list.append({
                "product": str(product),
                "campaign_count": campaign_count,
                "total_amount_spent_usd": round(float(total_amount_spent), 2),
                "total_purchases": int(total_purchases),
                "reason": "Product cost input = 0 and delivery rate = 0"
            })
        else:
            valid_product_names.add(product)
    
    campaign_json_output["campaign_data"]["excluded_products"] = excluded_products_list
    
    # -----------------------------------------------------------------
    # STEP 2: Calculate product-level aggregated values (for totals)
    # -----------------------------------------------------------------
    product_total_delivery_rates = {}
    product_total_avg_prices = {}
    
    for product in valid_product_names:
        product_df = df[df['Product'] == product]
        
        # Calculate weighted average delivery rate
        total_purchases_delivery = 0
        weighted_delivery_rate_sum = 0
        
        # Calculate weighted average price
        total_purchases_price = 0
        weighted_avg_price_sum = 0
        
        for date in unique_dates:
            date_delivery_rate = safe_lookup_get(product_date_delivery_rates, product, 0.0)
            date_avg_price = safe_lookup_get(product_date_avg_prices, product, 0.0)
            date_purchases = product_df[product_df['Date'].astype(str) == date]['Purchases'].sum() if 'Purchases' in product_df.columns else 0
            
            total_purchases_delivery += date_purchases
            weighted_delivery_rate_sum += date_delivery_rate * date_purchases
            
            total_purchases_price += date_purchases
            weighted_avg_price_sum += date_avg_price * date_purchases
        
        product_total_delivery_rates[product] = weighted_delivery_rate_sum / total_purchases_delivery if total_purchases_delivery > 0 else 0
        product_total_avg_prices[product] = weighted_avg_price_sum / total_purchases_price if total_purchases_price > 0 else 0
    
    # -----------------------------------------------------------------
    # STEP 3: Build complete data structure with hierarchy
    # -----------------------------------------------------------------
    
    # Sort products by total purchases (highest first)
    product_purchase_totals = []
    for product in valid_product_names:
        product_df = df[df['Product'] == product]
        total_purchases = product_df.get("Purchases", 0).sum() if "Purchases" in product_df.columns else 0
        product_purchase_totals.append((product, product_df, total_purchases))
    
    product_purchase_totals.sort(key=lambda x: x[2], reverse=True)
    
    # Build products array with complete hierarchy
    products_array = []
    
    for product, product_df, _ in product_purchase_totals:
        product_json = {
            "product_name": str(product),
            "product_total": None,  # Will be filled
            "campaigns": []         # Will be filled
        }
        
        # Group and sort campaigns by CPP
        campaign_groups = []
        for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
            total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum()
            total_purchases = campaign_group.get("Purchases", 0).sum()
            cpp = total_amount_spent_usd / max(total_purchases, 1) if total_amount_spent_usd > 0 else 0
            campaign_groups.append((cpp, campaign_name, campaign_group))
        
        campaign_groups.sort(key=lambda x: x[0])
        
        # Aggregate day-wise data for ALL campaigns (for product total)
        product_day_wise_aggregate = {}
        
        # Process each campaign
        for cpp, campaign_name, campaign_group in campaign_groups:
            campaign_json = {
                "campaign_name": str(campaign_name),
                "cpp": round(float(cpp), 2),
                "be": 0.0,  # Will be set later
                "day_wise_data": {}
            }
            
            # Day-wise data for each date
            for date in unique_dates:
                date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                
                if not date_data.empty:
                    row_data = date_data.iloc[0]
                    
                    # Get values
                    amount_spent = round(float(row_data.get("Amount Spent (USD)", 0) or 0), 2)
                    purchases = int(row_data.get("Purchases", 0) or 0)
                    
                    # Get day-wise lookup data
                    date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                    date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                    date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
                    
                    # Calculate metrics
                    delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                    delivered_orders = round(purchases * delivery_rate, 2)
                    net_revenue = round(delivered_orders * date_avg_price, 2)
                    total_product_cost = round(delivered_orders * date_product_cost, 2)
                    total_shipping_cost = round(purchases * shipping_rate, 2)
                    total_operational_cost = round(purchases * operational_rate, 2)
                    net_profit = round(net_revenue - (amount_spent * 100) - 
                                     total_shipping_cost - total_operational_cost - total_product_cost, 2)
                    
                    # Net profit percentage
                    purchases_for_denominator = max(purchases, 1) if amount_spent > 0 else purchases
                    denominator = date_avg_price * delivery_rate * purchases_for_denominator
                    net_profit_pct = round((net_profit / denominator * 100), 2) if denominator > 0 else 0
                    
                    day_data_json = {
                        "avg_price": date_avg_price,
                        "delivery_rate": date_delivery_rate,
                        "product_cost_input": date_product_cost,
                        "amount_spent_usd": amount_spent,
                        "purchases": purchases,
                        "delivered_orders": delivered_orders,
                        "net_revenue": net_revenue,
                        "total_product_cost": total_product_cost,
                        "total_shipping_cost": total_shipping_cost,
                        "total_operational_cost": total_operational_cost,
                        "net_profit": net_profit,
                        "net_profit_pct": net_profit_pct
                    }
                    
                    campaign_json["day_wise_data"][date] = day_data_json
                    
                    # Aggregate for product total
                    if date not in product_day_wise_aggregate:
                        product_day_wise_aggregate[date] = {
                            "avg_price": date_avg_price,
                            "delivery_rate": date_delivery_rate,
                            "product_cost_input": date_product_cost,
                            "amount_spent_usd": 0,
                            "purchases": 0,
                            "delivered_orders": 0,
                            "net_revenue": 0,
                            "total_product_cost": 0,
                            "total_shipping_cost": 0,
                            "total_operational_cost": 0,
                            "net_profit": 0
                        }
                    
                    # Add to aggregate
                    product_day_wise_aggregate[date]["amount_spent_usd"] += amount_spent
                    product_day_wise_aggregate[date]["purchases"] += purchases
                    product_day_wise_aggregate[date]["delivered_orders"] += delivered_orders
                    product_day_wise_aggregate[date]["net_revenue"] += net_revenue
                    product_day_wise_aggregate[date]["total_product_cost"] += total_product_cost
                    product_day_wise_aggregate[date]["total_shipping_cost"] += total_shipping_cost
                    product_day_wise_aggregate[date]["total_operational_cost"] += total_operational_cost
                    product_day_wise_aggregate[date]["net_profit"] += net_profit
            
            # Calculate ALL totals for this campaign using helper function
            campaign_totals = calculate_all_totals(campaign_json["day_wise_data"], product)
            campaign_json["totals"] = campaign_totals
            
            product_json["campaigns"].append(campaign_json)
        
        # Calculate net profit % for each date in product aggregate
        for date, day_agg in product_day_wise_aggregate.items():
            date_avg_price = day_agg["avg_price"]
            date_delivery_rate = day_agg["delivery_rate"]
            date_amount_spent = day_agg["amount_spent_usd"]
            date_purchases = day_agg["purchases"]
            date_net_profit = day_agg["net_profit"]
            
            purchases_for_denom = max(date_purchases, 1) if date_amount_spent > 0 else date_purchases
            delivery_rate_decimal = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
            denominator = date_avg_price * delivery_rate_decimal * purchases_for_denom
            date_net_profit_pct = round((date_net_profit / denominator * 100), 2) if denominator > 0 else 0
            
            day_agg["net_profit_pct"] = date_net_profit_pct
        
        # Calculate ALL totals for product using helper function
        product_totals = calculate_all_totals(product_day_wise_aggregate, product)
        
        # Calculate BE for product
        total_net_revenue = product_totals["net_revenue"]
        total_shipping_cost = product_totals["total_shipping_cost"]
        total_operational_cost = product_totals["total_operational_cost"]
        total_product_cost = product_totals["total_product_cost"]
        total_purchases = product_totals["purchases"]
        
        be = 0
        if total_net_revenue > 0 and total_purchases > 0:
            be = (total_net_revenue - total_shipping_cost - total_operational_cost - total_product_cost) / 100 / total_purchases
        
        product_totals["be"] = round(float(be), 2)
        
        # Set BE for all campaigns under this product
        for campaign in product_json["campaigns"]:
            campaign["be"] = round(float(be), 2)
        
        # Add product total with day-wise data
        product_json["product_total"] = {
            "day_wise_data": product_day_wise_aggregate,
            "totals": product_totals
        }
        
        products_array.append(product_json)
    
    campaign_json_output["campaign_data"]["products"] = products_array
    
    # -----------------------------------------------------------------
    # STEP 4: Calculate GRAND TOTAL (ALL VALID PRODUCTS)
    # -----------------------------------------------------------------
    logger.info("📊 Calculating Grand Total...")
    
    grand_total_day_wise = {}
    
    # Aggregate all product day-wise data
    for product_json in products_array:
        product_day_wise = product_json["product_total"]["day_wise_data"]
        
        for date, day_data in product_day_wise.items():
            if date not in grand_total_day_wise:
                grand_total_day_wise[date] = {
                    "amount_spent_usd": 0,
                    "purchases": 0,
                    "delivered_orders": 0,
                    "net_revenue": 0,
                    "total_product_cost": 0,
                    "total_shipping_cost": 0,
                    "total_operational_cost": 0,
                    "net_profit": 0
                }
            
            # Add up all additive metrics
            grand_total_day_wise[date]["amount_spent_usd"] += day_data.get("amount_spent_usd", 0)
            grand_total_day_wise[date]["purchases"] += day_data.get("purchases", 0)
            grand_total_day_wise[date]["delivered_orders"] += day_data.get("delivered_orders", 0)
            grand_total_day_wise[date]["net_revenue"] += day_data.get("net_revenue", 0)
            grand_total_day_wise[date]["total_product_cost"] += day_data.get("total_product_cost", 0)
            grand_total_day_wise[date]["total_shipping_cost"] += day_data.get("total_shipping_cost", 0)
            grand_total_day_wise[date]["total_operational_cost"] += day_data.get("total_operational_cost", 0)
            grand_total_day_wise[date]["net_profit"] += day_data.get("net_profit", 0)
    
    # Calculate weighted averages for grand total
    for date in grand_total_day_wise.keys():
        total_purchases_for_date = 0
        weighted_avg_price_sum = 0
        weighted_delivery_rate_sum = 0
        weighted_cost_input_sum = 0
        
        for product_json in products_array:
            product_day_data = product_json["product_total"]["day_wise_data"].get(date, {})
            date_purchases = product_day_data.get("purchases", 0)
            
            if date_purchases > 0:
                total_purchases_for_date += date_purchases
                weighted_avg_price_sum += product_day_data.get("avg_price", 0) * date_purchases
                weighted_delivery_rate_sum += product_day_data.get("delivery_rate", 0) * date_purchases
                weighted_cost_input_sum += product_day_data.get("product_cost_input", 0) * date_purchases
        
        if total_purchases_for_date > 0:
            grand_total_day_wise[date]["avg_price"] = round(weighted_avg_price_sum / total_purchases_for_date, 2)
            grand_total_day_wise[date]["delivery_rate"] = round(weighted_delivery_rate_sum / total_purchases_for_date, 2)
            grand_total_day_wise[date]["product_cost_input"] = round(weighted_cost_input_sum / total_purchases_for_date, 2)
        else:
            grand_total_day_wise[date]["avg_price"] = 0
            grand_total_day_wise[date]["delivery_rate"] = 0
            grand_total_day_wise[date]["product_cost_input"] = 0
        
        # Calculate net profit % for this date
        date_avg_price = grand_total_day_wise[date]["avg_price"]
        date_delivery_rate = grand_total_day_wise[date]["delivery_rate"]
        date_amount_spent = grand_total_day_wise[date]["amount_spent_usd"]
        date_purchases = grand_total_day_wise[date]["purchases"]
        date_net_profit = grand_total_day_wise[date]["net_profit"]
        
        purchases_for_denom = max(date_purchases, 1) if date_amount_spent > 0 else date_purchases
        delivery_rate_decimal = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
        denominator = date_avg_price * delivery_rate_decimal * purchases_for_denom
        date_net_profit_pct = round((date_net_profit / denominator * 100), 2) if denominator > 0 else 0
        
        grand_total_day_wise[date]["net_profit_pct"] = date_net_profit_pct
    
    # Calculate grand total ALL totals
    grand_totals = {}
    
    # Calculate weighted averages for totals
    total_purchases_all = sum([d.get("purchases", 0) for d in grand_total_day_wise.values()])
    weighted_avg_price_sum_all = 0
    weighted_delivery_rate_sum_all = 0
    weighted_cost_input_sum_all = 0
    
    for date, day_data in grand_total_day_wise.items():
        date_purchases = day_data.get("purchases", 0)
        weighted_avg_price_sum_all += day_data.get("avg_price", 0) * date_purchases
        weighted_delivery_rate_sum_all += day_data.get("delivery_rate", 0) * date_purchases
        weighted_cost_input_sum_all += day_data.get("product_cost_input", 0) * date_purchases
    
    if total_purchases_all > 0:
        grand_totals["avg_price"] = round(weighted_avg_price_sum_all / total_purchases_all, 2)
        grand_totals["delivery_rate"] = round(weighted_delivery_rate_sum_all / total_purchases_all, 2)
        grand_totals["product_cost_input"] = round(weighted_cost_input_sum_all / total_purchases_all, 2)
    else:
        grand_totals["avg_price"] = 0
        grand_totals["delivery_rate"] = 0
        grand_totals["product_cost_input"] = 0
    
    # Sum up all other metrics
    grand_totals["amount_spent_usd"] = round(sum([d.get("amount_spent_usd", 0) for d in grand_total_day_wise.values()]), 2)
    grand_totals["purchases"] = int(sum([d.get("purchases", 0) for d in grand_total_day_wise.values()]))
    grand_totals["delivered_orders"] = round(sum([d.get("delivered_orders", 0) for d in grand_total_day_wise.values()]), 2)
    grand_totals["net_revenue"] = round(sum([d.get("net_revenue", 0) for d in grand_total_day_wise.values()]), 2)
    grand_totals["total_product_cost"] = round(sum([d.get("total_product_cost", 0) for d in grand_total_day_wise.values()]), 2)
    grand_totals["total_shipping_cost"] = round(sum([d.get("total_shipping_cost", 0) for d in grand_total_day_wise.values()]), 2)
    grand_totals["total_operational_cost"] = round(sum([d.get("total_operational_cost", 0) for d in grand_total_day_wise.values()]), 2)
    grand_totals["net_profit"] = round(sum([d.get("net_profit", 0) for d in grand_total_day_wise.values()]), 2)
    
    # Calculate CPP
    if grand_totals["amount_spent_usd"] > 0 and grand_totals["purchases"] == 0:
        grand_totals["cost_per_purchase_usd"] = round(grand_totals["amount_spent_usd"] / 1, 2)
    elif grand_totals["purchases"] > 0:
        grand_totals["cost_per_purchase_usd"] = round(grand_totals["amount_spent_usd"] / grand_totals["purchases"], 2)
    else:
        grand_totals["cost_per_purchase_usd"] = 0
    
    # Calculate Net Profit %
    calc_purchases_grand = grand_totals["purchases"]
    if calc_purchases_grand == 0 and grand_totals["amount_spent_usd"] > 0:
        calc_purchases_grand = 1
    
    delivery_rate_decimal_grand = grand_totals["delivery_rate"] / 100 if grand_totals["delivery_rate"] > 1 else grand_totals["delivery_rate"]
    denominator_grand = grand_totals["avg_price"] * delivery_rate_decimal_grand * calc_purchases_grand
    grand_totals["net_profit_pct"] = round((grand_totals["net_profit"] / denominator_grand * 100), 2) if denominator_grand > 0 else 0
    
    # Calculate BE for grand total
    grand_be = 0
    if grand_totals["net_revenue"] > 0 and grand_totals["purchases"] > 0:
        grand_be = (grand_totals["net_revenue"] - grand_totals["total_shipping_cost"] - 
                    grand_totals["total_operational_cost"] - grand_totals["total_product_cost"]) / 100 / grand_totals["purchases"]
    
    grand_totals["be"] = round(float(grand_be), 2)
    
    campaign_json_output["campaign_data"]["grand_total"] = {
        "day_wise_data": grand_total_day_wise,
        "totals": grand_totals
    }
    
    logger.info(f"✅ Complete Campaign JSON generation finished!")
    logger.info(f"   - Products: {len(products_array)}")
    logger.info(f"   - Total campaigns: {sum(len(p['campaigns']) for p in products_array)}")
    logger.info(f"   - Grand total calculated with {len(grand_total_day_wise)} dates")
    
    
    # -----------------------------------------------------------------
    # 2. UNMATCHED CAMPAIGNS JSON
    # -----------------------------------------------------------------
    logger.info("📊 Generating Unmatched Campaigns JSON...")
    
    matched_campaigns = []
    unmatched_campaigns = []
    
    for product, product_df in df.groupby("Product"):
        has_shopify_data = (
            (product in product_date_avg_prices and product_date_avg_prices[product] > 0) or
            (product in product_date_delivery_rates and product_date_delivery_rates[product] > 0) or
            (product in product_date_cost_inputs and product_date_cost_inputs[product] > 0)
        )
        
        for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
            total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum()
            total_purchases = campaign_group.get("Purchases", 0).sum()
            
            campaign_info = {
                "product": str(product),
                "campaign_name": str(campaign_name),
                "amount_spent_usd": round(float(total_amount_spent_usd), 2),
                "purchases": int(total_purchases),
                "has_shopify_data": has_shopify_data,
                "dates": sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d)])
            }
            
            if not has_shopify_data:
                unmatched_campaigns.append(campaign_info)
    
    campaign_json_output["unmatched_campaigns"] = unmatched_campaigns
    
    # -----------------------------------------------------------------
    # 3. NEGATIVE NET PROFIT CAMPAIGNS JSON
    # -----------------------------------------------------------------
    logger.info("📊 Generating Negative Net Profit Campaigns JSON...")
    
    # Build complete analysis data (same as Excel)
    all_campaigns_complete_analysis = []
    
    for product, product_df in df.groupby("Product"):
        if product not in valid_product_names:
            continue
        
        for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
            # Calculate day-wise metrics
            day_wise_metrics = {}
            
            for date in unique_dates:
                date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                if not date_data.empty:
                    row_data = date_data.iloc[0]
                    
                    # [Calculate all metrics - same as Excel generation]
                    amount_spent = round(float(row_data.get("Amount Spent (USD)", 0) or 0), 2)
                    purchases = round(float(row_data.get("Purchases", 0) or 0), 2)
                    
                    date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                    date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                    date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
                    
                    delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                    delivered_orders = round(purchases * delivery_rate, 2)
                    net_revenue = round(delivered_orders * date_avg_price, 2)
                    total_product_cost = round(delivered_orders * date_product_cost, 2)
                    total_shipping_cost = round(purchases * shipping_rate, 2)
                    total_operational_cost = round(purchases * operational_rate, 2)
                    net_profit = round(net_revenue - (amount_spent * 100) - 
                                     total_shipping_cost - total_operational_cost - total_product_cost, 2)
                    
                    purchases_for_denominator = max(purchases, 1) if amount_spent > 0 else purchases
                    denominator = date_avg_price * delivery_rate * purchases_for_denominator
                    day_net_profit_pct = round((net_profit / denominator * 100), 2) if denominator > 0 else 0
                    
                    day_wise_metrics[date] = {
                        "net_profit_pct": day_net_profit_pct,
                        "net_profit": net_profit,
                        "amount_spent": amount_spent,
                        "purchases": purchases
                    }
            
            # Calculate total net profit percentage
            total_net_profit_sum = sum([d["net_profit"] for d in day_wise_metrics.values()])
            total_purchases = sum([d["purchases"] for d in day_wise_metrics.values()])
            total_amount_spent = sum([d["amount_spent"] for d in day_wise_metrics.values()])
            
            product_avg_price = round(float(product_date_avg_prices.get(product, 0)), 2)
            product_delivery_rate = round(float(product_date_delivery_rates.get(product, 0)), 2)
            
            calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent > 0) else total_purchases
            delivery_rate_total = round(product_delivery_rate / 100 if product_delivery_rate > 1 
                                      else product_delivery_rate, 2)
            
            numerator_total = round(total_net_profit_sum, 2)
            denominator_total = round(product_avg_price * calc_purchases_total * delivery_rate_total, 2)
            total_net_profit_pct = round((numerator_total / denominator_total * 100), 2) \
                if denominator_total > 0 else 0
            
            campaign_analysis = {
                "product": str(product),
                "campaign_name": str(campaign_name),
                "day_wise_data": day_wise_metrics,
                "total_net_profit_pct": total_net_profit_pct,
                "total_amount_spent_usd": round(float(total_amount_spent), 2),
                "total_purchases": int(total_purchases)
            }
            
            all_campaigns_complete_analysis.append(campaign_analysis)
    
    campaign_json_output["negative_profit_campaigns"]["complete_analysis"] = all_campaigns_complete_analysis
    
    # Filter into categories
    if selected_days:
        for campaign in all_campaigns_complete_analysis:
            negative_days = sum(1 for d in campaign["day_wise_data"].values() if d["net_profit_pct"] < 0)
            
            if selected_days is None or selected_days == 0 or negative_days >= selected_days:
                  if campaign["total_net_profit_pct"] <= -10:
                     campaign_json_output["negative_profit_campaigns"]["severe_negative"].append(campaign)
                  elif -10 < campaign["total_net_profit_pct"] < 0:
                     campaign_json_output["negative_profit_campaigns"]["moderate_negative"].append(campaign)
                  elif campaign["total_net_profit_pct"] >= 0:
                     campaign_json_output["negative_profit_campaigns"]["positive_campaigns"].append(campaign)
    # -----------------------------------------------------------------
    # 4. PROFIT AND LOSS PRODUCTS JSON
    # -----------------------------------------------------------------
    logger.info("📊 Generating Profit and Loss Products JSON...")
    
    # Calculate product net profits (from your existing logic)
    product_net_profit_values = {}  # This should be calculated in your existing code
    
    for product, product_df in df.groupby("Product"):
        if product not in valid_product_names:
            continue
        
        total_net_profit = 0
        for date in unique_dates:
            date_data = product_df[product_df['Date'].astype(str) == date]
            for _, campaign_row in date_data.iterrows():
                # [Calculate net profit - same as Excel]
                date_purchases = round(campaign_row.get('Purchases', 0), 2)
                date_amount_spent = round(campaign_row.get("Amount Spent (USD)", 0), 2)
                
                date_avg_price = round(safe_lookup_get(product_date_avg_prices, product, 0.0), 2)
                date_delivery_rate = round(safe_lookup_get(product_date_delivery_rates, product, 0.0), 2)
                date_product_cost = round(safe_lookup_get(product_date_cost_inputs, product, 0.0), 2)
                
                delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                delivered_orders = round(date_purchases * delivery_rate, 2)
                net_revenue = round(delivered_orders * date_avg_price, 2)
                product_cost = round(delivered_orders * date_product_cost, 2)
                shipping_cost = round(date_purchases * shipping_rate, 2)
                operational_cost = round(date_purchases * operational_rate, 2)
                
                campaign_net_profit = round(net_revenue - (date_amount_spent * 100) - 
                                          shipping_cost - operational_cost - product_cost, 2)
                total_net_profit += campaign_net_profit
        
        product_net_profit_values[product] = round(total_net_profit, 2)
    
    # Split into positive and negative
    # Add profit/loss products WITH CPP, BE, and Total Net Profit %
    for product, net_profit in product_net_profit_values.items():
    # Calculate product totals
        product_df = df_main[df_main['Product'] == product]
        total_amount_spent = product_df["Amount Spent (USD)"].sum()
        total_purchases = product_df["Purchases"].sum()
    
    # Calculate CPP
        cpp = total_amount_spent / max(total_purchases, 1) if total_amount_spent > 0 else 0
    
    # Get BE from stored values
        be = product_be_values.get(product, 0)
    
    # Calculate Total Net Profit %
        product_avg_price = product_total_avg_prices.get(product, 0)
        product_delivery_rate = product_total_delivery_rates.get(product, 0)
    
        calc_purchases = 1 if (total_purchases == 0 and total_amount_spent > 0) else total_purchases
        delivery_rate_decimal = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
    
        denominator = product_avg_price * calc_purchases * delivery_rate_decimal
        total_net_profit_pct = round((net_profit / denominator * 100), 2) if denominator > 0 else 0
    
        product_data = {
         "product": str(product),
         "cpp": round(float(cpp), 2),
         "be": round(float(be), 2),
         "total_net_profit_pct": round(float(total_net_profit_pct), 2),
         "total_net_profit": float(round(net_profit, 2))
         }
        if net_profit >= 0:
            campaign_json_output["profit_loss_products"]["positive_products"].append(product_data)
        else:
            campaign_json_output["profit_loss_products"]["negative_products"].append(product_data)
            
            # Calculate ratio for negative products
            # [Your existing ratio calculation logic]
    
    # -----------------------------------------------------------------
    # 5. SCALABLE CAMPAIGNS JSON
    # -----------------------------------------------------------------
    logger.info("📊 Generating Scalable Campaigns JSON...")
    
    scalable_campaigns = []
    
    for campaign in all_campaigns_complete_analysis:
        if campaign["total_net_profit_pct"] > 10:
            scalable_campaign = {
                "product": campaign["product"],
                "campaign_name": campaign["campaign_name"],
                "total_net_profit_pct": campaign["total_net_profit_pct"],
                "total_amount_spent_usd": campaign["total_amount_spent_usd"],
                "total_purchases": campaign["total_purchases"]
            }
            
            # Categorize
            if 10 < campaign["total_net_profit_pct"] <= 20:
                if campaign["total_amount_spent_usd"] >= 10:
                    campaign_json_output["scalable_campaigns"]["moderate_high_spend"].append(scalable_campaign)
                else:
                    campaign_json_output["scalable_campaigns"]["moderate_low_spend"].append(scalable_campaign)
            elif campaign["total_net_profit_pct"] > 20:
                if campaign["total_amount_spent_usd"] >= 10:
                    campaign_json_output["scalable_campaigns"]["high_high_spend"].append(scalable_campaign)
                else:
                    campaign_json_output["scalable_campaigns"]["high_low_spend"].append(scalable_campaign)
    
    logger.info(f"✅ Campaign JSON generation complete!")
    logger.info(f"   - Main campaigns: {len(campaign_json_output['campaign_data']['main_data'])}")
    logger.info(f"   - Excluded products: {len(campaign_json_output['campaign_data']['excluded_products'])}")
    logger.info(f"   - Unmatched campaigns: {len(campaign_json_output['unmatched_campaigns'])}")
    logger.info(f"   - Negative profit campaigns: {len(campaign_json_output['negative_profit_campaigns']['complete_analysis'])}")
    logger.info(f"   - Scalable campaigns: {len(campaign_json_output['scalable_campaigns']['moderate_high_spend']) + len(campaign_json_output['scalable_campaigns']['high_high_spend'])}")
    
    # Return both Excel bytes and JSON
    return output.getvalue(), campaign_json_output


# main.py (continued from where we left off)

# Copy your convert_shopify_to_excel and convert_shopify_to_excel_with_date_columns_fixed functions AS-IS here
# Copy your convert_final_campaign_to_excel and convert_final_campaign_to_excel_with_date_columns_fixed functions AS-IS here
# (These functions don't need any changes - just copy them)
processed_files: Dict[str, bytes] = {}
# Now, here's the main API endpoint with all the processing logic:

def format_unmatched_products_for_response(unmatched_products: list, database_df: pd.DataFrame, store_name: str):
    """
    Format unmatched products with complete information from database
    Returns UNIQUE list with: Product Title, Product Variant Title, Delivery Rate, Product Cost (Input), Store name
    """
    # ========== STEP 1: CREATE DATABASE LOOKUP ==========
    db_lookup = {}
    if not database_df.empty:
        for _, row in database_df.iterrows():
            key = (
                str(row.get('Product Title', '')).strip().lower(),
                str(row.get('Product Variant Title', '')).strip().lower()
            )
            
            # Get delivery rate
            delivery_rate = row.get('Delivery Rate', 0)
            if pd.isna(delivery_rate) or delivery_rate == '':
                delivery_rate = 0
            else:
                try:
                    delivery_rate = float(delivery_rate)
                except:
                    delivery_rate = 0
            
            # Get product cost - try both possible column names
            product_cost = row.get('Product Cost (Input)', row.get('Product Cost Input', 0))
            if pd.isna(product_cost) or product_cost == '':
                product_cost = 0
            else:
                try:
                    product_cost = float(product_cost)
                except:
                    product_cost = 0
            
            db_lookup[key] = {
                'delivery_rate': delivery_rate,
                'product_cost': product_cost
            }
    
    # ========== STEP 2: DEDUPLICATE UNMATCHED PRODUCTS ==========
    seen = set()
    unique_unmatched = []
    
    for product in unmatched_products:
        # Create unique key from product title + variant title
        key = (
            str(product.get('product_title', '')).strip().lower(),
            str(product.get('variant_title', '')).strip().lower()
        )
        
        # Only process if we haven't seen this combination before
        if key not in seen:
            seen.add(key)
            unique_unmatched.append(product)
    
    logger.info(f"🔍 Deduplication: {len(unmatched_products)} total → {len(unique_unmatched)} unique unmatched products")
    
    # ========== STEP 3: FORMAT UNIQUE PRODUCTS ==========
    formatted_products = []
    
    for product in unique_unmatched:
        product_key = (
            str(product.get('product_title', '')).strip().lower(),
            str(product.get('variant_title', '')).strip().lower()
        )
        
        # Get current values from database (if they exist)
        db_values = db_lookup.get(product_key, {'delivery_rate': 0, 'product_cost': 0})
        
        # Determine status
        if db_values['delivery_rate'] == 0 and db_values['product_cost'] == 0:
            status = 'Pending - Both Missing'
        elif db_values['delivery_rate'] == 0:
            status = 'Pending - Delivery Rate Missing'
        elif db_values['product_cost'] == 0:
            status = 'Pending - Product Cost Missing'
        else:
            status = 'Complete'
        
        formatted_product = {
            'Product Title': product.get('product_title', ''),
            'Product Variant Title': product.get('variant_title', ''),
            'Delivery Rate': db_values['delivery_rate'],
            'Product Cost (Input)': db_values['product_cost'],
            'Store name': store_name,
            'Status': status
        }
        
        formatted_products.append(formatted_product)
    
    logger.info(f"✅ Formatted {len(formatted_products)} UNIQUE unmatched products for response")
    return formatted_products


@app.post("/api/process-files")
async def process_files(
    campaign_files: Optional[List[UploadFile]] = File(None),
    shopify_files: Optional[List[UploadFile]] = File(None),
    reference_files: Optional[List[UploadFile]] = File(None),
    product_data_json: Optional[str] = Form(
        None,
        description="JSON array of products from database. Format: [{\"Product Title\":\"...\",\"Product Variant Title\":\"...\",\"Delivery Rate\":0,\"Product Cost (Input)\":0,\"Store name\":\"...\",\"Status\":\"...\"}]",
        example='[{"Product Title":"Stainless Steel Food Tray","Product Variant Title":"2 piece - 499 Rs","Delivery Rate":77.57,"Product Cost (Input)":75.91,"Store name":"HCC","Status":"Complete"}]'
    ),
    shipping_rate: int = Form(77),
    operational_rate: int = Form(65),
    selected_days: Optional[int] = Form(None)
):
    try:
        global SHIPPING_RATE, OPERATIONAL_RATE, processed_files
        SHIPPING_RATE = shipping_rate
        OPERATIONAL_RATE = operational_rate
        messages = []
        
        # ==================== STEP 1: EXTRACT AND VALIDATE STORE NAME ====================
        all_filenames = []
        if campaign_files:
            all_filenames.extend([f.filename for f in campaign_files])
        if shopify_files:
            all_filenames.extend([f.filename for f in shopify_files])
        
        if not all_filenames:
            raise HTTPException(status_code=400, detail="No files uploaded")
        
        # Extract and validate store name consistency
        store_name = validate_store_consistency(all_filenames)
        messages.append(f"✓ Extracted store name: {store_name}")
        messages.append(f"✓ All files are from the same store")
        
        # ==================== STEP 2: READ UPLOADED FILES ====================
        campaign_data = []
        if campaign_files:
            for file in campaign_files:
                content = await file.read()
                if file.filename.endswith('.csv'):
                    df = pd.read_csv(BytesIO(content))
                else:
                    df = pd.read_excel(BytesIO(content))
                campaign_data.append({'filename': file.filename, 'data': df})
        
        shopify_data = []
        if shopify_files:
            for file in shopify_files:
                content = await file.read()
                if file.filename.endswith('.csv'):
                    df = pd.read_csv(BytesIO(content))
                else:
                    df = pd.read_excel(BytesIO(content))
                shopify_data.append({'filename': file.filename, 'data': df})
        
        reference_data = []
        if reference_files:
            for file in reference_files:
                content = await file.read()
                if file.filename.endswith('.csv'):
                    df = pd.read_csv(BytesIO(content))
                else:
                    df = pd.read_excel(BytesIO(content))
                reference_data.append({'filename': file.filename, 'data': df})
        
        # ==================== STEP 3: PARSE PRODUCT DATA JSON (REPLACES GOOGLE SHEETS) ====================
        # ==================== STEP 3: PARSE PRODUCT DATA JSON (REPLACES GOOGLE SHEETS) ====================
        try:
            if product_data_json:
                logger.info(f"📦 RECEIVED product_data_json: {len(product_data_json)} characters")
                logger.info(f"📦 First 200 chars: {product_data_json[:200]}")
                
                database_df = parse_product_data_json(product_data_json)
                
                logger.info(f"📊 Parsed DataFrame shape: {database_df.shape}")
                logger.info(f"📊 DataFrame columns: {database_df.columns.tolist()}")
                
                # Filter by store name
                if not database_df.empty:
                    logger.info(f"📊 First few rows:\n{database_df.head()}")
                    
                    # Find Store name column (case-insensitive)
                    store_col = None
                    for col in database_df.columns:
                        if col.lower().replace(' ', '') == 'storename':
                            store_col = col
                            break
                    
                    if store_col:
                        logger.info(f"✓ Found store column: {store_col}")
                        database_df['Store Name_norm'] = database_df[store_col].astype(str).str.strip().str.lower()
                        store_name_norm = store_name.strip().lower()
                        
                        logger.info(f"🔍 Filtering by store: '{store_name_norm}'")
                        logger.info(f"🔍 Unique stores in database: {database_df['Store Name_norm'].unique().tolist()}")
                        
                        database_df = database_df[database_df['Store Name_norm'] == store_name_norm]
                        
                        logger.info(f"✓ After filtering: {len(database_df)} products for store: {store_name}")
                        messages.append(f"✓ Loaded {len(database_df)} products from JSON for store: {store_name}")
                    else:
                        logger.warning("⚠ No 'Store name' column found in JSON")
                        messages.append(f"✓ Loaded {len(database_df)} products from JSON")
                else:
                    logger.warning("⚠ DataFrame is empty after parsing")
                    messages.append(f"⚠ Product data JSON is empty - all products will be unmatched")
            else:
                logger.warning("⚠ No product_data_json received")
                database_df = pd.DataFrame()
                messages.append(f"⚠ No product data JSON provided - all products will be unmatched")
                
        except Exception as e:
            logger.error(f"❌ Error parsing product data JSON: {str(e)}", exc_info=True)
            messages.append(f"⚠ Error parsing product data JSON: {str(e)}")
            database_df = pd.DataFrame()  # Empty if parsing fails
        
        # Initialize tracking variables
        matched_count = 0
        unmatched_products = []
        
        # ==================== STEP 4: PROCESS REFERENCE FILES (IF PROVIDED) ====================
        df_old_merged = None
        if reference_data:
            df_old_merged, ref_messages = merge_reference_files(reference_data)
            messages.extend(ref_messages)
        
        # State variables
        df_campaign, df_shopify = None, None
        df_final_campaign = None
        product_date_avg_prices = {}
        product_date_delivery_rates = {}
        product_date_cost_inputs = {}
        grouped_campaign = None
        
        # ==================== STEP 5: PROCESS CAMPAIGN FILES ====================
        if campaign_data:
            df_campaign, campaign_messages = merge_campaign_files(campaign_data)
            messages.extend(campaign_messages)
            
            if df_campaign is not None:
                df_campaign["Product Name"] = df_campaign["Campaign name"].astype(str).apply(clean_product_name)
                
                unique_names = df_campaign["Product Name"].unique().tolist()
                mapping = {}
                for name in unique_names:
                    if name in mapping:
                        continue
                    result = process.extractOne(name, mapping.keys(), scorer=fuzz.token_sort_ratio, score_cutoff=85)
                    if result:
                        mapping[name] = mapping[result[0]]
                    else:
                        mapping[name] = name
                df_campaign["Canonical Product"] = df_campaign["Product Name"].map(mapping)
                
                grouped_campaign = (
                    df_campaign.groupby("Canonical Product", as_index=False)
                    .agg({"Amount spent (USD)": "sum"})
                )
                grouped_campaign["Amount spent (INR)"] = grouped_campaign["Amount spent (USD)"] * 100
                grouped_campaign = grouped_campaign.rename(columns={
                    "Canonical Product": "Product",
                    "Amount spent (USD)": "Total Amount Spent (USD)",
                    "Amount spent (INR)": "Total Amount Spent (INR)"
                })
                
                final_campaign_data = []
                has_purchases = "Purchases" in df_campaign.columns
                has_dates = 'Date' in df_campaign.columns
                has_delivery_status = 'Delivery status' in df_campaign.columns
                
                for product, product_campaigns in df_campaign.groupby("Canonical Product"):
                    for _, campaign in product_campaigns.iterrows():
                        row = {
                            "Product Name": "",
                            "Campaign Name": campaign["Campaign name"],
                            "Amount Spent (USD)": campaign["Amount spent (USD)"],
                            "Amount Spent (INR)": campaign["Amount spent (USD)"] * 100,
                            "Product": product
                        }
                        if has_purchases:
                            row["Purchases"] = campaign.get("Purchases", 0)
                        if has_dates:
                            row["Date"] = campaign.get("Date", "")
                        if has_delivery_status:
                            row["Delivery status"] = campaign.get("Delivery status", "")
                        final_campaign_data.append(row)
                
                df_final_campaign = pd.DataFrame(final_campaign_data)
                
                if not df_final_campaign.empty:
                    order = (
                        df_final_campaign.groupby("Product")["Amount Spent (INR)"].sum()
                        .sort_values(ascending=False).index
                    )
                    df_final_campaign["Product"] = pd.Categorical(
                        df_final_campaign["Product"], categories=order, ordered=True
                    )
                    
                    sort_cols = ["Product"]
                    if has_dates:
                        sort_cols.append("Date")
                    
                    df_final_campaign = df_final_campaign.sort_values(sort_cols).reset_index(drop=True)
                    df_final_campaign["Delivered Orders"] = ""
                    df_final_campaign["Delivery Rate"] = ""
        
        # ==================== STEP 6: PROCESS SHOPIFY FILES ====================
        if shopify_data:
            df_shopify, shopify_messages = merge_shopify_files(shopify_data)
            messages.extend(shopify_messages)
            
            if df_shopify is not None:
                required_cols = ["Product title", "Product variant title", "Product variant price", "Net items sold"]
                available_cols = [col for col in required_cols if col in df_shopify.columns]
                
                if 'Date' in df_shopify.columns:
                    available_cols.append('Date')
                
                df_shopify = df_shopify[available_cols]
                
                # Add columns
                df_shopify["In Order"] = ""
                df_shopify["Product Cost (Input)"] = ""
                df_shopify["Delivery Rate"] = ""
                df_shopify["Delivered Orders"] = ""
                df_shopify["Net Revenue"] = ""
                df_shopify["Ad Spend (USD)"] = 0.0
                df_shopify["Shipping Cost"] = ""
                df_shopify["Operational Cost"] = ""
                df_shopify["Product Cost (Output)"] = ""
                df_shopify["Net Profit"] = ""
                df_shopify["Net Profit (%)"] = ""
                
                # ==================== MATCH WITH GOOGLE SHEETS ====================
                df_shopify, matched_count, unmatched_products = match_products_with_database(
                    df_shopify, 
                    database_df, 
                    store_name
                )
                
                messages.append(f"✓ Matched {matched_count} products from Google Sheets")
                if unmatched_products:
                    messages.append(f"⚠ Found {len(unmatched_products)} unmatched products")
                
                
                
                # ==================== OPTIONAL: OVERRIDE WITH REFERENCE FILE ====================
                if df_old_merged is not None:
                    df_shopify["Product title_norm"] = df_shopify["Product title"].astype(str).str.strip().str.lower()
                    df_shopify["Product variant title_norm"] = df_shopify["Product variant title"].astype(str).str.strip().str.lower()
                    
                    delivery_rate_lookup = {}
                    product_cost_lookup = {}
                    has_product_cost = "Product Cost (Input)" in df_old_merged.columns
                    
                    for _, row in df_old_merged.iterrows():
                        key = (row["Product title_norm"], row["Product variant title_norm"])
                        delivery_rate_lookup[key] = row["Delivery Rate"]
                        
                        if has_product_cost and pd.notna(row["Product Cost (Input)"]) and row["Product Cost (Input)"] != "":
                            product_cost_lookup[key] = row["Product Cost (Input)"]
                    
                    delivery_matched_count = 0
                    product_cost_matched_count = 0
                    
                    for idx, row in df_shopify.iterrows():
                        key = (row["Product title_norm"], row["Product variant title_norm"])
                        
                        if key in delivery_rate_lookup:
                            df_shopify.loc[idx, "Delivery Rate"] = delivery_rate_lookup[key]
                            delivery_matched_count += 1
                        
                        if key in product_cost_lookup:
                            df_shopify.loc[idx, "Product Cost (Input)"] = product_cost_lookup[key]
                            product_cost_matched_count += 1
                    
                    df_shopify = df_shopify.drop(columns=["Product title_norm", "Product variant title_norm"])
                    
                    messages.append(f"✓ Reference file overrode {delivery_matched_count} delivery rates")
                    if has_product_cost and product_cost_matched_count > 0:
                        messages.append(f"✓ Reference file overrode {product_cost_matched_count} product costs")
                
                # Clean Shopify product titles
                df_shopify["Product Name"] = df_shopify["Product title"].astype(str).apply(clean_product_name)
                
                # Build candidate set from campaign canonical names
                campaign_products = grouped_campaign["Product"].unique().tolist() if grouped_campaign is not None else []
                
                df_shopify["Canonical Product"] = df_shopify["Product Name"].apply(
                    lambda x: fuzzy_match_to_campaign(x, campaign_products)
                )
                
                # ---- CORRECTED AD SPEND ALLOCATION (DAY-WISE DISTRIBUTION) ----
                if grouped_campaign is not None and df_campaign is not None:
                    # Initialize Ad Spend column to 0 for all rows
                    df_shopify["Ad Spend (USD)"] = 0.0
                    
                    # Create campaign spend lookup by product and date
                    campaign_spend_by_product_date = {}
                    
                    # First, build the campaign spend lookup from df_campaign (which has dates)
                    if 'Date' in df_campaign.columns:
                        for _, row in df_campaign.iterrows():
                            product = row['Canonical Product']
                            date = str(row['Date'])
                            amount_usd = row['Amount spent (USD)']
                            
                            if product not in campaign_spend_by_product_date:
                                campaign_spend_by_product_date[product] = {}
                            
                            if date not in campaign_spend_by_product_date[product]:
                                campaign_spend_by_product_date[product][date] = 0
                            
                            campaign_spend_by_product_date[product][date] += amount_usd
                    
                    # Track which products have received date-specific allocation
                    products_with_date_allocation = set()
                    
                    # Now allocate ad spend to Shopify variants based on their share of items sold per product per date
                    for product, product_df in df_shopify.groupby("Canonical Product"):
                        if product in campaign_spend_by_product_date:
                            has_any_date_allocation = False
                            
                            # For each date, calculate total items sold by this product on that date
                            for date in campaign_spend_by_product_date[product].keys():
                                date_campaign_spend_usd = campaign_spend_by_product_date[product][date]
                                
                                # Get all variants of this product sold on this date
                                product_date_variants = product_df[product_df['Date'].astype(str) == date]
                                
                                if not product_date_variants.empty:
                                    total_items_on_date = product_date_variants['Net items sold'].sum()
                                    
                                    if total_items_on_date > 0:
                                        # Distribute the campaign spend for this date proportionally
                                        for idx, variant_row in product_date_variants.iterrows():
                                            variant_items = variant_row['Net items sold']
                                            variant_share = variant_items / total_items_on_date
                                            variant_ad_spend_usd = date_campaign_spend_usd * variant_share
                                            
                                            # Update the ad spend for this specific variant on this date
                                            df_shopify.loc[idx, "Ad Spend (USD)"] = variant_ad_spend_usd
                                            has_any_date_allocation = True
                            
                            # Mark this product as having received date-specific allocation
                            if has_any_date_allocation:
                                products_with_date_allocation.add(product)
                    
                    # For products WITHOUT any date-specific campaign data, fall back to total allocation
                    ad_spend_map = dict(zip(grouped_campaign["Product"], grouped_campaign["Total Amount Spent (USD)"]))
                    
                    for product, product_df in df_shopify.groupby("Canonical Product"):
                        # FIXED: Only allocate total spend if this product did NOT get date-specific allocation
                        if product not in products_with_date_allocation and product in ad_spend_map:
                            total_items = product_df["Net items sold"].sum()
                            if total_items > 0:
                                total_spend_usd = ad_spend_map[product]
                                
                                # Allocate spend based on items sold for this product
                                for idx, variant_row in product_df.iterrows():
                                    variant_items = variant_row['Net items sold']
                                    variant_share = variant_items / total_items
                                    variant_ad_spend_usd = total_spend_usd * variant_share
                                    df_shopify.loc[idx, "Ad Spend (USD)"] = variant_ad_spend_usd
                
                # Sort products
                product_order = (
                    df_shopify.groupby("Product title")["Net items sold"]
                    .sum()
                    .sort_values(ascending=False)
                    .index
                )
                df_shopify["Product title"] = pd.Categorical(df_shopify["Product title"], categories=product_order, ordered=True)
                
                sort_cols = ["Product title"]
                if 'Date' in df_shopify.columns:
                    sort_cols.append("Date")
                
                df_shopify = df_shopify.sort_values(by=sort_cols).reset_index(drop=True)
        
        # ---- CREATE OVERALL WEIGHTED AVERAGE LOOKUPS (like Streamlit - SIMPLER) ----
        product_date_avg_prices = {}  # Keep variable names for compatibility
        product_date_delivery_rates = {}
        product_date_cost_inputs = {}

        if df_shopify is not None and not df_shopify.empty:
            logger.info("📊 Creating overall weighted average lookups from Shopify data...")
            
            for product, product_df in df_shopify.groupby("Canonical Product"):
                total_sold = product_df["Net items sold"].sum()
                
                if total_sold > 0:
                    # Weighted average price (OVERALL - not per date)
                    weighted_avg_price = (
                        (product_df["Product variant price"] * product_df["Net items sold"]).sum()
                        / total_sold
                    )
                    product_date_avg_prices[product] = float(weighted_avg_price)  # Convert to Python float
                    
                    # Weighted average delivery rate (OVERALL - not per date)
                    # First, ensure Delivery Rate column exists and has valid data
                    if "Delivery Rate" in product_df.columns:
                        valid_delivery_df = product_df[
                            pd.to_numeric(product_df["Delivery Rate"], errors="coerce").notna()
                        ].copy()
                        
                        if not valid_delivery_df.empty and valid_delivery_df["Net items sold"].sum() > 0:
                            # Convert delivery rates properly
                            delivery_rates_numeric = pd.to_numeric(
                                valid_delivery_df["Delivery Rate"], 
                                errors="coerce"
                            )
                            weighted_avg_delivery = (
                                (delivery_rates_numeric * valid_delivery_df["Net items sold"]).sum()
                                / valid_delivery_df["Net items sold"].sum()
                            )
                            product_date_delivery_rates[product] = float(weighted_avg_delivery)  # Convert to Python float
                        else:
                            product_date_delivery_rates[product] = 0.0
                    else:
                        product_date_delivery_rates[product] = 0.0
                    
                    # Weighted average product cost (OVERALL - not per date)
                    if "Product Cost (Input)" in product_df.columns:
                        valid_cost_df = product_df[
                            pd.to_numeric(product_df["Product Cost (Input)"], errors="coerce").notna()
                        ].copy()
                        
                        if not valid_cost_df.empty and valid_cost_df["Net items sold"].sum() > 0:
                            cost_inputs_numeric = pd.to_numeric(
                                valid_cost_df["Product Cost (Input)"], 
                                errors="coerce"
                            )
                            weighted_avg_cost = (
                                (cost_inputs_numeric * valid_cost_df["Net items sold"]).sum()
                                / valid_cost_df["Net items sold"].sum()
                            )
                            product_date_cost_inputs[product] = float(weighted_avg_cost)  # Convert to Python float
                        else:
                            product_date_cost_inputs[product] = 0.0
                    else:
                        product_date_cost_inputs[product] = 0.0
            
            logger.info(f"✓ Created overall weighted averages for {len(product_date_avg_prices)} products")
            # ============ ADD THIS BLOCK ============
            logger.info("\n" + "="*60)
            logger.info("VERIFYING LOOKUP DATA STRUCTURES")
            logger.info("="*60)
        
            for lookup_name, lookup_dict in [
              ("avg_prices", product_date_avg_prices),
              ("delivery_rates", product_date_delivery_rates),
              ("cost_inputs", product_date_cost_inputs)
            ]: 
              logger.info(f"\n{lookup_name}: {len(lookup_dict)} products")
              if lookup_dict:
                sample_key = list(lookup_dict.keys())[0]
                sample_value = lookup_dict[sample_key]
                logger.info(f"  Sample: '{sample_key}' → {sample_value} (type: {type(sample_value)})")
                
                for product, value in lookup_dict.items():
                    if not isinstance(value, (int, float)):
                        logger.error(f"  ❌ ERROR: {lookup_name}['{product}'] is {type(value)}: {value}")
        
            logger.info("="*60 + "\n")
        # ============ END OF ADDED BLOCK ============
        
            logger.info(f"  Products with prices: {list(product_date_avg_prices.keys())}")
            logger.info(f"  Products with delivery rates: {list(product_date_delivery_rates.keys())}")
            logger.info(f"  Products with cost inputs: {list(product_date_cost_inputs.keys())}")
            
            # DEBUG: Show sample values
            for product in list(product_date_avg_prices.keys())[:3]:  # Show first 3 products
                logger.info(f"\n📋 Sample data for '{product}':")
                logger.info(f"   Avg Price: {safe_lookup_get(product_date_avg_prices, product, 0.0):.2f}")
                logger.info(f"   Delivery Rate: {safe_lookup_get(product_date_delivery_rates, product, 0.0):.2f}")
                logger.info(f"   Cost Input: {safe_lookup_get(product_date_cost_inputs, product, 0.0):.2f}")
        else:
            logger.warning("⚠️ No Shopify data available for creating lookups")
        # ========== CALCULATE SELECTED DAYS ==========
        unique_campaign_dates = []
        if df_campaign is not None and 'Date' in df_campaign.columns:
            unique_campaign_dates = sorted([str(d) for d in df_campaign['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        
        if selected_days is None:
            if len(unique_campaign_dates) > 0:
                n_days = len(unique_campaign_dates)
                selected_days = n_days // 2 if n_days % 2 == 0 else (n_days + 1) // 2
            else:
                selected_days = 1
        
        
        
        # ---- GENERATE EXCEL FILES ----
        results = {}
        
        if df_shopify is not None:
            export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")
            has_dates = 'Date' in export_df.columns
            
            if has_dates:
                shopify_excel, shopify_json = convert_shopify_to_excel_with_date_columns_fixed(export_df, shipping_rate, operational_rate)
            else:
                shopify_excel = convert_shopify_to_excel(export_df, shipping_rate, operational_rate)
                shopify_json = []
            
            # Generate filename (without extension)
            shopify_base_filename = generate_processed_filename(
                store_name, 
                'shopify', 
                shopify_files[0].filename if shopify_files else 'raw'
            )
            
            # STORE FILE IN MEMORY FOR DOWNLOAD ENDPOINT
            processed_files['shopify'] = shopify_excel
            processed_json_data['shopify'] = shopify_json
            processed_filenames['shopify'] = shopify_base_filename  # Store base filename
            
            logger.info(f"✅ Generated Shopify filename: {shopify_base_filename}")
            
            # Also include in base64 for backward compatibility
            results['shopify_file'] = base64.b64encode(shopify_excel).decode('utf-8')
            results['shopify_filename'] = "shopify_processed.xlsx"
            
            results['shopify_data'] = shopify_json  # ✅ NEW: Add JSON to response
            logger.info(f"✅ Shopify JSON: {len(shopify_json)} records")
        
        if df_final_campaign is not None:
            has_dates = 'Date' in df_final_campaign.columns
            
            if has_dates:
                campaign_excel, campaign_json = convert_final_campaign_to_excel_with_date_columns_fixed(
                      df_final_campaign, 
                      df_shopify, 
                      selected_days, 
                      shipping_rate, 
                      operational_rate,
                      product_date_avg_prices=product_date_avg_prices,
                      product_date_delivery_rates=product_date_delivery_rates,
                      product_date_cost_inputs=product_date_cost_inputs
                )
            else:
                campaign_excel = convert_final_campaign_to_excel(df_final_campaign, df_shopify, shipping_rate, operational_rate)
                
            # Generate filename (without extension)
            campaign_base_filename = generate_processed_filename(
                store_name, 
                'campaign', 
                campaign_files[0].filename if campaign_files else 'raw'
            )
            
            # STORE FILE IN MEMORY FOR DOWNLOAD ENDPOINT
            processed_files['campaign'] = campaign_excel
            processed_json_data['campaign'] = campaign_json  # ← ADD THIS LINE
            processed_filenames['campaign'] = campaign_base_filename  # Store base filename
            
            logger.info(f"✅ Generated Campaign filename: {campaign_base_filename}")
            
            # Also include in base64 for backward compatibility
            results['campaign_file'] = base64.b64encode(campaign_excel).decode('utf-8')
            results['campaign_filename'] = "campaign_processed.xlsx"
            results['campaign_data'] = campaign_json  # ← ADD THIS LINE
       # ==================== RETURN RESPONSE WITH STORE NAME AND UNMATCHED PRODUCTS ====================
        formatted_unmatched = format_unmatched_products_for_response(
            unmatched_products, 
            database_df, 
            store_name
        )
        
        results['messages'] = messages
        results['store_name'] = store_name
        results['unmatched_products'] = formatted_unmatched
        results['summary'] = {
            'campaign_files': len(campaign_data),
            'shopify_files': len(shopify_data),
            'reference_files': len(reference_data),
            'selected_days': selected_days,
            'unique_dates': len(unique_campaign_dates) if unique_campaign_dates else 0,
            'store_name': store_name,
            'total_shopify_products': len(df_shopify) if df_shopify is not None else 0,
            'matched_from_database': matched_count,
            'unmatched_count': len(unmatched_products),
            'formatted_unmatched_count': len(formatted_unmatched),
            'input_products_count': len(database_df) if not database_df.empty else 0  # NEW
        }
        download_urls = {}

# Check if shopify file was generated
        if 'shopify_file' in results and results['shopify_file']:
             download_urls['shopify'] = True
    
# Check if campaign file was generated
        if 'campaign_file' in results and results['campaign_file']:
            download_urls['campaign'] = True

# Add to results
        results['download_urls'] = download_urls

        logger.info(f"✅ Download URLs prepared: {list(download_urls.keys())}")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/api/download/{file_type}")
async def download_file(file_type: str):
    """
    Download processed Excel files directly
    
    Args:
        file_type: Either 'shopify' or 'campaign'
    
    Returns:
        Excel file as downloadable response
    """
    logger.info(f"Download request for: {file_type}")
    logger.info(f"Available files: {list(processed_files.keys())}")
    logger.info(f"processed_files dict size: {len(processed_files)}")
    
    if file_type not in ['shopify', 'campaign']:
        raise HTTPException(
            status_code=400, 
            detail="Invalid file type. Use 'shopify' or 'campaign'"
        )
    
    if file_type not in processed_files:
        raise HTTPException(
            status_code=404, 
            detail=f"{file_type.capitalize()} file not found. Please process files first using POST /api/process-files"
        )
    
    try:
        # Get the file bytes
        file_bytes = processed_files[file_type]
        logger.info(f"Sending {file_type} file: {len(file_bytes)} bytes")
        
        # Create a BytesIO object
        file_stream = BytesIO(file_bytes)
        
        # Set filename
       
        base_filename = processed_filenames.get(file_type, f"{file_type}_processed")
        filename = f"{base_filename}.xlsx"
        
        # Return as streaming response
        return StreamingResponse(
            file_stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(file_bytes))
            }
        )
    except Exception as e:
        logger.error(f"Error downloading {file_type}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")


@app.get("/api/download-json/{file_type}")
async def download_json(file_type: str):
    """
    Download processed data as JSON
    
    Args:
        file_type: Either 'shopify' or 'campaign'
    
    Returns:
        JSON file as downloadable response
    """
    logger.info(f"JSON download request for: {file_type}")
    
    if file_type not in ['shopify', 'campaign']:
        raise HTTPException(
            status_code=400, 
            detail="Invalid file type. Use 'shopify' or 'campaign'"
        )
    
    if file_type not in processed_json_data or not processed_json_data[file_type]:
        raise HTTPException(
            status_code=404, 
            detail=f"{file_type.capitalize()} JSON data not found. Please process files first using POST /api/process-files"
        )
    
    try:
        # Get the JSON data
        json_data = processed_json_data[file_type]
        
        # ← REPLACE THIS LINE:
        # logger.info(f"Sending {file_type} JSON: {len(json_data)} records")
        
        # ← WITH THESE LINES:
        if file_type == 'shopify':
            logger.info(f"Sending {file_type} JSON: {len(json_data)} records")
        else:  # campaign
            logger.info(f"Sending {file_type} JSON")
            logger.info(f"   - Main campaigns: {len(json_data.get('campaign_data', {}).get('main_data', []))}")
            logger.info(f"   - Excluded products: {len(json_data.get('campaign_data', {}).get('excluded_products', []))}")
        
        # Convert to JSON string
        json_str = json.dumps(json_data, indent=2)
        
        # Create a BytesIO object
        json_stream = BytesIO(json_str.encode('utf-8'))
        
        # Set filename from stored base filename (same as Excel but with .json extension)
        base_filename = processed_filenames.get(file_type, f"{file_type}_processed")
        filename = f"{base_filename}.json"
        
        # Return as streaming response
        return StreamingResponse(
            json_stream,
            media_type="application/json",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(json_str))
            }
        )
    except Exception as e:
        logger.error(f"Error downloading {file_type} JSON: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error downloading JSON: {str(e)}")
# ==================== OPTIONAL: ENDPOINT TO VIEW JSON IN BROWSER ====================
@app.get("/api/view-json/{file_type}")
async def view_json(file_type: str):
    """
    View processed data as JSON in browser (without download)
    
    Args:
        file_type: Either 'shopify' or 'campaign'
    
    Returns:
        JSON response
    """
    if file_type not in ['shopify', 'campaign']:
        raise HTTPException(
            status_code=400, 
            detail="Invalid file type. Use 'shopify' or 'campaign'"
        )
    
    if file_type not in processed_json_data or not processed_json_data[file_type]:
        raise HTTPException(
            status_code=404, 
            detail=f"{file_type.capitalize()} JSON data not found. Please process files first"
        )
    
    return {
        "file_type": file_type,
        "record_count": len(processed_json_data[file_type]),
        "data": processed_json_data[file_type]
    }


@app.get("/api/preview/{file_type}")
async def preview_file(file_type: str, sheet: str = "Sheet1", rows: int = 10):
    """
    Preview the first few rows of a processed file
    
    Args:
        file_type: Either 'shopify' or 'campaign'
        sheet: Sheet name to preview (default: first sheet)
        rows: Number of rows to preview (default: 10)
    """
    if file_type not in processed_files:
        raise HTTPException(
            status_code=404, 
            detail=f"{file_type.capitalize()} file not found"
        )
    
    try:
        # Read Excel file from bytes
        df = pd.read_excel(BytesIO(processed_files[file_type]), sheet_name=sheet, nrows=rows)
        
        # Convert to JSON for preview
        preview_data = {
            'columns': df.columns.tolist(),
            'data': df.to_dict('records'),
            'total_columns': len(df.columns),
            'preview_rows': len(df)
        }
        
        return preview_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")


@app.get("/api/file-info/{file_type}")
async def get_file_info(file_type: str):
    """
    Get information about a processed file without downloading it
    """
    if file_type not in processed_files:
        raise HTTPException(
            status_code=404, 
            detail=f"{file_type.capitalize()} file not found"
        )
    
    try:
        # Read Excel file
        excel_file = pd.ExcelFile(BytesIO(processed_files[file_type]))
        
        # Get info about all sheets
        sheets_info = {}
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            sheets_info[sheet_name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': df.columns.tolist()
            }
        
        return {
            'file_type': file_type,
            'file_size_bytes': len(processed_files[file_type]),
            'file_size_mb': round(len(processed_files[file_type]) / (1024 * 1024), 2),
            'sheets': sheets_info,
            'total_sheets': len(excel_file.sheet_names)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")


@app.delete("/api/clear-files")
async def clear_processed_files():
    """
    Clear all processed files from memory
    """
    global processed_files
    count = len(processed_files)
    processed_files.clear()
    
    return {
        'status': 'success',
        'message': f'Cleared {count} processed file(s)'}
    
    
@app.get("/")
async def root():
    return {"message": "Excel Processing API is running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}



    """Test Google Sheets connection and return sample data with detailed error info"""
    try:
        logger.info("Testing Google Sheets connection...")
        logger.info(f"Credentials file: {GOOGLE_SHEETS_CREDENTIALS_FILE}")
        logger.info(f"Sheet ID: {GOOGLE_SHEET_ID}")
        logger.info(f"Worksheet name: {DATABASE_WORKSHEET_NAME}")
        
        # Step 1: Test credentials file
        import os
        if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
            return {
                "status": "error",
                "step": "credentials_file",
                "message": f"Credentials file not found: {GOOGLE_SHEETS_CREDENTIALS_FILE}",
                "detail": "Make sure credentials.json is in the correct location"
            }
        
        # Step 2: Connect to Google Sheets
        logger.info("Connecting to Google Sheets...")
        client = connect_to_google_sheets()
        logger.info("Connection successful!")
        
        # Step 3: Open the sheet
        logger.info(f"Opening sheet with ID: {GOOGLE_SHEET_ID}")
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        logger.info(f"Sheet opened: {sheet.title}")
        
        # Step 4: Get worksheet
        logger.info(f"Getting worksheet: {DATABASE_WORKSHEET_NAME}")
        try:
            worksheet = sheet.worksheet(DATABASE_WORKSHEET_NAME)
            logger.info(f"Worksheet found: {worksheet.title}")
        except Exception as ws_error:
            available_worksheets = [ws.title for ws in sheet.worksheets()]
            return {
                "status": "error",
                "step": "worksheet_not_found",
                "message": f"Worksheet '{DATABASE_WORKSHEET_NAME}' not found",
                "available_worksheets": available_worksheets,
                "detail": f"Available worksheets: {', '.join(available_worksheets)}"
            }
        
        # Step 5: Get data
        logger.info("Fetching data from worksheet...")
        data = worksheet.get_all_records()
        logger.info(f"Fetched {len(data)} records")
        
        # Get sample data
        sample_data = data[:5] if len(data) >= 5 else data
        
        # Get column headers
        headers = worksheet.row_values(1) if data else []
        
        return {
            "status": "success",
            "message": "Successfully connected to Google Sheets",
            "sheet_title": sheet.title,
            "worksheet_name": worksheet.title,
            "total_records": len(data),
            "column_headers": headers,
            "sample_data": sample_data
        }
    except Exception as e:
        logger.error(f"Error in test endpoint: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "step": "unknown",
            "message": str(e),
            "error_type": type(e).__name__,
            "detail": "Check logs for full error traceback"
        }

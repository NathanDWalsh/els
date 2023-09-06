import sys
import pandas as pd
import yaml
import eel.config as ec
import eel.execute as ei
import eel.tree as et


def process_input(yaml_string: str) -> pd.DataFrame:
    # For now, we're ignoring the yaml_string and returning a fixed DataFrame.
    # data = {"Column1": ["Value1", "Value2"], "Column2": ["Value3", "Value4"]}
    yaml_dict = yaml.safe_load(yaml_string)
    default_config_dict = ec.Config().model_dump()
    config_dict = et.merge_dicts_by_top_level_keys(default_config_dict, yaml_dict)
    config = ec.Config(**config_dict)
    config.eval_dynamic_attributes()
    df = ei.get_df(config.source, nrows=100)
    return df


if __name__ == "__main__":
    # if len(sys.argv) > 1:  # check if an argument was passed
    #     yaml_input = sys.argv[1]  # the first argument
    #     df = process_input(yaml_input)
    #     print(df.to_html(index=False))
    # else:
    #     print("no results")

    yaml_input = """target:
  table: SalesMX
source:
  read_excel:
    sheet_name: _leaf_name
    dtype:
      'Country (Sold-to)': str
      'Distribution Channel Code': str
      'Distribution Channel Name': str
      'CO Channel': str
      'Bus Field Code': str
      'Bus Field Name': str
      'Sales Order No.': str
      'Company Code': str
      'Company Name': str
      '0Item category': str
      'Business': str
      'Product Family': str
      'Product Group Code': str
      'Product Group Name': str
      'SKU Code': str
      'SKU Name': str
      'Cust Group (Sold-to) Code': str
      'Cust Group (Sold-to) Name': str
      'Customer (Sold to) Code': str
      'Customer (Sold to) Name': str
      'Customer (Ship-to) Code': str
      'Customer (Ship-to) Name': str
      'Fiscal year / period': str
      'Valid from': str
      'Valid to': str
      'Posting period': int64
      'Fiscal year': str
      'Number of Days': int64
      'Number of Workdays': int64
      'Transaction Type': str
      'Posting Date': str
      'Gross Sales': float
      '805111 Distribution Channel Discounts': float
      '805121 Customer / Basic Discounts': float
      '805122 Product Discounts': float
      '805123 Volume Discounts': float
      '805120 Value Discounts': float
      '805190 Other Sales Discounts': float
      '805110 Mandatory Discounts given at the moment of sale': float
      'Reba.Dir.': float
      '805640 Prompt Payment Discounts': float
      'Reba.Indir': float
      'Man.Di.Pay': float
      'Net Sales Invoiced': float
      '8080 Royalty Income': float
      '8090 Other Income': float
      '8100 Net Sales': float
      'Sales quantity': float
      'Quant. Free goods': float
      'Quant.Free of charge': float
      'Quant. Returns': float
      'Quant. Donations': float
      'Quant.Samples': float
    names:
      - Country (Sold-to)
      - Distribution Channel Code
      - Distribution Channel Name
      - CO Channel
      - Bus Field Code
      - Bus Field Name
      - Sales Order No.
      - Company Code
      - Company Name
      - 0Item category
      - Business
      - Product Family
      - Product Group Code
      - Product Group Name
      - SKU Code
      - SKU Name
      - Cust Group (Sold-to) Code
      - Cust Group (Sold-to) Name
      - Customer (Sold to) Code
      - Customer (Sold to) Name
      - Customer (Ship-to) Code
      - Customer (Ship-to) Name
      - Fiscal year / period
      - Valid from
      - Valid to
      - Posting period
      - Fiscal year
      - Number of Days
      - Number of Workdays
      - Transaction Type
      - Posting Date
      - Gross Sales
      - 805111 Distribution Channel Discounts
      - 805121 Customer / Basic Discounts
      - 805122 Product Discounts
      - 805123 Volume Discounts
      - 805120 Value Discounts
      - 805190 Other Sales Discounts
      - 805110 Mandatory Discounts given at the moment of sale
      - Reba.Dir.
      - 805640 Prompt Payment Discounts
      - Reba.Indir
      - Man.Di.Pay
      - Net Sales Invoiced
      - 8080 Royalty Income
      - 8090 Other Income
      - 8100 Net Sales
      - Sales quantity
      - Quant. Free goods
      - Quant.Free of charge
      - Quant. Returns
      - Quant. Donations
      - Quant.Samples"""

    df = process_input(yaml_input)

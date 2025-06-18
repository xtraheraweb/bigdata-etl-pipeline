from pyspark.sql import SparkSession
from pymongo import MongoClient
from datetime import datetime
import json
import os
import re
import xml.etree.ElementTree as ET

# MongoDB connection parameters from environment variables
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongo:27017')
MONGO_DB = os.environ.get('MONGO_DB', 'bigdata_saham')
MONGO_SOURCE_COLLECTION = os.environ.get('MONGO_COLLECTION', 'idx_raw')
MONGO_TARGET_COLLECTION = os.environ.get('MONGO_TRANSFORM_COLLECTION', 'idx_transform')

# Exchange rate for USD to IDR
EXCHANGE_RATE_USD_TO_IDR = 16251.0

# Sector-based revenue mapping
SECTOR_REVENUE_MAPPING = {
    'A. Energy': 'idx-cor:SalesAndRevenue',
    'B. Basic Materials': 'idx-cor:SalesAndRevenue', 
    'C. Industrials': 'idx-cor:SalesAndRevenue',
    'D. Consumer Non-Cyclicals': 'idx-cor:SalesAndRevenue',
    'E. Consumer Cyclicals': 'idx-cor:SalesAndRevenue',
    'F. Healthcare': 'idx-cor:SalesAndRevenue',
    'G. Financials': 'idx-cor:SalesAndRevenue',  # Default for financial sector
    'H. Properties & Real Estate': 'idx-cor:SalesAndRevenue',
    'I. Technology': 'idx-cor:SalesAndRevenue',
    'J. Infrastructures': 'idx-cor:SalesAndRevenue',
    'K. Transportation & Logistic': 'idx-cor:SalesAndRevenue'
}

# New mapping for Gross Profit
SECTOR_GROSS_PROFIT_MAPPING = {
    'A. Energy': 'idx-cor:GrossProfit',
    'B. Basic Materials': 'idx-cor:GrossProfit',
    'C. Industrials': 'idx-cor:GrossProfit',
    'D. Consumer Non-Cyclicals': 'idx-cor:GrossProfit',
    'E. Consumer Cyclicals': 'idx-cor:GrossProfit',
    'F. Healthcare': 'idx-cor:GrossProfit',
    # G. Financials is intentionally omitted as they typically don't report Gross Profit directly
    'H. Properties & Real Estate': 'idx-cor:GrossProfit',
    'I. Technology': 'idx-cor:GrossProfit',
    'J. Infrastructures': 'idx-cor:GrossProfit',
    'K. Transportation & Logistic': 'idx-cor:GrossProfit'
}

# New mapping for Net Profit
SECTOR_NET_PROFIT_MAPPING = {
    'A. Energy': 'idx-cor:ProfitLoss',
    'B. Basic Materials': 'idx-cor:ProfitLoss',
    'C. Industrials': 'idx-cor:ProfitLoss',
    'D. Consumer Non-Cyclicals': 'idx-cor:ProfitLoss',
    'E. Consumer Cyclicals': 'idx-cor:ProfitLoss',
    'F. Healthcare': 'idx-cor:ProfitLoss',
    # G. Financials is intentionally omitted for now
    'H. Properties & Real Estate': 'idx-cor:ProfitLoss',
    'I. Technology': 'idx-cor:ProfitLoss',
    'J. Infrastructures': 'idx-cor:ProfitLoss',
    'K. Transportation & Logistic': 'idx-cor:ProfitLoss'
}

# New mapping for Cash
SECTOR_CASH_MAPPING = {
    'A. Energy': 'idx-cor:CashAndCashEquivalents',
    'B. Basic Materials': 'idx-cor:CashAndCashEquivalents',
    'C. Industrials': 'idx-cor:CashAndCashEquivalents',
    'D. Consumer Non-Cyclicals': 'idx-cor:CashAndCashEquivalents',
    'E. Consumer Cyclicals': 'idx-cor:CashAndCashEquivalents',
    'F. Healthcare': 'idx-cor:CashAndCashEquivalents',
    # G. Financials is intentionally omitted for now
    'H. Properties & Real Estate': 'idx-cor:CashAndCashEquivalents',
    'I. Technology': 'idx-cor:CashAndCashEquivalents',
    'J. Infrastructures': 'idx-cor:CashAndCashEquivalents',
    'K. Transportation & Logistic': 'idx-cor:CashAndCashEquivalents'
}

# Short and long term borrowing fields (not sector-specific)
SHORT_TERM_BORROWING_FIELDS = [
    'ShortTermLoans',
    'ShortTermBankLoans'
]

LONG_TERM_BORROWING_FIELDS = [
    'LongTermLoans',
    'LongTermBankLoans'
]

# Equity fields (for all sectors including financial sectors)
EQUITY_FIELDS = [
    'Equity',
    'TotalEquity'
]

# Cash from operations fields (for all sectors including financial sectors)
CASH_FROM_OPERATIONS_FIELDS = [
    'NetCashFlowsReceivedFromUsedInOperatingActivities',
    'idx-cor:NetCashFlowsReceivedFromUsedInOperatingActivities',
    'CashFlowsFromUsedInOperatingActivities',
    'idx-cor:CashFlowsFromUsedInOperatingActivities',
    'NetCashProvidedByOperatingActivities'
]

# Cash from investing activities fields (for all sectors)
CASH_FROM_INVESTING_FIELDS = [
    'NetCashFlowsReceivedFromUsedInInvestingActivities',
    'idx-cor:NetCashFlowsReceivedFromUsedInInvestingActivities',
    'CashFlowsFromUsedInInvestingActivities',
    'idx-cor:CashFlowsFromUsedInInvestingActivities',
    'NetCashProvidedByInvestingActivities'
]

# Cash from financing activities fields (for all sectors)
CASH_FROM_FINANCING_FIELDS = [
    'NetCashFlowsReceivedFromUsedInFinancingActivities',
    'idx-cor:NetCashFlowsReceivedFromUsedInFinancingActivities',
    'CashFlowsFromUsedInFinancingActivities',
    'idx-cor:CashFlowsFromUsedInFinancingActivities',
    'NetCashProvidedByFinancingActivities'
]

# New mapping for Operating Profit
# For non-financial sectors: GrossProfit - SellingExpenses - GeneralAndAdministrativeExpenses
SECTOR_OPERATING_PROFIT_MAPPING = {
    'A. Energy': 'GrossProfit-Expenses',
    'B. Basic Materials': 'GrossProfit-Expenses',
    'C. Industrials': 'GrossProfit-Expenses',
    'D. Consumer Non-Cyclicals': 'GrossProfit-Expenses',
    'E. Consumer Cyclicals': 'GrossProfit-Expenses',
    'F. Healthcare': 'GrossProfit-Expenses',
    # G. Financials is handled separately by subsector
    'H. Properties & Real Estate': 'GrossProfit-Expenses',
    'I. Technology': 'GrossProfit-Expenses',
    'J. Infrastructures': 'GrossProfit-Expenses',
    'K. Transportation & Logistic': 'GrossProfit-Expenses'
}

# Financial subsector-specific operating profit mapping
FINANCIAL_SUBSECTOR_OPERATING_PROFIT_MAPPING = {
    'G1. Banks': 'ProfitFromOperation',
    'G2. Financing Service': 'Revenue-Expenses',
    'G3. Investment Service': 'Revenue-Expenses',
    'G4. Insurance': 'Revenue-Expenses',
    'G5. Holding & Investment Companies': 'ProfitFromOperation'
}

# Financial subsector-specific revenue mapping
FINANCIAL_SUBSECTOR_MAPPING = {
    'G1. Banks': ['InterestIncome', 'idx-cor:InterestIncome'],
    'G2. Financing Service': [
        'IncomeFromConsumerFinancing', 'idx-cor:IncomeFromConsumerFinancing',
        'IncomeFromMurabahahAndIstishna', 'idx-cor:IncomeFromMurabahahAndIstishna', 
        'IncomeFromFinanceLease', 'idx-cor:IncomeFromFinanceLease'
    ],
    'G3. Investment Service': ['IncomeFromInvestmentManagementServices', 'idx-cor:IncomeFromInvestmentManagementServices'],
    'G4. Insurance': ['RevenueFromInsurancePremiums', 'idx-cor:RevenueFromInsurancePremiums'],
    'G5. Holding & Investment Companies': [
        'DividendsIncome', 'idx-cor:DividendsIncome',
        'InterestIncome', 'idx-cor:InterestIncome'
    ]
}

def create_spark_session():
    """Create Spark session with LOCAL configuration"""
    spark = SparkSession.builder \
        .appName("IDX_Revenue_Transform") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.extraJavaOptions", 
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " + 
                "--add-exports java.base/sun.security.action=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " + 
                "--add-exports java.base/sun.security.action=ALL-UNNAMED") \
        .getOrCreate()
    
    return spark

def parse_xbrl_element(element_data):
    """
    Parse XBRL element to extract value and metadata.
    Handles lists of XML strings, single XML strings, or plain values.
    Returns a list of dictionaries, each representing a parsed XBRL fact.
    """

    # Helper function to parse a single XML string element
    def _parse_single_xml_string(xml_str):
        try:
            elem = ET.fromstring(xml_str)
            element_info = {
                'value': elem.text, # Store original text
                'tag_name': elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag,
                'contextRef': elem.get('contextRef'),
                'decimals': elem.get('decimals'),
                'unitRef': elem.get('unitRef'),
                'id': elem.get('id'),
                'scaled_value': None # Initialize scaled_value
            }
            
            is_nil = elem.get('{http://www.w3.org/2001/XMLSchema-instance}nil') == 'true'

            if not is_nil and elem.text and elem.text.strip():
                try:
                    numeric_value = float(elem.text.strip())
                    decimals_str = element_info['decimals']
                    
                    if decimals_str and decimals_str.lower() != 'inf':
                        try:
                            decimals_val = int(decimals_str)
                            # Apply scaling based on user's rule: value * 10^(-decimals)
                            # e.g., decimals = -3 => multiply by 10^3 = 1000
                            # e.g., decimals = 2  => multiply by 10^-2 = 0.01
                            scale_factor = 10 ** (-decimals_val)
                            numeric_value = numeric_value * scale_factor
                        except ValueError:
                            # If decimals_str is not a valid int, keep numeric_value as is (after float conversion)
                            pass
                    
                    # Perform currency conversion if unit is USD
                    if element_info['unitRef'] == 'USD' and numeric_value is not None:
                        numeric_value = numeric_value * EXCHANGE_RATE_USD_TO_IDR
                        element_info['unitRef'] = 'IDR' # Update unitRef to reflect conversion

                    element_info['scaled_value'] = numeric_value
                except ValueError:
                    # If elem.text is not a valid float, scaled_value remains None
                    pass
            
            return element_info
        except ET.XMLSyntaxError:
            # Fallback for invalid XML string
            return {'value': xml_str, 'scaled_value': None, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None}

    if isinstance(element_data, list):
        parsed_results = []
        for item in element_data:
            if isinstance(item, str) and '<' in item and '>' in item:
                parsed_results.append(_parse_single_xml_string(item))
            else: # Item in list is not an XML string
                try:
                    num_val = float(item)
                    parsed_results.append({'value': num_val, 'scaled_value': num_val, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None})
                except (ValueError, TypeError, AttributeError):
                    parsed_results.append({'value': item, 'scaled_value': item, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None})
        return parsed_results

    if isinstance(element_data, str):
        if '<' in element_data and '>' in element_data: # Single XML string
            return [_parse_single_xml_string(element_data)]
        else: # Plain string, not XML - less likely for fields we parse this way but handle
            try:
                # Attempt to treat as a number if it's a plain string that looks like one
                num_val = float(re.sub(r'[^\\d.-]', '', element_data))
                return [{'value': element_data, 'scaled_value': num_val, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None}]
            except (ValueError, TypeError):
                # If not a number, scaled_value is the string itself or None
                return [{'value': element_data, 'scaled_value': element_data, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None}]

    # Fallback for other types
    # If element_data is already a dict (e.g. from a different parsing path, though unlikely here)
    if isinstance(element_data, dict) and 'scaled_value' in element_data:
        return [element_data]
    
    # If element_data is a direct number
    try:
        num_val = float(element_data)
        return [{'value': num_val, 'scaled_value': num_val, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None}]
    except (ValueError, TypeError, AttributeError) :
        return [{'value': element_data, 'scaled_value': element_data, 'contextRef': None, 'tag_name': None, 'decimals': None, 'unitRef': None, 'id': None}]

def determine_revenue_field(company_sector):
    """
    Determine which revenue field to extract based on company sector
    """
    if not company_sector:
        return 'idx-cor:SalesAndRevenue'  # Default fallback
    
    # Check if it's financial sector
    if company_sector.startswith('G.'):
        # Check for financial subsectors
        for subsector, field_list in FINANCIAL_SUBSECTOR_MAPPING.items():
            if subsector in company_sector:
                return field_list  # Return list of fields for financial subsector
        # Default for general financial sector
        return 'idx-cor:SalesAndRevenue'
    
    # For non-financial sectors
    return SECTOR_REVENUE_MAPPING.get(company_sector, 'idx-cor:SalesAndRevenue')

def extract_revenue_by_sector(laporan_keuangan, company_sector):
    """
    Extract revenue based on company sector and subsector
    """
    revenue_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [], # Will store all parsed elements for the chosen field
        'revenue_field_used': None
    }
    
    # EXCHANGE_RATE_USD_TO_IDR = 
    # .0 # Moved to global scope
    
    # Determine which revenue field to use
    primary_field = determine_revenue_field(company_sector)
    
    # Fallback fields to try if primary field not found
    fallback_fields = [
        'idx-cor:SalesAndRevenue',
        'SalesAndRevenue', 
        'Revenue', 
        'TotalRevenue',
        'NetRevenue',
        'InterestIncome',  # For banks
        'idx-cor:InterestIncome',
        'IncomeFromConsumerFinancing',  # For financing
        'idx-cor:IncomeFromConsumerFinancing',
        'RevenueFromInsurancePremiums',  # For insurance
        'idx-cor:RevenueFromInsurancePremiums'
    ]
    
    # Try primary field first, then fallbacks
    fields_to_try = []
    
    # If primary_field is a list (for financial subsectors), add each item
    if isinstance(primary_field, list):
        fields_to_try.extend(primary_field)
    else:
        fields_to_try.append(primary_field)
    
    # Add fallbacks that aren't already in the list
    fields_to_try.extend([f for f in fallback_fields if f not in fields_to_try])
    
    for field in fields_to_try:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            
            revenue_data['metadata'] = parsed_elements # Store all elements for this field
            revenue_data['revenue_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                
                # Filter for specific contextRefs 'CurrentYearDuration' or 'PriorYearDuration'
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        # unit_ref = element.get('unitRef') # No longer needed here
                        # if unit_ref == 'USD': # Conversion now done in parse_xbrl_element
                        #     scaled_value = scaled_value * EXCHANGE_RATE_USD_TO_IDR
                        revenue_data['current_year'] = scaled_value
                        current_year_val_found = True 
                               
                elif context_ref == 'PriorYearDuration' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        # unit_ref = element.get('unitRef') # No longer needed here
                        # if unit_ref == 'USD': # Conversion now done in parse_xbrl_element
                        #     scaled_value = scaled_value * EXCHANGE_RATE_USD_TO_IDR
                        revenue_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                # If both primary current and prior year values are found, no need to process more elements for *this specific field*
                if current_year_val_found and prior_year_val_found:
                    break 
            
            # If we found any value (current or prior) for this field, break from fields_to_try
            if revenue_data['current_year'] is not None or revenue_data['prior_year'] is not None:
                break
    
    return revenue_data

def calculate_revenue_growth(current_year, prior_year):
    """Calculate revenue growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# New function to determine gross profit field
def determine_gross_profit_field(company_sector):
    """
    Determine which gross profit field to extract based on company sector.
    Financial sector (G) is ignored.
    """
    if not company_sector or company_sector.startswith('G.'): # Ignore Financials for Gross Profit
        return None
    return SECTOR_GROSS_PROFIT_MAPPING.get(company_sector)

# New function to extract gross profit by sector
def extract_gross_profit_by_sector(laporan_keuangan, company_sector):
    gross_profit_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [], # Stores parsed elements for the chosen field
        'gross_profit_field_used': None
    }

    # Determine which gross profit field to use based on sector
    primary_field = determine_gross_profit_field(company_sector)

    if not primary_field: # If sector is Financial or not mapped for gross profit
        return gross_profit_data

    # Fallback fields to try if primary field not found
    fallback_fields = ['GrossProfit'] # Generic fallback without namespace

    fields_to_try = [primary_field]
    fields_to_try.extend([f for f in fallback_fields if f not in fields_to_try])
    
    for field in fields_to_try:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            
            gross_profit_data['metadata'] = parsed_elements
            gross_profit_data['gross_profit_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        gross_profit_data['current_year'] = scaled_value
                        current_year_val_found = True
                               
                elif context_ref == 'PriorYearDuration' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        gross_profit_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                if current_year_val_found and prior_year_val_found:
                    break 
            
            # If we found any value (current or prior) for this field, break from fields_to_try
            if gross_profit_data['current_year'] is not None or gross_profit_data['prior_year'] is not None:
                break
    
    return gross_profit_data

# New function for gross profit growth (identical logic to revenue growth)
def calculate_gross_profit_growth(current_year, prior_year):
    """Calculate gross profit growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# New function to determine net profit field
def determine_net_profit_field(company_sector):
    """
    Determine which net profit field to extract based on company sector.
    Financial sector (G) is ignored for now.
    """
    if not company_sector or company_sector.startswith('G.'): # Ignore Financials for Net Profit for now
        return None
    return SECTOR_NET_PROFIT_MAPPING.get(company_sector)

# New function to extract net profit by sector
def extract_net_profit_by_sector(laporan_keuangan, company_sector):
    net_profit_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'net_profit_field_used': None
    }
    primary_field = determine_net_profit_field(company_sector)
    if not primary_field:
        return net_profit_data

    fallback_fields = ['ProfitLoss'] # Generic fallback without namespace

    fields_to_try = [primary_field]
    fields_to_try.extend([f for f in fallback_fields if f not in fields_to_try])
    
    for field in fields_to_try:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            net_profit_data['metadata'] = parsed_elements
            net_profit_data['net_profit_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        net_profit_data['current_year'] = scaled_value
                        current_year_val_found = True
                elif context_ref == 'PriorYearDuration' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        net_profit_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                if current_year_val_found and prior_year_val_found:
                    break
            
            if net_profit_data['current_year'] is not None or net_profit_data['prior_year'] is not None:
                break
    return net_profit_data

# New function for net profit growth
def calculate_net_profit_growth(current_year, prior_year):
    """Calculate net profit growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# New function to determine cash field
def determine_cash_field(company_sector):
    """
    Determine which cash field to extract based on company sector.
    Financial sector (G) is ignored for now.
    """
    if not company_sector or company_sector.startswith('G.'): # Ignore Financials for Cash for now
        return None
    return SECTOR_CASH_MAPPING.get(company_sector)

# New function to extract cash by sector
def extract_cash_by_sector(laporan_keuangan, company_sector):
    cash_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'cash_field_used': None
    }
    primary_field = determine_cash_field(company_sector)
    if not primary_field:
        return cash_data

    fallback_fields = ['CashAndCashEquivalents'] # Generic fallback

    fields_to_try = [primary_field]
    fields_to_try.extend([f for f in fallback_fields if f not in fields_to_try])
    
    for field in fields_to_try:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            cash_data['metadata'] = parsed_elements
            cash_data['cash_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Instant contexts for balance sheet items like Cash
                if context_ref == 'CurrentYearInstant' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        cash_data['current_year'] = scaled_value
                        current_year_val_found = True
                elif context_ref == 'PriorEndYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        cash_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                # Fallback to PriorYearInstant if PriorEndYearInstant not found
                elif context_ref == 'PriorYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        cash_data['prior_year'] = scaled_value
                        prior_year_val_found = True

                if current_year_val_found and prior_year_val_found:
                    break
            
            if cash_data['current_year'] is not None or cash_data['prior_year'] is not None:
                break
    return cash_data

# Function to calculate cash growth
def calculate_cash_growth(current_year, prior_year):
    """Calculate cash growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# Function to determine short term borrowing field
def determine_short_term_borrowing_field(company_sector):
    """
    Determine which short-term borrowing fields to try.
    Financial sector (G) is ignored for now.
    """
    if not company_sector or company_sector.startswith('G.'): # Ignore Financials for Short Term Borrowing for now
        return None
    return SHORT_TERM_BORROWING_FIELDS

# Function to extract short term borrowing by sector
def extract_short_term_borrowing_by_sector(laporan_keuangan, company_sector):
    """Extract short term borrowing data based on company sector"""
    short_term_borrowing_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'short_term_borrowing_field_used': None
    }
    
    # Skip for financial sectors for now
    if company_sector and company_sector.startswith('G.'): 
        return short_term_borrowing_data

    # Try each field in the predefined list
    for field in SHORT_TERM_BORROWING_FIELDS:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            short_term_borrowing_data['metadata'] = parsed_elements
            short_term_borrowing_data['short_term_borrowing_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Instant contexts for balance sheet items like Short Term Borrowing
                if context_ref == 'CurrentYearInstant' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        short_term_borrowing_data['current_year'] = scaled_value
                        current_year_val_found = True
                elif context_ref == 'PriorEndYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        short_term_borrowing_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                # Fallback to PriorYearInstant if PriorEndYearInstant not found
                elif context_ref == 'PriorYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        short_term_borrowing_data['prior_year'] = scaled_value
                        prior_year_val_found = True

                if current_year_val_found and prior_year_val_found:
                    break
            
            if short_term_borrowing_data['current_year'] is not None or short_term_borrowing_data['prior_year'] is not None:
                break
    
    return short_term_borrowing_data

# Function to calculate short term borrowing growth rate
def calculate_short_term_borrowing_growth(current_year, prior_year):
    """Calculate short term borrowing growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# Function to determine long term borrowing field
def determine_long_term_borrowing_field(company_sector):
    """
    Determine which long-term borrowing field to extract based on company sector.
    Financial sector (G) is ignored for now.
    """
    if not company_sector or company_sector.startswith('G.'): # Ignore Financials for Long Term Borrowing for now
        return None
    return LONG_TERM_BORROWING_FIELDS

# Function to extract long term borrowing by sector
def extract_long_term_borrowing_by_sector(laporan_keuangan, company_sector):
    """Extract long term borrowing data based on company sector"""
    long_term_borrowing_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'long_term_borrowing_field_used': None
    }
    
    # Skip for financial sectors for now
    if company_sector and company_sector.startswith('G.'): 
        return long_term_borrowing_data

    # Try each field in the predefined list
    for field in LONG_TERM_BORROWING_FIELDS:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            long_term_borrowing_data['metadata'] = parsed_elements
            long_term_borrowing_data['long_term_borrowing_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Instant contexts for balance sheet items like Long Term Borrowing
                if context_ref == 'CurrentYearInstant' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        long_term_borrowing_data['current_year'] = scaled_value
                        current_year_val_found = True
                elif context_ref == 'PriorEndYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        long_term_borrowing_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                # Fallback to PriorYearInstant if PriorEndYearInstant not found
                elif context_ref == 'PriorYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        long_term_borrowing_data['prior_year'] = scaled_value
                        prior_year_val_found = True

                if current_year_val_found and prior_year_val_found:
                    break
            
            if long_term_borrowing_data['current_year'] is not None or long_term_borrowing_data['prior_year'] is not None:
                break
    
    return long_term_borrowing_data

# Function to calculate long term borrowing growth rate
def calculate_long_term_borrowing_growth(current_year, prior_year):
    """Calculate long term borrowing growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# New mapping for Assets (all sectors use the same field)
ASSETS_FIELD = 'idx-cor:Assets'

# Function to extract assets data (same for all sectors)
def extract_assets_data(laporan_keuangan):
    """
    Extract assets data from the financial report using idx-cor:Assets
    Uses CurrentYearInstant and PriorEndYearInstant context references
    """
    assets_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'assets_field_used': None
    }
    
    # Primary field is always idx-cor:Assets for all sectors
    primary_field = ASSETS_FIELD
    
    # Fallback fields to try if primary field not found
    fallback_fields = ['Assets', 'TotalAssets']
    
    fields_to_try = [primary_field]
    fields_to_try.extend([f for f in fallback_fields if f not in fields_to_try])
    
    for field in fields_to_try:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            assets_data['metadata'] = parsed_elements
            assets_data['assets_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False
            
            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Instant contexts for balance sheet items like Assets
                if context_ref == 'CurrentYearInstant' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        assets_data['current_year'] = scaled_value
                        current_year_val_found = True
                elif context_ref == 'PriorEndYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        assets_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                # Fallback to PriorYearInstant if PriorEndYearInstant not found
                elif context_ref == 'PriorYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        assets_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                if current_year_val_found and prior_year_val_found:
                    break
            
            if assets_data['current_year'] is not None or assets_data['prior_year'] is not None:
                break
    
    return assets_data

# Function to calculate assets growth (same as other growth calculations)
def calculate_assets_growth(current_year, prior_year):
    """Calculate assets growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# Function to extract equity data (common for all sectors including financial sectors)
def extract_equity_data(laporan_keuangan):
    """
    Extract equity data from the financial report using equity fields
    This function works for all sectors, including financial sectors
    """
    equity_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'equity_field_used': None
    }
    
    # Try each field in the predefined list
    for field in EQUITY_FIELDS:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            equity_data['metadata'] = parsed_elements
            equity_data['equity_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False
            
            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Instant contexts for balance sheet items like Equity
                if context_ref == 'CurrentYearInstant' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        equity_data['current_year'] = scaled_value
                        current_year_val_found = True
                elif context_ref == 'PriorEndYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        equity_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                # Fallback to PriorYearInstant if PriorEndYearInstant not found
                elif context_ref == 'PriorYearInstant' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        equity_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                if current_year_val_found and prior_year_val_found:
                    break
            
            if equity_data['current_year'] is not None or equity_data['prior_year'] is not None:
                break
    
    return equity_data

# Function to calculate equity growth
def calculate_equity_growth(current_year, prior_year):
    """Calculate equity growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# Function to extract cash from operations data (for all sectors including financial sectors)
def extract_cash_from_operations_data(laporan_keuangan):
    """
    Extract cash from operations data from the financial report (current year only)
    This function works for all sectors, including financial sectors
    """
    cash_from_operations_data = {
        'current_year': None,
        'metadata': [],
        'cash_from_operations_field_used': None
    }
    
    # Try each field in the predefined list
    for field in CASH_FROM_OPERATIONS_FIELDS:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            cash_from_operations_data['metadata'] = parsed_elements
            cash_from_operations_data['cash_from_operations_field_used'] = field
            
            current_year_val_found = False
            
            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Duration contexts for cash flow statements
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        cash_from_operations_data['current_year'] = scaled_value
                        current_year_val_found = True
                        break
            
            if cash_from_operations_data['current_year'] is not None:
                break
    
    return cash_from_operations_data

# Function to extract cash from investing activities (current year only)
def extract_cash_from_investing_data(laporan_keuangan):
    """
    Extract cash from investing activities data (current year only)
    This function works for all sectors, including financial sectors
    """
    cash_from_investing_data = {
        'current_year': None,
        'metadata': [],
        'cash_from_investing_field_used': None
    }
    
    # Try each field in the predefined list
    for field in CASH_FROM_INVESTING_FIELDS:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            cash_from_investing_data['metadata'] = parsed_elements
            cash_from_investing_data['cash_from_investing_field_used'] = field
            
            current_year_val_found = False
            
            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Duration contexts for cash flow statements
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        cash_from_investing_data['current_year'] = scaled_value
                        current_year_val_found = True
                        break
            
            if cash_from_investing_data['current_year'] is not None:
                break
    
    return cash_from_investing_data

# Function to extract cash from financing activities (current year only)
def extract_cash_from_financing_data(laporan_keuangan):
    """
    Extract cash from financing activities data (current year only)
    This function works for all sectors, including financial sectors
    """
    cash_from_financing_data = {
        'current_year': None,
        'metadata': [],
        'cash_from_financing_field_used': None
    }
    
    # Try each field in the predefined list
    for field in CASH_FROM_FINANCING_FIELDS:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            cash_from_financing_data['metadata'] = parsed_elements
            cash_from_financing_data['cash_from_financing_field_used'] = field
            
            current_year_val_found = False
            
            for element in parsed_elements:
                context_ref = element.get('contextRef')
                # Use Duration contexts for cash flow statements
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        cash_from_financing_data['current_year'] = scaled_value
                        current_year_val_found = True
                        break
            
            if cash_from_financing_data['current_year'] is not None:
                break
    
    return cash_from_financing_data

def extract_financial_revenue_by_subsector(laporan_keuangan, company_sector):
    """
    Extract and sum revenue for financial subsectors (G2 and G5) that have multiple income sources.
    For G2: Financing Service - Sum of IncomeFromConsumerFinancing, IncomeFromMurabahahAndIstishna, and IncomeFromFinanceLease
    For G5: Holding & Investment Companies - Sum of DividendsIncome and InterestIncome
    """
    revenue_data = {
        'current_year': 0,  # Start with 0 for summing
        'prior_year': 0,    # Start with 0 for summing
        'metadata': [],      # Will store all parsed elements
        'revenue_field_used': [],  # List of fields used
        'is_combined': True  # Flag to indicate combined revenue sources
    }
    
    # Get list of fields for this subsector
    fields_to_try = []
    
    for subsector, field_list in FINANCIAL_SUBSECTOR_MAPPING.items():
        if subsector in company_sector:
            fields_to_try.extend(field_list)
            break
    
    if not fields_to_try:  # Fallback if no matching subsector found
        return extract_revenue_by_sector(laporan_keuangan, company_sector)
    
    # Track which fields were found
    fields_found = []
    current_year_found = False
    prior_year_found = False
    
    # Iterate through all fields and sum their values
    for field in fields_to_try:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            revenue_data['metadata'].extend(parsed_elements)  # Combine metadata
            fields_found.append(field)
            
            for element in parsed_elements:
                context_ref = element.get('contextRef')
                
                # Sum values for CurrentYearDuration
                if context_ref == 'CurrentYearDuration':
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        revenue_data['current_year'] += scaled_value
                        current_year_found = True
                
                # Sum values for PriorYearDuration
                elif context_ref == 'PriorYearDuration':
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        revenue_data['prior_year'] += scaled_value
                        prior_year_found = True
    
    # Set fields_used to the list of fields that were actually found
    if fields_found:
        revenue_data['revenue_field_used'] = '+'.join(fields_found)
    else:
        revenue_data['revenue_field_used'] = None
        
    # Reset to None if no values were found
    if not current_year_found:
        revenue_data['current_year'] = None
    if not prior_year_found:
        revenue_data['prior_year'] = None
    
    # If no values were found at all, fallback to regular extraction
    if not current_year_found and not prior_year_found:
        return extract_revenue_by_sector(laporan_keuangan, company_sector)
        
    return revenue_data

# Function to extract selling expenses data
def extract_selling_expenses_data(laporan_keuangan):
    """
    Extract selling expenses data from financial report
    """
    selling_expenses_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'selling_expenses_field_used': None
    }
    
    # Primary field for selling expenses
    primary_fields = ['idx-cor:SellingExpenses', 'SellingExpenses']
    
    # Try each field
    for field in primary_fields:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            selling_expenses_data['metadata'] = parsed_elements
            selling_expenses_data['selling_expenses_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        selling_expenses_data['current_year'] = scaled_value
                        current_year_val_found = True
                               
                elif context_ref == 'PriorYearDuration' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        selling_expenses_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                if current_year_val_found and prior_year_val_found:
                    break 
            
            # If we found any value (current or prior) for this field, break
            if selling_expenses_data['current_year'] is not None or selling_expenses_data['prior_year'] is not None:
                break
    
    return selling_expenses_data

# Function to extract general and administrative expenses data
def extract_general_admin_expenses_data(laporan_keuangan):
    """
    Extract general and administrative expenses data from financial report
    """
    general_admin_expenses_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'general_admin_expenses_field_used': None
    }
    
    # Primary field for general and administrative expenses
    primary_fields = ['idx-cor:GeneralAndAdministrativeExpenses', 'GeneralAndAdministrativeExpenses']
    
    # Try each field
    for field in primary_fields:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            general_admin_expenses_data['metadata'] = parsed_elements
            general_admin_expenses_data['general_admin_expenses_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        general_admin_expenses_data['current_year'] = scaled_value
                        current_year_val_found = True
                               
                elif context_ref == 'PriorYearDuration' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        general_admin_expenses_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                if current_year_val_found and prior_year_val_found:
                    break 
            
            # If we found any value (current or prior) for this field, break
            if general_admin_expenses_data['current_year'] is not None or general_admin_expenses_data['prior_year'] is not None:
                break
    
    return general_admin_expenses_data

# Function to extract profit from operation data (for banks and holding companies)
def extract_profit_from_operation_data(laporan_keuangan):
    """
    Extract profit from operation data from financial report (primarily for G1 Banks and G5 Holding/Investment)
    """
    profit_from_operation_data = {
        'current_year': None,
        'prior_year': None,
        'metadata': [],
        'profit_from_operation_field_used': None
    }
    
    # Primary fields to try
    primary_fields = ['idx-cor:ProfitFromOperation', 'ProfitFromOperation', 'OperatingProfit']
    
    # Try each field
    for field in primary_fields:
        if field in laporan_keuangan:
            parsed_elements = parse_xbrl_element(laporan_keuangan[field])
            profit_from_operation_data['metadata'] = parsed_elements
            profit_from_operation_data['profit_from_operation_field_used'] = field
            
            current_year_val_found = False
            prior_year_val_found = False

            for element in parsed_elements:
                context_ref = element.get('contextRef')
                
                if context_ref == 'CurrentYearDuration' and not current_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        profit_from_operation_data['current_year'] = scaled_value
                        current_year_val_found = True
                               
                elif context_ref == 'PriorYearDuration' and not prior_year_val_found:
                    scaled_value = element.get('scaled_value')
                    if scaled_value is not None:
                        profit_from_operation_data['prior_year'] = scaled_value
                        prior_year_val_found = True
                
                if current_year_val_found and prior_year_val_found:
                    break 
            
            # If we found any value (current or prior) for this field, break
            if profit_from_operation_data['current_year'] is not None or profit_from_operation_data['prior_year'] is not None:
                break
    
    return profit_from_operation_data

# Function to calculate operating profit based on sector
def calculate_operating_profit(company_sector, laporan_keuangan, gross_profit_data=None, revenue_data=None):
    """
    Calculate operating profit based on sector rules:
    - Non-financial sectors: GrossProfit - SellingExpenses - GeneralAndAdministrativeExpenses
    - G1. Banks: Use ProfitFromOperation directly
    - G2-G4 Financial services: revenue - (GeneralAndAdministrativeExpenses + SellingExpenses)
    - G5. Holding/Investment: Use ProfitFromOperation directly
    """
    operating_profit_data = {
        'current_year': None,
        'prior_year': None,
        'method_used': None,
        'components': []
    }
    
    # Check if it's a financial sector
    if company_sector and company_sector.startswith('G.'):
        # Handle financial subsectors
        if 'G1.' in company_sector or 'G5.' in company_sector:
            # For Banks (G1) and Holding/Investment Companies (G5), use ProfitFromOperation directly
            profit_from_operation = extract_profit_from_operation_data(laporan_keuangan)
            operating_profit_data['current_year'] = profit_from_operation['current_year']
            operating_profit_data['prior_year'] = profit_from_operation['prior_year']
            operating_profit_data['method_used'] = 'ProfitFromOperation'
            operating_profit_data['components'].append(profit_from_operation['profit_from_operation_field_used'])
        
        elif ('G2.' in company_sector or 'G3.' in company_sector or 'G4.' in company_sector):
            # For Financing Service (G2), Investment Service (G3), Insurance (G4)
            # Formula: revenue - (GeneralAndAdministrativeExpenses + SellingExpenses)
            
            # Get Revenue data if not provided
            if revenue_data is None:
                if 'G2.' in company_sector:
                    revenue_data = extract_financial_revenue_by_subsector(laporan_keuangan, company_sector)
                else:
                    revenue_data = extract_revenue_by_sector(laporan_keuangan, company_sector)
            
            # Get expenses
            selling_expenses = extract_selling_expenses_data(laporan_keuangan)
            general_admin_expenses = extract_general_admin_expenses_data(laporan_keuangan)
            
            # Calculate current year operating profit
            if revenue_data['current_year'] is not None:
                total_expenses_current = 0
                
                if selling_expenses['current_year'] is not None:
                    total_expenses_current += selling_expenses['current_year']
                
                if general_admin_expenses['current_year'] is not None:
                    total_expenses_current += general_admin_expenses['current_year']
                
                operating_profit_data['current_year'] = revenue_data['current_year'] - total_expenses_current
            
            # Calculate prior year operating profit
            if revenue_data['prior_year'] is not None:
                total_expenses_prior = 0
                
                if selling_expenses['prior_year'] is not None:
                    total_expenses_prior += selling_expenses['prior_year']
                
                if general_admin_expenses['prior_year'] is not None:
                    total_expenses_prior += general_admin_expenses['prior_year']
                
                operating_profit_data['prior_year'] = revenue_data['prior_year'] - total_expenses_prior
            
            operating_profit_data['method_used'] = 'Revenue-Expenses'
            operating_profit_data['components'] = [
                revenue_data.get('revenue_field_used'),
                selling_expenses.get('selling_expenses_field_used'),
                general_admin_expenses.get('general_admin_expenses_field_used')
            ]
            
    else:
        # For non-financial sectors
        # Formula: GrossProfit - SellingExpenses - GeneralAndAdministrativeExpenses
        
        # Get Gross Profit data if not provided
        if gross_profit_data is None:
            gross_profit_data = extract_gross_profit_by_sector(laporan_keuangan, company_sector)
        
        # Get expenses
        selling_expenses = extract_selling_expenses_data(laporan_keuangan)
        general_admin_expenses = extract_general_admin_expenses_data(laporan_keuangan)
        
        # Calculate current year operating profit
        if gross_profit_data['current_year'] is not None:
            operating_profit_current = gross_profit_data['current_year']
            
            if selling_expenses['current_year'] is not None:
                operating_profit_current -= selling_expenses['current_year']
            
            if general_admin_expenses['current_year'] is not None:
                operating_profit_current -= general_admin_expenses['current_year']
            
            operating_profit_data['current_year'] = operating_profit_current
        
        # Calculate prior year operating profit
        if gross_profit_data['prior_year'] is not None:
            operating_profit_prior = gross_profit_data['prior_year']
            
            if selling_expenses['prior_year'] is not None:
                operating_profit_prior -= selling_expenses['prior_year']
            
            if general_admin_expenses['prior_year'] is not None:
                operating_profit_prior -= general_admin_expenses['prior_year']
            
            operating_profit_data['prior_year'] = operating_profit_prior
        
        operating_profit_data['method_used'] = 'GrossProfit-Expenses'
        operating_profit_data['components'] = [
            gross_profit_data.get('gross_profit_field_used'),
            selling_expenses.get('selling_expenses_field_used'),
            general_admin_expenses.get('general_admin_expenses_field_used')
        ]
    
    return operating_profit_data

# Function to calculate operating profit growth
def calculate_operating_profit_growth(current_year, prior_year):
    """Calculate operating profit growth rate"""
    if current_year is not None and prior_year is not None and prior_year != 0:
        growth_rate = ((current_year - prior_year) / abs(prior_year)) * 100
        return round(growth_rate, 2)
    return None

# Function to extract report type (Annual/Quarterly)
def extract_report_type(laporan_keuangan):
    """
    Extract the report type (Annual/Quarterly) from PeriodOfFinancialStatementsSubmissions tag
    """
    report_type_data = {
        'report_type': None,
        'report_type_raw': None
    }
    
    if 'PeriodOfFinancialStatementsSubmissions' in laporan_keuangan:
        elements = parse_xbrl_element(laporan_keuangan['PeriodOfFinancialStatementsSubmissions'])
        for element in elements:
            value = element.get('value')
            if value:
                report_type_data['report_type_raw'] = value
                if 'Tahunan' in value or 'Annual' in value:
                    report_type_data['report_type'] = 'Annual'
                elif 'Triwulan' in value or 'Quarterly' in value:
                    report_type_data['report_type'] = 'Quarterly'
                break
    
    return report_type_data

# Function to extract reporting year
def extract_reporting_year(laporan_keuangan):
    """
    Extract the reporting year from date fields
    """
    reporting_year_data = {
        'year': None,
        'period_end_date': None
    }
    
    # Try to extract from CurrentPeriodEndDate first
    if 'CurrentPeriodEndDate' in laporan_keuangan:
        elements = parse_xbrl_element(laporan_keuangan['CurrentPeriodEndDate'])
        for element in elements:
            date_str = element.get('value')
            if date_str and '-' in date_str:
                try:
                    # Extract year from date string (format: YYYY-MM-DD)
                    year = date_str.split('-')[0]
                    if year.isdigit() and 2000 <= int(year) <= 2100:  # Sanity check
                        reporting_year_data['year'] = int(year)
                        reporting_year_data['period_end_date'] = date_str
                        break
                except (IndexError, ValueError):
                    pass
    
    # Fallback to PriorYearEndDate if year is still None
    if reporting_year_data['year'] is None and 'PriorYearEndDate' in laporan_keuangan:
        elements = parse_xbrl_element(laporan_keuangan['PriorYearEndDate'])
        for element in elements:
            date_str = element.get('value')
            if date_str and '-' in date_str:
                try:
                    # Extract year and add 1 (since prior year + 1 = current year)
                    year = int(date_str.split('-')[0]) + 1
                    if 2000 <= year <= 2100:  # Sanity check
                        reporting_year_data['year'] = year
                        break
                except (IndexError, ValueError):
                    pass
    
    return reporting_year_data

def transform_revenue_data():
    """Main transformation function for revenue data only"""
    print("Starting revenue transformation by sector...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        raw_collection = db[MONGO_SOURCE_COLLECTION]
        transformed_collection = db[MONGO_TARGET_COLLECTION]
        
        print(f"Connected to MongoDB - Database: {MONGO_DB}")
        
        # Get all raw financial reports
        raw_data = list(raw_collection.find({}))
        print(f"Found {len(raw_data)} raw financial reports")

        if not raw_data:
            print("No raw data found to transform")
            return
        
        transformed_data = []
        sector_summary = {}
        
        # Processing counter for simple progress indicator
        total_docs = len(raw_data)
        processed_docs = 0
        success_count = 0
        error_count = 0
        
        print("Processing financial reports...")
        
        for doc in raw_data:
            try:
                emiten = doc.get('emiten', 'Unknown')
                laporan_keuangan = doc.get('laporan_keuangan_data', {})
                
                # Extract company_sector from laporan_keuangan_data
                company_sector_raw = laporan_keuangan.get('Sector', '')
                if isinstance(company_sector_raw, list) and company_sector_raw:
                    first_sector_item = company_sector_raw[0]
                    if isinstance(first_sector_item, str) and '<' in first_sector_item and '>' in first_sector_item:
                        try:
                            sector_elem = ET.fromstring(first_sector_item)
                            company_sector = sector_elem.text.strip() if sector_elem.text else ''
                        except ET.XMLSyntaxError:
                            company_sector = str(first_sector_item)
                    else:
                        company_sector = str(first_sector_item)
                elif isinstance(company_sector_raw, str):
                    company_sector = company_sector_raw.strip()
                else:
                    company_sector = ''

                # Extract revenue based on sector
                if ('G2.' in company_sector or 'G5.' in company_sector):
                    # For financial subsectors with combined revenue sources
                    revenue_data = extract_financial_revenue_by_subsector(laporan_keuangan, company_sector)
                else:
                    revenue_data = extract_revenue_by_sector(laporan_keuangan, company_sector)
                
                # Calculate growth
                revenue_growth = calculate_revenue_growth(
                    revenue_data['current_year'], 
                    revenue_data['prior_year']
                )

                # Extract gross profit, net profit, cash, and assets
                gross_profit_data = extract_gross_profit_by_sector(laporan_keuangan, company_sector)
                gross_profit_growth = calculate_gross_profit_growth(
                    gross_profit_data['current_year'], gross_profit_data['prior_year']
                )
                
                net_profit_data = extract_net_profit_by_sector(laporan_keuangan, company_sector)
                net_profit_growth = calculate_net_profit_growth(
                    net_profit_data['current_year'], net_profit_data['prior_year']
                )
                
                cash_data = extract_cash_by_sector(laporan_keuangan, company_sector)
                cash_growth = calculate_cash_growth(
                    cash_data['current_year'], cash_data['prior_year']
                )

                assets_data = extract_assets_data(laporan_keuangan)
                assets_growth = calculate_assets_growth(
                    assets_data['current_year'], assets_data['prior_year']
                )
                
                # Extract short term borrowing data
                short_term_borrowing_data = extract_short_term_borrowing_by_sector(laporan_keuangan, company_sector)
                short_term_borrowing_growth = calculate_short_term_borrowing_growth(
                    short_term_borrowing_data['current_year'], short_term_borrowing_data['prior_year']
                )
                
                # Extract long term borrowing data
                long_term_borrowing_data = extract_long_term_borrowing_by_sector(laporan_keuangan, company_sector)
                long_term_borrowing_growth = calculate_long_term_borrowing_growth(
                    long_term_borrowing_data['current_year'], long_term_borrowing_data['prior_year']
                )
                
                # Extract equity data (for all sectors including financial sectors)
                equity_data = extract_equity_data(laporan_keuangan)
                equity_growth = calculate_equity_growth(
                    equity_data['current_year'], equity_data['prior_year']
                )
                
                # Extract cash from operations data (for all sectors including financial sectors)
                cash_from_operations_data = extract_cash_from_operations_data(laporan_keuangan)
                
                # Extract cash from investing activities (current year only)
                cash_from_investing_data = extract_cash_from_investing_data(laporan_keuangan)
                
                # Extract cash from financing activities (current year only)
                cash_from_financing_data = extract_cash_from_financing_data(laporan_keuangan)
                
                # Calculate operating profit based on sector
                operating_profit_data = calculate_operating_profit(
                    company_sector, laporan_keuangan, gross_profit_data, revenue_data
                )
                operating_profit_growth = calculate_operating_profit_growth(
                    operating_profit_data['current_year'], operating_profit_data['prior_year']
                )

                # Extract report type and reporting year
                report_type_data = extract_report_type(laporan_keuangan)
                reporting_year_data = extract_reporting_year(laporan_keuangan)

                # Create transformed document
                transformed_doc = {
                    'emiten': emiten,
                    'company_code': doc.get('company_code', ''),
                    'sector': company_sector,
                    'report_type': report_type_data['report_type'],
                    'reporting_year': reporting_year_data['year'],
                    'period_end_date': reporting_year_data['period_end_date'],
                    'revenue': {
                        'current_year': revenue_data['current_year'],
                        'prior_year': revenue_data['prior_year'],
                        'growth_rate_percent': revenue_growth,
                        'revenue_field_used': revenue_data['revenue_field_used']
                    },
                    'gross_profit': {
                        'current_year': gross_profit_data['current_year'],
                        'prior_year': gross_profit_data['prior_year'],
                        'growth_rate_percent': gross_profit_growth,
                        'gross_profit_field_used': gross_profit_data['gross_profit_field_used']
                    },
                    'operating_profit': {
                        'current_year': operating_profit_data['current_year'],
                        'prior_year': operating_profit_data['prior_year'],
                        'growth_rate_percent': operating_profit_growth,
                        'calculation_method': operating_profit_data['method_used'],
                        'components': operating_profit_data['components']
                    },
                    'net_profit': {
                        'current_year': net_profit_data['current_year'],
                        'prior_year': net_profit_data['prior_year'],
                        'growth_rate_percent': net_profit_growth,
                        'net_profit_field_used': net_profit_data['net_profit_field_used']
                    },
                    'cash': {
                        'current_year': cash_data['current_year'],
                        'prior_year': cash_data['prior_year'],
                        'growth_rate_percent': cash_growth,
                        'cash_field_used': cash_data['cash_field_used']
                    },
                    'assets': {
                        'current_year': assets_data['current_year'],
                        'prior_year': assets_data['prior_year'],
                        'growth_rate_percent': assets_growth,
                        'assets_field_used': assets_data['assets_field_used']
                    },
                    'short_term_borrowing': {
                        'current_year': short_term_borrowing_data['current_year'],
                        'prior_year': short_term_borrowing_data['prior_year'],
                        'growth_rate_percent': short_term_borrowing_growth,
                        'short_term_borrowing_field_used': short_term_borrowing_data['short_term_borrowing_field_used']
                    },
                    'long_term_borrowing': {
                        'current_year': long_term_borrowing_data['current_year'],
                        'prior_year': long_term_borrowing_data['prior_year'],
                        'growth_rate_percent': long_term_borrowing_growth,
                        'long_term_borrowing_field_used': long_term_borrowing_data['long_term_borrowing_field_used']
                    },
                    'equity': {
                        'current_year': equity_data['current_year'],
                        'prior_year': equity_data['prior_year'],
                        'growth_rate_percent': equity_growth,
                        'equity_field_used': equity_data['equity_field_used']
                    },
                    'cash_from_operations': {
                        'current_year': cash_from_operations_data['current_year'],
                        'cash_from_operations_field_used': cash_from_operations_data['cash_from_operations_field_used']
                    },
                    'cash_from_investing': {
                        'current_year': cash_from_investing_data['current_year'],
                        'cash_from_investing_field_used': cash_from_investing_data['cash_from_investing_field_used']
                    },
                    'cash_from_financing': {
                        'current_year': cash_from_financing_data['current_year'],
                        'cash_from_financing_field_used': cash_from_financing_data['cash_from_financing_field_used']
                    },
                    'xbrl_metadata': {
                        'revenue_elements_found': len(revenue_data['metadata']),
                        'gross_profit_elements_found': len(gross_profit_data['metadata']),
                        'net_profit_elements_found': len(net_profit_data['metadata']),
                        'cash_elements_found': len(cash_data['metadata']),
                        'assets_elements_found': len(assets_data['metadata']),
                        'short_term_borrowing_elements_found': len(short_term_borrowing_data['metadata']),
                        'long_term_borrowing_elements_found': len(long_term_borrowing_data['metadata'])
                    },
                    'data_quality': {
                        'has_current_year_revenue': revenue_data['current_year'] is not None,
                        'has_prior_year_revenue': revenue_data['prior_year'] is not None,
                        'can_calculate_revenue_growth': revenue_growth is not None,
                        'has_current_year_gross_profit': gross_profit_data['current_year'] is not None,
                        'has_prior_year_gross_profit': gross_profit_data['prior_year'] is not None,
                        'can_calculate_gross_profit_growth': gross_profit_growth is not None,
                        'has_current_year_operating_profit': operating_profit_data['current_year'] is not None,
                        'has_prior_year_operating_profit': operating_profit_data['prior_year'] is not None,
                        'can_calculate_operating_profit_growth': operating_profit_growth is not None,
                        'has_current_year_net_profit': net_profit_data['current_year'] is not None,
                        'has_prior_year_net_profit': net_profit_data['prior_year'] is not None,
                        'can_calculate_net_profit_growth': net_profit_growth is not None,
                        'has_current_year_cash': cash_data['current_year'] is not None,
                        'has_prior_year_cash': cash_data['prior_year'] is not None,
                        'can_calculate_cash_growth': cash_growth is not None,
                        'has_current_year_assets': assets_data['current_year'] is not None,
                        'has_prior_year_assets': assets_data['prior_year'] is not None,
                        'can_calculate_assets_growth': assets_growth is not None,
                        'has_current_year_short_term_borrowing': short_term_borrowing_data['current_year'] is not None,
                        'has_prior_year_short_term_borrowing': short_term_borrowing_data['prior_year'] is not None,
                        'can_calculate_short_term_borrowing_growth': short_term_borrowing_growth is not None,
                        'has_current_year_long_term_borrowing': long_term_borrowing_data['current_year'] is not None,
                        'has_prior_year_long_term_borrowing': long_term_borrowing_data['prior_year'] is not None,
                        'can_calculate_long_term_borrowing_growth': long_term_borrowing_growth is not None,
                        'has_current_year_equity': equity_data['current_year'] is not None,
                        'has_prior_year_equity': equity_data['prior_year'] is not None,
                        'can_calculate_equity_growth': equity_growth is not None,
                        'has_current_year_cash_from_operations': cash_from_operations_data['current_year'] is not None,
                        'has_current_year_cash_from_investing': cash_from_investing_data['current_year'] is not None,
                        'has_current_year_cash_from_financing': cash_from_financing_data['current_year'] is not None
                    },
                    'raw_document_id': str(doc.get('_id', ''))
                }
                
                transformed_data.append(transformed_doc)
                
                # Update sector summary (simplified)
                if company_sector not in sector_summary:
                    sector_summary[company_sector] = {
                        'count': 0,
                        'with_revenue': 0,
                        'with_growth': 0,
                        'with_operating_profit': 0
                    }
                
                sector_summary[company_sector]['count'] += 1
                if revenue_data['current_year'] is not None:
                    sector_summary[company_sector]['with_revenue'] += 1
                if revenue_growth is not None:
                    sector_summary[company_sector]['with_growth'] += 1
                if operating_profit_data['current_year'] is not None:
                    sector_summary[company_sector]['with_operating_profit'] += 1
                
                # Update counter
                processed_docs += 1
                success_count += 1
                
                # Simple progress indicator - print every 20 documents
                if processed_docs % 20 == 0 or processed_docs == total_docs:
                    print(f"Processed {processed_docs}/{total_docs} documents ({processed_docs/total_docs:.1%})")
                
            except Exception as e:
                print(f"Error processing {doc.get('emiten', 'Unknown')}: {str(e)}")
                error_count += 1
                processed_docs += 1
                continue
        
        # Store to MongoDB
        if transformed_data:
            print(f"\nStoring {len(transformed_data)} records to MongoDB...")
            
            # Clear existing transformed data
            transformed_collection.delete_many({})
            
            # Insert transformed data
            transformed_collection.insert_many(transformed_data)
            print(f"Successfully stored {len(transformed_data)} records")
            
            # Generate brief summary report
            print("\n=== TRANSFORMATION SUMMARY ===")
            print(f"Total documents processed: {total_docs}")
            print(f"Successful transformations: {success_count}")
            print(f"Failed transformations: {error_count}")
            
            # Basic statistics
            total_companies = len(transformed_data)
            companies_with_revenue = sum(1 for doc in transformed_data if doc['data_quality']['has_current_year_revenue'])
            companies_with_growth = sum(1 for doc in transformed_data if doc['data_quality']['can_calculate_revenue_growth'])
            companies_with_operating_profit = sum(1 for doc in transformed_data if doc['data_quality']['has_current_year_operating_profit'])
            
            print(f"\nCompanies with Revenue: {companies_with_revenue}/{total_companies} ({companies_with_revenue/total_companies:.1%})")
            print(f"Companies with Revenue Growth: {companies_with_growth}/{total_companies} ({companies_with_growth/total_companies:.1%})")
            print(f"Companies with Operating Profit: {companies_with_operating_profit}/{total_companies} ({companies_with_operating_profit/total_companies:.1%})")
            
            # Top 5 sectors by count
            top_sectors = sorted([(k, v['count']) for k, v in sector_summary.items() if k], 
                                key=lambda x: x[1], reverse=True)[:5]
            
            if top_sectors:
                print("\nTop 5 sectors by company count:")
                for sector, count in top_sectors:
                    print(f"- {sector}: {count} companies")
        
        else:
            print("No data to transform")
    
    except Exception as e:
        print(f"Error in transformation: {str(e)}")
        raise e
    
    finally:
        spark.stop()
        print("\nTransformation process completed")

def validate_revenue_data():
    """Validasi data hasil transformasi"""
    print("Validating transformed data...")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        transformed_collection = db[MONGO_TARGET_COLLECTION]
        
        # Hitung total records dan data finansial
        total_records = transformed_collection.count_documents({})
        revenue_records = transformed_collection.count_documents({"data_quality.has_current_year_revenue": True})
        growth_records = transformed_collection.count_documents({"data_quality.can_calculate_revenue_growth": True})
        gp_records = transformed_collection.count_documents({"data_quality.has_current_year_gross_profit": True})
        op_records = transformed_collection.count_documents({"data_quality.has_current_year_operating_profit": True})
        np_records = transformed_collection.count_documents({"data_quality.has_current_year_net_profit": True})
        cash_records = transformed_collection.count_documents({"data_quality.has_current_year_cash": True})
        assets_records = transformed_collection.count_documents({"data_quality.has_current_year_assets": True})
        stb_records = transformed_collection.count_documents({"data_quality.has_current_year_short_term_borrowing": True})
        ltb_records = transformed_collection.count_documents({"data_quality.has_current_year_long_term_borrowing": True})
        equity_records = transformed_collection.count_documents({"data_quality.has_current_year_equity": True})
        
        # Tampilkan ringkasan data yang disederhanakan
        print("\n=== VALIDATION SUMMARY ===")
        print(f"Total Records: {total_records}")
        print(f"Records with Revenue: {revenue_records}")
        print(f"Records with Revenue Growth: {growth_records}")
        print(f"Records with Gross Profit: {gp_records}")
        print(f"Records with Operating Profit: {op_records}")
        print(f"Records with Net Profit: {np_records}")
        print(f"Records with Cash: {cash_records}")
        print(f"Records with Assets: {assets_records}")
        print(f"Records with Short Term Borrowing: {stb_records}")
        print(f"Records with Long Term Borrowing: {ltb_records}")
        print(f"Records with Equity: {equity_records}")
        print("\n✓ Validation completed successfully")
        
        
    except Exception as e:
        print(f"Error in validation: {str(e)}")
        raise e

# Execute the transformation pipeline if this script is run directly
if __name__ == "__main__":
    print("Starting IDX revenue transformation process")
    transform_revenue_data()
    validate_revenue_data()
    print("IDX revenue transformation process completed")
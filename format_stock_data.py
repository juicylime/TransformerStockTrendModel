import json
from datetime import datetime

def convert_to_epoch(date_str):
    """Convert date string to epoch time."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())

def convert_field(value):
    """Convert strings that represent numbers into integers or floats."""
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        raise ValueError(f"Value {value} cannot be converted to a number.")

def calculate_percentage_change(current, previous):
    """Calculate percentage change compared to previous value."""
    if previous is None or previous == 0:
        return None  # Return None for the first day or if previous is 0 to avoid division by zero
    return ((current - previous) / previous) * 100

def format_data(input_file, output_file, percentage_change_file, combined_file):
    with open(input_file, 'r') as file:
        data = json.load(file)

    formatted_data = {}
    percentage_change_data = {}
    combined_data = {}

    # These fields will not have percentage change calculated
    fields_to_skip_percentage = {
        "reportedEPS", "estimatedEPS", "surprise", "surprisePercentage", "grossProfit",
        "totalRevenue", "costOfRevenue", "costofGoodsAndServicesSold", "operatingIncome",
        "sellingGeneralAndAdministrative", "researchAndDevelopment", "operatingExpenses", 
        "interestIncome", "investmentIncomeNet", "depreciation", 
        "interestExpense", "nonInterestIncome", "depreciationAndAmortization", 
        "incomeBeforeTax", "incomeTaxExpense", "interestAndDebtExpense", 
        "netIncomeFromContinuingOperations", "comprehensiveIncomeNetOfTax", 
        "ebit", "ebitda", "netIncome"
    }

    for stock_symbol, dates in data.items():
        formatted_data[stock_symbol] = {}
        percentage_change_data[stock_symbol] = {}
        combined_data[stock_symbol] = {}
        previous_metrics = None  # Initialize previous_metrics

        for date, metrics in dates.items():
            epoch_time = convert_to_epoch(date)
            formatted_metrics = {'Epoch': epoch_time}
            percentage_metrics = {'Epoch': epoch_time}
            combined_metrics = {'Epoch': epoch_time}

            for key, value in metrics.items():
                if key == 'reportedCurrency':
                    continue  # Skip reportedCurrency field
                elif key in ['fiscalDateEnding', 'reportedDate', 'Next_Earnings_Report_Date']:
                    formatted_metrics[key] = convert_to_epoch(value)
                    percentage_metrics[key] = convert_to_epoch(value)
                    combined_metrics[key] = convert_to_epoch(value)
                elif isinstance(value, str) and not value.replace('.','',1).isdigit():
                    continue
                elif isinstance(value, str):
                    converted_value = convert_field(value)
                    formatted_metrics[key] = converted_value
                    combined_metrics[key] = converted_value
                    if previous_metrics and key in previous_metrics and key not in fields_to_skip_percentage:
                        change_value = calculate_percentage_change(
                            converted_value, previous_metrics[key]
                        )
                        combined_metrics[f"{key}_change"] = change_value
                        percentage_metrics[f"{key}_change"] = change_value
                    elif key in fields_to_skip_percentage:
                        percentage_metrics[key] = converted_value
                        
                else:
                    formatted_metrics[key] = value
                    combined_metrics[key] = value
                    if previous_metrics and key in previous_metrics and key not in fields_to_skip_percentage:
                        change_value = calculate_percentage_change(
                            value, previous_metrics[key]
                        )
                        combined_metrics[f"{key}_change"] = change_value
                        percentage_metrics[f"{key}_change"] = change_value
                    elif key in fields_to_skip_percentage:
                        percentage_metrics[key] = value
                        

            formatted_data[stock_symbol][date] = formatted_metrics
            if previous_metrics:
                percentage_change_data[stock_symbol][date] = percentage_metrics
                combined_data[stock_symbol][date] = combined_metrics
            previous_metrics = formatted_metrics  # Update previous_metrics for the next iteration

    with open(output_file, 'w') as file:
        json.dump(formatted_data, file, indent=4)

    with open(percentage_change_file, 'w') as file:
        json.dump(percentage_change_data, file, indent=4)

    with open(combined_file, 'w') as file:
        json.dump(combined_data, file, indent=4)

# Define input and output paths
input_path = 'G:/StockData/combined_stock_data.json'
output_path = 'G:/StockData/formatted_combined_stock_data.json'
percentage_change_output_path = 'G:/StockData/percentage_change_stock_data.json'
combined_output_path = 'G:/StockData/combined_stock_data_with_changes.json'

# Call the function to format the data and calculate percentage changes
format_data(input_path, output_path, percentage_change_output_path, combined_output_path)

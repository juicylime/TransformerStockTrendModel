import json
from datetime import datetime
import os


def is_convertible_to_number(value):
    """Check if the string can be converted to an int or a float."""
    try:
        float(value)
        return True
    except ValueError:
        return False


def convert_to_epoch(date_str):
    """Convert date string to epoch time."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def convert_to_days(epoch):
    """Convert Epoch to number of days"""
    days = epoch // (24 * 60 * 60)
    return days


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


def get_day_of_week(epoch):
    # Convert the epoch to a datetime object
    date = datetime.utcfromtimestamp(epoch)
    # Get the day of the week as a number
    return date.weekday()


def format_data(input_file, output_file, percentage_change_file, combined_file):
    with open(input_file, 'r') as file:
        data = json.load(file)

    formatted_data = {}
    percentage_change_data = {}
    combined_data = {}

    # These fields will not have percentage change calculated
    fields_to_skip_percentage = [
        "fiscalDateEnding",
        "reportedDate",
        "reportedEPS",
        "estimatedEPS",
        "surprise",
        "surprisePercentage",
        "grossProfit",
        "totalRevenue",
        "costOfRevenue",
        "operatingIncome",
        "ebitda",
        "netIncome",
        "operatingCashflow",
        "profitLoss",
        "cashflowFromInvestment",
        "cashflowFromFinancing",
        "changeInCashAndCashEquivalents",
        "cashAndCashEquivalentsAtCarryingValue",
        "cashAndShortTermInvestments",
        "inventory",
        "currentNetReceivables",
        "propertyPlantEquipment",
        "longTermInvestments",
        "shortTermInvestments",
        "totalLiabilities",
        "totalCurrentLiabilities",
        "totalNonCurrentLiabilities",
        "longTermDebt",
        "currentLongTermDebt",
        "shortLongTermDebtTotal",
        "otherCurrentLiabilities",
        "otherNonCurrentLiabilities",
        "commonStockSharesOutstanding",
        "nextEstimatedEPS",
        "CONSUMER_value",
        "CONSUMER_percent_change",
        "CONSUMER_yearly_percent_change",
        "INDPRO_value",
        "INDPRO_percent_change",
        "INDPRO_yearly_percent_change",
        "RSAFS_value",
        "RSAFS_percent_change",
        "RSAFS_yearly_percent_change",
        "GDP_value",
        "GDP_percent_change",
        "GDP_yearly_percent_change",
        "CPIAUCSL_value",
        "CPIAUCSL_percent_change",
        "CPIAUCSL_yearly_percent_change",
        "UNRATE_value",
        "UNRATE_percent_change",
        "UNRATE_yearly_percent_change",
        "USEPUINDXD_value",
        "USEPUINDXD_percent_change",
        "USEPUINDXD_yearly_percent_change",
        "DGS10_value",
        "DGS10_percent_change",
        "DGS10_yearly_percent_change",
        "PSAR_combined",
        "FEDFUNDS_value",
        "FEDFUNDS_percent_change",
        "FEDFUNDS_yearly_percent_change",
        "article_count",
        "52_week_high",
        "52_week_low",
        "EMA_10",
        "MACD_12_26_9",
        "MACDh_12_26_9",
        "MACDs_12_26_9",
        "RSI_14",
        "BBL_5_2.0",
        "BBM_5_2.0",
        "BBU_5_2.0",
        "BBB_5_2.0",
        "BBP_5_2.0",
        "STOCHk_14_3_3",
        "STOCHd_14_3_3",
        "PSAR_combined",
        "NASDAQ_EMA_10",
        "average_sentiment_score"
    ]

    fields_to_skip_all = [
        # Unneccessary features
        "depreciationDepletionAndAmortization",
        "incomeBeforeTax",
        "netIncomeFromContinuingOperations",
        "comprehensiveIncomeNetOfTax",
        "ebit",
        "costofGoodsAndServicesSold",
        "nonInterestIncome",
        "otherNonOperatingIncome",
        "interestIncome",
        "interestExpense",
        "depreciation",
        "depreciationAndAmortization",
        "changeInOperatingLiabilities",
        "changeInOperatingAssets",
        "capitalExpenditures",
        "changeInReceivables",
        "changeInInventory",
        "paymentsForRepurchaseOfEquity",
        "dividendPayout",
        "dividendPayoutCommonStock",
        "dividendPayoutPreferredStock",
        "accumulatedDepreciationAmortizationPPE",
        "intangibleAssetsExcludingGoodwill",
        "otherCurrentAssets",
        "otherNonCurrentAssets",
        "otherCurrentLiabilities",
        "otherNonCurrentLiabilities",
        "reportedCurrency",
        "sellingGeneralAndAdministrative",
        "researchAndDevelopment",
        "operatingExpenses",
        "investmentIncomeNet",
        "netInterestIncome",
        "incomeTaxExpense",
        "interestAndDebtExpense",
        "proceedsFromOperatingActivities",
        "proceedsFromRepaymentsOfShortTermDebt",
        "paymentsForRepurchaseOfCommonStock",
        "proceedsFromIssuanceOfCommonStock",
        "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet",
        "proceedsFromIssuanceOfPreferredStock",
        "proceedsFromRepurchaseOfEquity",
        "totalAssets",
        "totalCurrentAssets",
        "totalNonCurrentAssets",
        "deferredRevenue",
        "currentAccountsPayable",
        "currentDebt",
        "shortTermDebt",
        "totalShareholderEquity",
        "commonStock",
        "goodwill",
        "intangibleAssets",
        "capitalLeaseObligations",
        "longTermDebtNoncurrent",
        "treasuryStock",
        "retainedEarnings",
        "investments",
        "proceedsFromSaleOfTreasuryStock",
        "changeInExchangeRate",
        "paymentsForOperatingActivities",
        "paymentsForRepurchaseOfPreferredStock",
        "inventory",

        # Testing different feature sets
        # "EMA_30",
        # "NASDAQ_EMA_30",
        # "SP500_Close",
        # "SP500_EMA_10",
        # "SP500_EMA_30",
        # "surprisePercentage",
        # "grossProfit",
        # "totalRevenue",
        # "costOfRevenue",
        # "operatingIncome",
        # "ebitda",
        # "netIncome",
        # "operatingCashflow",
        # "profitLoss",
        # "cashflowFromInvestment",
        # "cashflowFromFinancing",
        # "changeInCashAndCashEquivalents",
        # "cashAndCashEquivalentsAtCarryingValue",
        # "cashAndShortTermInvestments",
        # "currentNetReceivables",
        # "propertyPlantEquipment",
        # "longTermInvestments",
        # "shortTermInvestments",
        # "totalLiabilities",
        # "totalCurrentLiabilities",
        # "totalNonCurrentLiabilities",
        # "longTermDebt",
        # "currentLongTermDebt",
        # "shortLongTermDebtTotal",
        # "commonStockSharesOutstanding",
        # "MCHI_Closing_Price",
        # "VGK_Closing_Price",
        # "EWJ_Closing_Price",
        # "VWO_Closing_Price",
        # "VXUS_Closing_Price",
        # "VT_Closing_Price",
        # "XLY_Closing_Price",
        # "XLP_Closing_Price",
        # "XLE_Closing_Price",
        # "XLF_Closing_Price",
        # "XLV_Closing_Price",
        # "XLI_Closing_Price",
        # "XLB_Closing_Price",
        # "XLRE_Closing_Price",
        # "XLK_Closing_Price",
        # "XLU_Closing_Price",
        # "XLC_Closing_Price",
        # "GDP_value",
        # "GDP_percent_change",
        # "GDP_yearly_percent_change",
        # "GDP_next_release_date",
        # "CPIAUCSL_value",
        # "CPIAUCSL_percent_change",
        # "CPIAUCSL_yearly_percent_change",
        # "CPIAUCSL_next_release_date",
        # "UNRATE_value",
        # "UNRATE_percent_change",
        # "UNRATE_yearly_percent_change",
        # "UNRATE_next_release_date",
        # "FEDFUNDS_value",
        # "FEDFUNDS_percent_change",
        # "FEDFUNDS_yearly_percent_change",
        # "FEDFUNDS_next_release_date",
        # "DGS10_value",
        # "DGS10_percent_change",
        # "DGS10_yearly_percent_change",
        # "DGS10_next_release_date",
        # "CONSUMER_value",
        # "CONSUMER_percent_change",
        # "CONSUMER_yearly_percent_change",
        # "CONSUMER_next_release_date",
        # "INDPRO_value",
        # "INDPRO_percent_change",
        # "INDPRO_yearly_percent_change",
        # "INDPRO_next_release_date",
        # "RSAFS_value",
        # "RSAFS_percent_change",
        # "RSAFS_yearly_percent_change",
        # "RSAFS_next_release_date",
        # "USEPUINDXD_value",
        # "USEPUINDXD_percent_change",
        # "USEPUINDXD_yearly_percent_change",
        # "USEPUINDXD_next_release_date",
        # "article_count",
        # "fiscalDateEnding",
        # "reportedDate",
        # "reportedEPS",
        # "estimatedEPS",
        # "surprise",
        # "nextEarningsReportDate",
        # "nextEstimatedEPS",
        # "BBL_5_2.0",
        # "BBM_5_2.0",
        # "BBU_5_2.0",
        # "BBB_5_2.0",
        # "BBP_5_2.0",
        # "avgTradingVolume",
        # "STOCHk_14_3_3",
        # "STOCHd_14_3_3",
        # "PSAR_combined",
        # "52_week_high",
        # "52_week_low",
        # "NASDAQ_Close",
        # "NASDAQ_EMA_10",
        # "MACDh_12_26_9",
        # "MACDs_12_26_9"
    ]

    dates_to_convert = [
        "GDP_next_release_date",
        "CPIAUCSL_next_release_date",
        "UNRATE_next_release_date",
        "FEDFUNDS_next_release_date",
        "DGS10_next_release_date",
        "CONSUMER_next_release_date",
        "INDPRO_next_release_date",
        "RSAFS_next_release_date",
        "USEPUINDXD_next_release_date",
        "fiscalDateEnding",
        "EarningsReportDate",
        "nextEarningsReportDate"
    ]

    for stock_symbol, dates in data.items():
        formatted_data[stock_symbol] = {}
        percentage_change_data[stock_symbol] = {}
        combined_data[stock_symbol] = {}
        previous_metrics = None  # Initialize previous_metrics

        for date, metrics in dates.items():
            epoch_time = convert_to_epoch(date)
            week_day = get_day_of_week(epoch_time)
            formatted_metrics = {'week_day': week_day}
            percentage_metrics = {'week_day': week_day}
            combined_metrics = {'week_day': week_day}

            for key, value in metrics.items():
                if key in fields_to_skip_all:
                    continue  # Pruning the metrics here
                elif key in dates_to_convert:
                    # Calculate difference between event dates and current date
                    event_epoch_value = convert_to_epoch(value)
                    epoch_diff = event_epoch_value - epoch_time
                    # Convert back to days
                    days_diff = convert_to_days(epoch_diff)
                    formatted_metrics[key] = days_diff
                    percentage_metrics[key] = days_diff
                    combined_metrics[key] = days_diff
                elif isinstance(value, str) and not is_convertible_to_number(value):
                    continue
                elif value is None:
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
                    if previous_metrics and key in previous_metrics and key not in fields_to_skip_percentage and value is not None:
                        change_value = calculate_percentage_change(
                            value, previous_metrics[key]
                        )
                        combined_metrics[f"{key}_change"] = change_value
                        percentage_metrics[f"{key}_change"] = change_value
                    elif key in fields_to_skip_percentage or key in ["average_sentiment_score", "article_count"]:
                        percentage_metrics[key] = value

            formatted_data[stock_symbol][date] = formatted_metrics
            if previous_metrics:
                percentage_change_data[stock_symbol][date] = percentage_metrics
                combined_data[stock_symbol][date] = combined_metrics
            # Update previous_metrics for the next iteration
            previous_metrics = formatted_metrics

    os.makedirs(path, exist_ok=True)

    with open(output_file, 'w') as file:
        json.dump(formatted_data, file, indent=4)

    with open(percentage_change_file, 'w') as file:
        json.dump(percentage_change_data, file, indent=4)

    with open(combined_file, 'w') as file:
        json.dump(combined_data, file, indent=4)


# Define input and output paths
path = 'G:/StockData/master_datasets'
input_path = f'G:/StockData/combined_stock_data.json'
output_path = f'{path}/stock_dataset_absolute_values.json'
percentage_change_output_path = f'{path}/stock_dataset_percent_values.json'
combined_output_path = f'{path}/stock_dataset_absolute_and_percent_values.json'

# Call the function to format the data and calculate percentage changes
format_data(input_path, output_path,
            percentage_change_output_path, combined_output_path)
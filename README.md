
# dYdX Trades and Loans Koinly Format Processor

<br>

## Table of Contents

- [Introduction](#introduction)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

<br>

## Introduction


This dydx_data_processing.py script processes trade data obtained from dYdX, a decentralized trading platform. It includes functions to process transfers, funding, and trades, and generates summaries of buy-side and sell-side loans based on the trade data. The project aims to facilitate analysis and reporting of trading activities on the dYdX platform and processes this data to generate structured output CSV files suitable for importing into Koinly tax software.

This Python script facilitates the filtering and processing of CSV files within a specified date range. It allows users to extract relevant data from CSV files based on date columns and additional criteria.

<br>

## Dependencies

- Python 3.x
- pandas
- numpy
- os

<br>

## Installation

1. Clone the repository to your local machine:
```bash
https://github.com/Jimmy-sha256/dYdX-tax-tools.git
```
<br>

2. Navigate to the project directory:
```bash
cd dYdX-tax-tools
```

<br>

3. Install the required dependencies using pip:
```bash
pip install -r requirements.txt
```

<br>

## Usage

To use the dydx_data_processing.py script, follow these steps:

<br>

* **Place Original Files**

Ensure that your original CSV files are located in the `Original_Files` directory. Rename them `Funding.csv`, `Trades.csv`, `Transfers.csv`.


```bash
dYdX-tax-tools/
│
├── Original_Files/
│ ├── Transfers.csv
│ ├── Funding.csv
│ ├── Trades.csv
│
├── Output/
│ └── ...
│
├── csv_date_filter.py
├── dydx_data_processing.py
├── requirements.txt
└── README.md
```

<br>

* **Instantiate the ProcessedTrades class:**

```python
processed_trades = ProcessedTrades()
```

<br>

* **Process transfers data:**

```python
processed_trades.process_file(
    "Transfers", processed_trades.process_transfers, "Transfers"
)
```
This step reads the Transfers.csv file located in the Original_Files directory, processes it using the process_transfers method, and saves the processed data to Output/dYdX_Transfers.csv.

<br>

* **Process deposit swap data**

```python
processed_trades.process_file(
    "Transfers", processed_trades.process_deposit_swaps, "Deposit_Swaps"
)
```
This step reads the Transfers.csv file located in the Original_Files directory, processes it using the process_deposit_swaps method, and saves the processed data to Output/dYdX_Deposit_Swaps.csv.

<br>

* **Process funding data:**

```python
processed_trades.process_file(
    "Funding", processed_trades.process_funding, "Funding"
)
```

Similarly, this step processes the Funding.csv file and saves the output to Output/dYdX_Funding.csv.

<br>

* **Process trades data:**

```python
processed_trades.process_file(
    "Trades", processed_trades.process_trades, "Trades"
)
```

Processes the Trades.csv file and saves the output to Output/dYdX_Trades.csv.

<br>

* **Process loan data:**

```python
processed_trades.process_trade_data()
processed_trades.process_buy_side_loans()
processed_trades.process_sell_side_loans()
processed_trades.merge_loans()
```

This step involves additional processing of trade data, including calculation of running sums and loan summaries. Generates data frames for buy-side loans. Generates summaries for sell-side loans. Merges the loan summaries and saves them as CSV files in the Output directory.

```bash
dYdX-tax-tools/
│
├── Original_Files/
│ ├── Transfers.csv
│ ├── Funding.csv
│ ├── Trades.csv
│
├── Output/
│ ├── dYdX_USDC_Loans.csv
│ ├── dYdX_Crypto_Loans.csv
│ ├── dYdX_Trades.csv
│ ├── dYdX_Buy_Side_Data/
│
├── csv_date_filter.py
├── dydx_data_processing.py
├── requirements.txt
└── README.md
```

Create a new wallet for dYdX on koinly and upload the csv files generated in the Output/ directory.

<br>

* **Accessing trade data:**

```python
trade_data = processed_trades.access_data("trade", "BTC-USDC")
```

Retrieve trade data for a specific trading pair, e.g., "BTC-USDC".

* **Accessing buy-side data:**

```python
buy_side_data = processed_trades.access_data("buy_side")
```

Retrieve buy-side loan data.

* **Accessing sell-side data:**

```python
sell_side_data = processed_trades.access_data("sell_side", "ETH-USDC")
```

Retrieve sell-side loan data for a specific trading pair, e.g., "ETH-USDC".

* **Saving data:**

Use the following methods to save the processed data into respective folders:

```python
processed_trades.save_data(processed_trades.trade_data, "Trade_Data")
processed_trades.save_data(processed_trades.buy_side_data, "Buy_Side_Data")
processed_trades.save_data(processed_trades.sell_side_data, "Sell_Side_Data")
```
This saves the data into folders named Trade_Data, Buy_Side_Data, and Sell_Side_Data, respectively.


```bash
dYdX-tax-tools/
│
├── Original_Files/
│ ├── Transfers.csv
│ ├── Funding.csv
│ ├── Trades.csv
│
├── Output/
│ ├── dYdX_USDC_Loans.csv
│ ├── dYdX_Crypto_Loans.csv
│ ├── dYdX_Trades.csv
│
├── Buy_Side_Data/
│ ├── BTC-USDC.csv
│ ├── ETH-USDC.csv
│ └── ...
│
├── Sell_Side_Data/
│ ├── BTC-USDC.csv
│ ├── ETH-USDC.csv
│ └── ...
│
├── Trade_Data/
│ ├── BTC-USDC.csv
│ ├── ETH-USDC.csv
│ └── ...
│
├── csv_date_filter.py
├── dydx_data_processing.py
├── requirements.txt
└── README.md
```

The csv_date_filter.py script allows you to slice the processed csv files generated by the dydx_data_processing.py script. To use the csv_date_filter.py follow these steps. 

* **Run The Script**

When prompted, enter the folder path where your CSV files are stored.

```python
Enter the folder path containing CSV files: /Output
```
Enter the start date in the format "YYYY-MM-DD".

```python
Enter the start date (YYYY-MM-DD): 2023-01-01
```

Enter the end date in the format "YYYY-MM-DD".

```python
Enter the end date (YYYY-MM-DD): 2023-12-31
```

* **Review Output**

The script will process each CSV file within the specified date range. Filtered files will be saved in a new directory with a name indicating the folder name, start date, and end date.

## License

This project is licensed under the [MIT License](LICENSE).


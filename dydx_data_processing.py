import pandas as pd
import numpy as np
import os
from datetime import timedelta


class ProcessedTrades:
    def __init__(self):
        # Initialize dictionaries to hold different types of processed data
        self.trade_data = {}  # Holds processed trade data
        self.buy_side_data = {}  # Holds processed buy-side loan data
        self.sell_side_data = {}  # Holds processed sell-side loan data


    # Function to process a file using a specific processing function and save the output
    def process_file(self, file_name, process_func, output_file):

        # Read the input CSV file
        df = pd.read_csv(f"Original_Files/{file_name}.csv")

        # Process the data using the provided function
        df = process_func(df)

        # Save the processed data to an output CSV file
        df.to_csv(f"Output/dYdX_{output_file}.csv", index=True)


    # Processing function for deposit auto swaps
    def process_deposit_swaps(self, df):

        # Create a new dataframe based on the condition
        df = df[df["debitAsset"] != "USDC"][
            [
                "createdAt",
                "debitAmount",
                "debitAsset",
                "creditAmount",
                "creditAsset",
                "transactionHash",
            ]
        ]

        df.columns = [
            "Date",
            "Sent Amount",
            "Sent Currency",
            "Received Amount",
            "Received Currency",
            "TxHash",
        ]

        # Add the additional columns with empty values or specified values
        df["Fee Amount"] = ""
        df["Fee Currency"] = "USDC"
        df["Label"] = "Swap"
        df["Description"] = "Auto Deposit"
        df.set_index("Date", inplace=True)

        return df


    # Processing function for transfers data
    def process_transfers(self, df):

        # Convert 'createdAt' column to datetime format
        df["createdAt"] = pd.to_datetime(
            df["createdAt"].str.replace("T", " ").str.slice(0, -8)
        )

        # Rename columns for consistency
        df.rename(
            columns={
                "createdAt": "Koinly Date",
                "debitAsset": "Currency",
                "transactionHash": "TxHash",
            },
            inplace=True,
        )

        # Set 'Koinly Date' as index
        df.set_index("Koinly Date", inplace=True)

        # Convert 'debitAmount' to string
        df["debitAmount"] = df["debitAmount"].astype(str)

        # Handle negative values for 'FAST_WITHDRAWAL' type
        df.loc[df["type"] == "FAST_WITHDRAWAL", "debitAmount"] = (
            "-" + df.loc[df["type"] == "FAST_WITHDRAWAL", "debitAmount"]
        )

        # Rename 'debitAmount' to 'Amount'
        df["Amount"] = df["debitAmount"]

        # Drop unnecessary columns
        df.drop(
            columns=[
                "type",
                "debitAmount",
            ],
            inplace=True,
        )

        # Sort the dataframe by index
        df.sort_index(inplace=True)

        return df


    # Processing function for funding data
    def process_funding(self, df):

        # Convert 'effectiveAt' column to datetime format
        df["effectiveAt"] = pd.to_datetime(df["effectiveAt"]).dt.normalize()

        # Convert 'payment' column to numeric, handling errors
        df["payment"] = pd.to_numeric(df["payment"], errors="coerce")

        # Aggregate funding data by date and sum payments
        df = df.groupby(df["effectiveAt"])["payment"].sum().reset_index()
        df.columns = ["Koinly Date", "Amount"]

        # Round 'Amount' to 10 decimal places
        df["Amount"] = df["Amount"].round(10)

        # Assign labels based on 'Amount' values
        df["Label"] = df["Amount"].apply(
            lambda x: "Margin Fee" if x < 0 else "Interest Earned"
        )

        # Assign fixed currency and description
        df["Currency"] = "USDC"
        df["Description"] = "Funding"

        # Set 'Koinly Date' as index
        df.set_index("Koinly Date", inplace=True)
        return df


    # Processing function for trade data
    def process_trades(self, df):

        # Convert 'createdAt' column to datetime format
        df["Koinly Date"] = pd.to_datetime(
            df["createdAt"].str.replace("T", " ").str.slice(0, -8)
        )

        # Calculate 'Total' column
        df["Total"] = df["size"] * df["price"]

        # Capitalize 'side' and add 'C' to 'market' column values
        df[["side", "market"]] = df[["side", "market"]].apply(
            lambda x: x.str.capitalize() if x.name == "side" else x + "C"
        )

        # Drop unnecessary columns
        df.drop(["liquidity", "type"], axis=1, inplace=True)

        # Assign 'Fee Currency' column
        df["Fee Currency"] = "USDC"

        # Rename columns for consistency
        df.rename(
            columns={
                "market": "Pair",
                "size": "Amount",
                "price": "Price",
                "side": "Side",
                "fee": "Fee Amount",
            },
            inplace=True,
        )

        # Reorder columns
        df = df[
            [
                "Koinly Date",
                "Pair",
                "Side",
                "Amount",
                "Price",
                "Total",
                "Fee Amount",
                "Fee Currency",
            ]
        ]

        # Adjust 'Total' and 'Amount' for sell trades
        df.loc[df["Side"] == "Sell", ["Total", "Amount"]] *= -1
        df.loc[:, "Fee Amount"] = np.where(
            df["Fee Amount"] > 0, -df["Fee Amount"], df["Fee Amount"]
        )

        # Separate data by unique pairs
        unique_pairs = df["Pair"].unique()

        for pair in unique_pairs:
            pair_df = df[df["Pair"] == pair]
            self.trade_data[pair] = pair_df

        # Set 'Koinly Date' as index
        df = df.set_index("Koinly Date")

        return df


    # Function to generate loan summary from blocks of trade data
    def generate_loan_summary(self, df, side):

        # Filter the DataFrame to include only rows where the "Side" column matches the provided `side` value
        blocks = df[df.groupby("Block")["Side"].transform("first") == side].copy()

        # Calculate the running sum within each block
        # If `side` is "Buy", calculate the running sum of the "Total" column
        # If `side` is "Sell", calculate the running sum of the "Amount" column
        blocks.loc[:, "Running_Sum"] = blocks.groupby("Block")[
            "Total" if side == "Buy" else "Amount"
        ].cumsum()

        # Find the maximum or minimum running sum within each block
        max_min_running_sum = (
            blocks.groupby("Block")["Running_Sum"]
            .agg(
                ["max" if side == "Buy" else "min"]
            )  # Select "max" if side is "Buy", otherwise select "min"
            .reset_index()
        )

        # Create a new DataFrame to store summarized information
        loan_summary_df = (
            blocks.groupby("Block")
            .agg(
                Start_Koinly_Date=(
                    "Koinly Date",
                    "first",
                ),  # First Koinly date within each block
                Last_Koinly_Date=(
                    "Koinly Date",
                    "last",
                ),  # Last Koinly date within each block
                Side=(
                    "Pair",
                    "first",
                ),  # First trading pair encountered within each block
            )
            .reset_index()
        )

        # Merge the maximum or minimum running sum values into the new DataFrame
        loan_summary_df = loan_summary_df.merge(max_min_running_sum, on="Block")

        # Rename the column indicating maximum or minimum running sum to "Amount"
        loan_summary_df.rename(
            columns={"max" if side == "Buy" else "min": "Amount"}, inplace=True
        )

        # Adjust the "Amount" column values
        loan_summary_df["Amount"] = loan_summary_df["Amount"].apply(
            lambda x: x if isinstance(x, str) else str(x).replace("-", "")
        )

        # Adjust the "Side" column values based on the `side` parameter
        if side == "Buy":
            loan_summary_df["Side"] = "USDC"

        elif side == "Sell":
            loan_summary_df["Side"] = loan_summary_df["Side"].apply(
                lambda x: x[:-5]
            )  # Remove last five characters

        # Return the generated loan summary DataFrame
        return loan_summary_df


    # Function to process trade data
    def process_granular_data(self):

        # Iterate over each trading pair in the trade data
        for pair in self.trade_data:

            # Reverse the order of the data for the current pair
            self.trade_data[pair] = self.trade_data[pair][::-1]

            # Calculate the running sum of the "Amount" column for the current pair
            self.trade_data[pair]["Running Sum"] = self.trade_data[pair][
                "Amount"
            ].cumsum()

            # Round the running sum to 8 decimal places for precision
            self.trade_data[pair]["Running Sum"] = self.trade_data[pair][
                "Running Sum"
            ].round(8)

            # Identify rows with zero running sum
            rows_with_zeros = (
                self.trade_data[pair]
                .index[
                    (self.trade_data[pair]["Running Sum"] == 0)
                    | (self.trade_data[pair]["Running Sum"] == -0)
                ]
                .to_list()
            )

            # Create new rows to replace rows with zero running sum
            new_rows = []

            for index, row in self.trade_data[pair].iterrows():

                # Add the current row to the list of new rows
                new_rows.append(row.to_dict())

                # If the current row has zero running sum, add a new row with NaN values for all columns
                if index in rows_with_zeros:
                    new_rows.append(
                        {col: np.nan for col in self.trade_data[pair].columns}
                    )

            # Create a new DataFrame with the modified rows
            new_df = pd.DataFrame(new_rows)

            # Reset the index of the new DataFrame and drop the old index
            new_df.reset_index(drop=True, inplace=True)

            # Assign block numbers to the data based on the presence of null values in the "Koinly Date" column
            new_df["Block"] = new_df["Koinly Date"].isnull().cumsum()

            # Update the trade data with the modified DataFrame for the current pair
            self.trade_data[pair] = new_df

            # Generate loan summaries for buy and sell sides for the current pair
            self.buy_side_data[pair] = self.generate_loan_summary(
                self.trade_data[pair], "Buy"
            )
            self.sell_side_data[pair] = self.generate_loan_summary(
                self.trade_data[pair], "Sell"
            )


    # Function to format loan data
    def format_loan_data(self, df):

        # Calculate start dates by subtracting 1 minute from "Start_Koinly_Date"
        start_dates = (df["Start_Koinly_Date"] - timedelta(minutes=1)).tolist()

        # Calculate last dates by adding 1 minute to "Last_Koinly_Date"
        last_dates = (df["Last_Koinly_Date"] + timedelta(minutes=1)).tolist()

        # Extract sides and values from DataFrame
        sides = df["Side"].tolist()
        values = df["Amount"].tolist()

        # Define labels for loan and repayment
        label_loan = "Margin Loan"
        label_repayment = "Margin Repayment"

        # Create dictionary for receiving loan
        receive_loan = {
            "Date": start_dates,
            "Received Amount": values,
            "Received Currency": sides,
            "Sent Amount": "",
            "Sent Currency": "",
            "Label": label_loan,
        }

        # Create dictionary for loan repayment
        return_loan = {
            "Date": last_dates,
            "Received Amount": "",
            "Received Currency": "",
            "Sent Amount": values,
            "Sent Currency": sides,
            "Label": label_repayment,
        }

        # Create DataFrames from loan receive and repayment dictionaries
        receive_df = pd.DataFrame(receive_loan)
        return_df = pd.DataFrame(return_loan)

        # Concatenate DataFrames for loan receive and repayment
        formatted_df = pd.concat([receive_df, return_df])

        # Sort DataFrame by date
        formatted_df = formatted_df.sort_values(by="Date")

        # Set "Date" column as index
        formatted_df.set_index("Date", inplace=True)
        return formatted_df


    # Function to process loan data
    def process_loan_data(self, data):

        # Iterate over each symbol and its corresponding DataFrame in the provided data
        for symbol, df in data.items():

            # Process the loan data DataFrame using the format_loan_data function
            processed_df = self.format_loan_data(df)

            # Replace the original DataFrame in the data dictionary with the processed DataFrame
            data[symbol] = processed_df


    # Function to process buy-side loans
    def process_buy_side_loans(self):

        # Process loan data for buy-side loans using the process_loan_data function
        self.process_loan_data(self.buy_side_data)


    # Function to process sell-side loans
    def process_sell_side_loans(self):

        # Process loan data for sell-side loans using the process_loan_data function
        self.process_loan_data(self.sell_side_data)


    # Function to merge loan data
    def merge_loans(self):

        # Collect non-empty dataframes for buy and sell sides
        buy_data_values = [
            value for value in self.buy_side_data.values() if not value.empty
        ]
        sell_data_values = [
            value for value in self.sell_side_data.values() if not value.empty
        ]

        # Concatenate dataframes for buy side loans and save to CSV
        merged_buy_df = pd.concat(buy_data_values)
        merged_buy_df.reset_index(inplace=True)  # Reset the index
        merged_buy_df.sort_values(by="Date", inplace=True)  # Sort by date
        merged_buy_df.set_index("Date", inplace=True)  # Set "Date" as index
        merged_buy_df.to_csv("Output/dYdX_USDC_Loans.csv", index=True)  # Save to CSV

        # Concatenate dataframes for sell side loans and save to CSV
        merged_sell_df = pd.concat(sell_data_values)
        merged_sell_df.reset_index(inplace=True)  # Reset the index
        merged_sell_df.sort_values(by="Date", inplace=True)  # Sort by date
        merged_sell_df.set_index("Date", inplace=True)  # Set "Date" as index
        merged_sell_df.to_csv("Output/dYdX_Crypto_Loans.csv", index=True)  # Save to CSV

        # Return the merged DataFrames for buy and sell side loans
        return merged_buy_df, merged_sell_df


    # Instance method to access data DataFrame by type
    def access_data(self, data_type, pair=None):

        # Determine the data dictionary based on the specified data type
        if data_type == "trade":
            data_dict = self.trade_data
        elif data_type == "buy_side":
            data_dict = self.buy_side_data
        elif data_type == "sell_side":
            data_dict = self.sell_side_data
        else:

            # Raise a ValueError if an invalid data type is provided
            raise ValueError(
                "Invalid data type. Choose from 'trade', 'buy_side', or 'sell_side'."
            )

        # If pair is specified, return the DataFrame corresponding to that pair
        if pair:
            return data_dict.get(pair)

        # Otherwise, return the entire data dictionary
        return data_dict


    # Instance method to save all data DataFrames into a folder
    def save_data(self, data_dict, directory):

        # Create the directory if it does not exist
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Iterate through each pair and DataFrame in the data dictionary
        for pair, df in data_dict.items():

            # Check if the DataFrame is not empty
            if not df.empty:

                # Construct the file name based on the pair and directory
                file_name = os.path.join(directory, f"{pair}.csv")

                # Save the DataFrame to a CSV file in the specified directory
                df.to_csv(file_name, index=True)


# Create an instance of the ProcessedTrades class
processed_trades = ProcessedTrades()

# Process transfers data
processed_trades.process_file(
    "Transfers", processed_trades.process_transfers, "Transfers"
)

# Process deposit auto swaps
processed_trades.process_file(
    "Transfers", processed_trades.process_deposit_swaps, "Deposit_Swaps"
)

# Process funding data
processed_trades.process_file(
    "Funding", processed_trades.process_funding, "Funding"
)

# Process trade data
processed_trades.process_file(
    "Trades", processed_trades.process_trades, "Trades"
)

# Process granular blocks of trades data
processed_trades.process_granular_data()

# Process buy-side loans
processed_trades.process_buy_side_loans()

# Process sell-side loans
processed_trades.process_sell_side_loans()

# Merge and save loan data frames
processed_trades.merge_loans()


# Accessing trade data
trade_data = processed_trades.access_data("trade", "BTC-USDC")

# Accessing buy-side data
buy_side_data = processed_trades.access_data("buy_side", "FYI-USDC")

# Accessing sell-side data
sell_side_data = processed_trades.access_data("sell_side", "ETH-USDC")


# Call the method to save the trade data into the "Trade_Data" folder
processed_trades.save_data(processed_trades.trade_data, "Trade_Data")

# Call the method to save the buy-side data into the "Buy_Side_Data" folder
processed_trades.save_data(processed_trades.buy_side_data, "Buy_Side_Data")

# Call the method to save the sell-side data into the "Sell_Side_Data" folder
processed_trades.save_data(processed_trades.sell_side_data, "Sell_Side_Data")

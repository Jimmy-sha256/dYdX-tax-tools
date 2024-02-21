import os
import pandas as pd
from collections import Counter

def filter_csv_files(folder_path, start_date, end_date):
    # Create a directory to save filtered files
    folder_name = os.path.basename(folder_path)  # Extract the folder name from the folder path

    # Generate a name for the output directory based on folder name, start date, and end date
    output_dir = f"{folder_name}_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}" 

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)  

    # Loop through each file in the specified folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):  # Check if the file is a CSV file
            file_path = os.path.join(folder_path, filename)  # Get the full file path
            df = pd.read_csv(file_path, skip_blank_lines=False)  # Read CSV file into a DataFrame

            # Check for the presence of date column either named 'Date' or 'Koinly Date'
            date_column = None
            for col in ["Date", "Koinly Date"]:
                if col in df.columns:
                    date_column = col  # Found the date column
                    break

            if date_column is None:
                print(f"Date column not found in {filename}")
                continue

            # Convert the date column to datetime format
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

            # Iterate over each row and keep track of rows within the specified date range or blank rows
            filtered_rows = []
            for i, row in df.iterrows():
                if pd.isnull(row[date_column]) or (
                    row[date_column].date() >= start_date.date()
                    and row[date_column].date() <= end_date.date()
                ):
                    filtered_rows.append(row)

            # Create a DataFrame with filtered rows
            filtered_df = pd.DataFrame(filtered_rows)

            if not filtered_df.empty:  # Check if there are rows left after filtering
                # Set the date column as the index
                filtered_df.set_index(date_column, inplace=True)

                # Remove 'Unnamed: 0' column if it exists
                if "Unnamed: 0" in filtered_df.columns:
                    filtered_df.drop("Unnamed: 0", axis=1, inplace=True)

                if "Block" in filtered_df.columns:
                    # Count occurrences of values in the 'Block' column
                    block_counts = Counter(filtered_df["Block"])

                    # Find start and end of recurring block values
                    start_block, end_block = None, None
                    for block, count in block_counts.items():
                        if count > 1:
                            if start_block is None or block < start_block:
                                start_block = block
                            if end_block is None or block > end_block:
                                end_block = block

                    # Filter rows based on recurring block values
                    filtered_df = filtered_df[
                        (filtered_df["Block"] >= start_block)
                        & (filtered_df["Block"] <= end_block)
                    ]

                    if not filtered_df.empty:

                        # Check if the first row has NaN values for every column except 'Block'
                        if filtered_df.iloc[0].drop("Block").isnull().all():

                            # Drop the first row if all values except 'Block' are NaN
                            filtered_df = filtered_df.iloc[1:]

                if not filtered_df.empty:  # Check if there are rows left after further processing
                    filtered_file_path = os.path.join(output_dir, f"{filename}")
                    filtered_df.to_csv(filtered_file_path)  # Save filtered DataFrame to a new CSV file
                    print(f"Filtered file saved as: {filtered_file_path}")
                else:
                    print(f"No data left after processing in {filename}")
            else:
                print(f"No data found between {start_date} and {end_date} in {filename}")

if __name__ == "__main__":

    # Get input folder path and date range from user
    folder_path = input("Enter the folder path containing CSV files: ")
    start_date = pd.to_datetime(input("Enter the start date (YYYY-MM-DD): "))
    end_date = pd.to_datetime(input("Enter the end date (YYYY-MM-DD): "))

    # Call the function to filter CSV files within the specified date range
    filter_csv_files(folder_path, start_date, end_date)

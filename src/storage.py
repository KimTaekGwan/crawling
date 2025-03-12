"""
Module for storing scraped data in CSV format.
"""

import csv
import os
from typing import List, Dict
import src.config as config


def ensure_data_dir():
    """Ensure the data directory exists."""
    os.makedirs(config.DATA_DIR, exist_ok=True)


def save_to_csv(data: List[Dict[str, str]], filename: str, headers=None):
    """
    Save data to a CSV file.

    Args:
        data: List of dictionaries containing the data to save
        filename: Name of the CSV file to save to
        headers: Column headers for the CSV file (defaults to config.CSV_HEADERS)
    """
    if not data:
        print(f"No data to save to {filename}")
        return

    ensure_data_dir()
    filepath = os.path.join(config.DATA_DIR, filename)

    # Check if file exists to decide whether to write headers
    file_exists = os.path.isfile(filepath)

    if headers is None:
        headers = config.CSV_HEADERS

    try:
        with open(filepath, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            # Write headers only if the file doesn't exist yet
            if not file_exists:
                writer.writeheader()

            # Write the data rows
            writer.writerows(data)

        print(f"Successfully saved {len(data)} records to {filepath}")

    except Exception as e:
        print(f"Error saving data to {filename}: {e}")


def save_page_data(search_query: str, page_num: int, data: List[Dict[str, str]]):
    """
    Save data from a specific page to the keyword-specific CSV and append to the combined CSV.

    Args:
        search_query: The search query used
        page_num: The page number where the data was scraped from
        data: The scraped data to save
    """
    # Save to the keyword-specific CSV file
    output_filename = config.OUTPUT_FILE_NAME_TEMPLATE.format(
        search_query.replace("@", "")
    )
    save_to_csv(data, output_filename)

    # Add keyword to each row for the all.csv file
    all_data = []
    for item in data:
        # Create a new dict with keyword field
        all_item = {"Keyword": search_query, **item}
        all_data.append(all_item)

    # Save to the combined CSV file with the keyword column
    save_to_csv(all_data, config.ALL_DATA_FILE_NAME, config.ALL_CSV_HEADERS)

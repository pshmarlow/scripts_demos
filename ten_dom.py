#! /usr/bin/python

import glob
import json
from datetime import datetime

def epoch_to_datetime(epoch_time):
    """Convert epoch time to a datetime string in the specified format."""
    return datetime.utcfromtimestamp(int(epoch_time) / 1000).strftime("%Y/%m/%d %H:%M:%S")

def process_line(line):
    """Process a single line of data, splitting by tab."""
    parts = line.strip().split('\t')
    return parts

def process_file(file_path):
    """Process a primary data file to extract and transform data."""
    rows = []  # List to hold dictionaries for each row
    start_reading = False
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith("COPY "):
                start_reading = True
                continue
            if line.startswith("\\."):
                break
            if start_reading:
                parts = process_line(line)
                parts += [''] * (7 - len(parts))  # Padding for missing columns
                row = {
                    "ID": parts[0],
                    "Name": parts[1],
                    "Description": parts[2],
                    "EPS": parts[3],
                    "FPM": parts[4],
                    "Deleted": parts[5],
                    "Edit Date": epoch_to_datetime(parts[6]) if parts[6] else ''
                }
                rows.append(row)
    return rows

def process_domains_file(file_path):
    """Process a domains data file to extract additional information."""
    domain_data = {}
    start_reading = False
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith("COPY "):
                start_reading = True
                continue
            if line.startswith("\\."):
                break
            if start_reading:
                parts = line.strip().split('\t')
                if len(parts) >= 6:
                    key = parts[5]  # Use column 6 as the key
                    value = (parts[0], epoch_to_datetime(parts[4]) if len(parts) > 4 else '')  # Extract columns 1 and 5
                    domain_data[key] = value
    return domain_data

def main():
    # Process primary data files
    files = glob.glob('DB_Dumps/tenant*.sql')
    all_rows = []
    for file_path in files:
        file_rows = process_file(file_path)
        all_rows.extend(file_rows)

    # Process domains data files and merge with primary data
    domain_files = glob.glob('DB_Dumps/domains*.sql')
    for file_path in domain_files:
        domains_data = process_domains_file(file_path)
        for row in all_rows:
            domain_info = domains_data.get(row["ID"])
            if domain_info:
                row["second_id"], row["second_edit_date"] = domain_info
            else:
                row["second_id"], row["second_edit_date"] = '', ''

    # Define headers, including new columns from domain data
    headers = [
        {"key": "ID", "header": "Header ID"},
        {"key": "Name", "header": "Header Name"},
        {"key": "Description", "header": "Header Description"},
        {"key": "EPS", "header": "Header EPS"},
        {"key": "FPM", "header": "Header FPM"},
        {"key": "Deleted", "header": "Header Deleted"},
        {"key": "Edit Date", "header": "Header Edit Date"},
        {"key": "second_id", "header": "Second ID"},
        {"key": "second_edit_date", "header": "Second Edit Date"}
    ]

    # Combine data and headers for final output
    combined = {"data": all_rows, "headers": headers}
    print(json.dumps(combined, indent=4))

if __name__ == "__main__":
    main()

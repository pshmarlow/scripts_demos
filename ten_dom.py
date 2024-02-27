#! /usr/bin/python

import glob
import json
from datetime import datetime

def process_file(file_path):
    data = []
    start_reading = False
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if line.startswith("COPY "):
                start_reading = True
                continue
            if line.startswith("\\."):
                break
            if start_reading:
                parts = line.strip().split("\t")
                # Handle empty values
                parts = [part if part != '' else None for part in parts]
                # Convert epoch to datetime for edit date columns
                if 'tenant' in file_path:
                    edit_date_index = 6
                else: # domains file
                    edit_date_index = 4
                if parts[edit_date_index] is not None:
                    parts[edit_date_index] = datetime.fromtimestamp(int(parts[edit_date_index])/1000).strftime("%Y-%m-%dT%H:%M:%S")
                data.append(parts)
    return data

def main():
    tenant_files = glob.glob('DB_Dumps/tenant*.sql')
    domain_files = glob.glob('DB_Dumps/domains*.sql')
    tenant_data = []
    domain_data = []

    # Process tenant and domain files
    for file_path in tenant_files:
        tenant_data.extend(process_file(file_path))
    for file_path in domain_files:
        domain_data.extend(process_file(file_path))

    # Combine data based on tenant ID and domain tenant_id
    combined_data = []
    for tenant_row in tenant_data:
        tenant_id = tenant_row[0]
        for domain_row in domain_data:
            if domain_row[-1] == tenant_id:  # Match found based on tenant ID
                combined_row = tenant_row + domain_row[:-1]  # Exclude redundant tenant_id from domain data
                combined_data.append(combined_row)
                break

    # Convert combined data to the desired dictionary format
    data_dicts = []
    for row in combined_data:
        data_dict = {
            "ID": row[0],
            "Name": row[1],
            "Description": row[2],
            "EPS": row[3],
            "FPM": row[4],
            "Deleted": row[5],
            "Edit Date": row[6],
            "domains_id": row[7],
            "domains_name": row[8],
            "domains_description": row[9],
            "domains_deleted": row[10],
            "domain_editdate": row[11]
        }
        data_dicts.append(data_dict)

    # Prepare headers
    headers = [
        {"key": "ID", "header": "Header ID"},
        {"key": "Name", "header": "Header Name"},
        # Add other headers as needed
    ]

    # Print combined data and headers
    combined = {"data": data_dicts, "headers": headers}
    print(json.dumps(combined, indent=4))

if __name__ == "__main__":
    main()

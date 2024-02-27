#! /usr/bin/python

import glob
import json
from datetime import datetime

def process_line(line):
    # Split the line by tab, handling empty values correctly
    return line.strip().split("\t")

def epoch_to_datetime(epoch):
    # Convert epoch to datetime in the specified format
    return datetime.fromtimestamp(int(epoch) / 1000).strftime("%Y/%m/%d %H:%M:%S")

def process_file(file_path):
    start_reading = False
    rows = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if line.startswith("COPY "):
                start_reading = True
                continue
            if line.startswith("\\."):
                break
            if start_reading:
                parts = process_line(line)
                # Convert epoch to datetime for Edit Date
                if len(parts) > 6:  # For tenant files
                    parts[6] = epoch_to_datetime(parts[6])
                if len(parts) > 4:  # For domains files
                    parts[4] = epoch_to_datetime(parts[4])
                rows.append(parts)
    return rows

def merge_data(tenants, domains):
    # Convert domains list to a dictionary for easy access
    domains_dict = {row[5]: row for row in domains}
    
    merged_data = []
    for tenant in tenants:
        tenant_id = tenant[0]
        domain_data = domains_dict.get(tenant_id, [""] * 6)  # Default to empty strings
        merged_row = tenant + domain_data
        merged_data.append(merged_row)
    
    # Add domains data that doesn't match any tenant
    tenant_ids = set(t[0] for t in tenants)
    for domain in domains:
        if domain[5] not in tenant_ids:
            merged_data.append([""] * 7 + domain)  # Prepend with empty tenant data

    return merged_data

def main():
    tenant_files = glob.glob('DB_Dumps/tenant*.sql')
    domain_files = glob.glob('DB_Dumps/domains*.sql')
    all_tenants = []
    all_domains = []

    for file_path in tenant_files:
        all_tenants.extend(process_file(file_path))
    
    for file_path in domain_files:
        all_domains.extend(process_file(file_path))

    merged_rows = merge_data(all_tenants, all_domains)

    # Prepare data for JSON output
    final_data = []
    for row in merged_rows:
        row_dict = {
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
            "domain_editdate": row[11],
            "tenant_id": row[12],
        }
        final_data.append(row_dict)

    # Print the final data
    print(json.dumps(final_data, indent=4))

if __name__ == "__main__":
    main()

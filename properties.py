#! /usr/bin/python3

import re
import os
from collections import defaultdict

logDirectory = os.getcwd()

DBText = logDirectory + "/DB_Text/"
DBDump = logDirectory + "/DB_Dumps/"

for File in os.listdir(DBText):
    if re.search("ariel_property_view.*", File):
        propertyview = File
        propertyviewpath = DBText + "/" + propertyview
        break
    else: propertyviewpath = ""

for File in os.listdir(DBDump):
    if re.search("tenant.*.sql", File):
        tenants = File
        tenantspath = DBDump + "/" + tenants
        break
    else: tenantspath = ""

for File in os.listdir(DBDump):
    if re.search("ariel_indexes.*.sql", File):
        indexes = File
        indexespath = DBDump + "/" + indexes
        break
    else: indexespath = ""


# Function to read and process the word file
def process_word_file(propertyviewpath):
    # Open and read the content of the word file
    with open(propertyviewpath, 'r') as file:
        content = file.read()

    # Split the content into individual records
    records = re.split(r'-\[ RECORD \d+ \]----', content)[1:] # Ignore the first split result as it will be empty


    count_events = 0
    count_flows = 0
    count_events_expressions = 0
    count_flows_expressions = 0
    count_forceparse_t = 0
    count_forceparse_f = 0
    count_forceparse_t_events = 0
    count_forceparse_t_flows = 0

    unique_propertynames_events = set()
    unique_propertynames_flows = set()
    unique_propertynames_events_expressions = set()
    unique_propertynames_flows_expressions = set()
    unique_propertynames_forceparse_t = set()
    unique_propertynames_forceparse_f = set()
    unique_propertynames_forceparse_t_events = set()
    unique_propertynames_forceparse_t_flows = set()

    tenant_property_counts = defaultdict(set)
    devicetype_properties = defaultdict(set)

    for record in records:
        # Split the record into lines and parse the properties
        lines = record.strip().split('\n')
        properties = {}
        for line in lines:
            if '|' in line:
                key, value = line.split('|', 1)
                properties[key.strip()] = value.strip()

        if properties.get('enabled') == 't' and properties.get('database') == 'events':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_events:
                unique_propertynames_events.add(propertyname)
                count_events += 1

        if properties.get('enabled') == 't' and properties.get('database') == 'flows':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_flows:
                unique_propertynames_flows.add(propertyname)
                count_flows += 1


        if properties.get('enabled') == 't' and properties.get('database') == 'events':
            propertyname = properties.get('expressionid')
            if propertyname and propertyname not in unique_propertynames_events_expressions:
                unique_propertynames_events_expressions.add(propertyname)
                count_events_expressions += 1

        if properties.get('enabled') == 't' and properties.get('database') == 'flows':
            propertyname = properties.get('expressionid')
            if propertyname and propertyname not in unique_propertynames_flows_expressions:
                unique_propertynames_flows_expressions.add(propertyname)
                count_flows_expressions += 1


        if properties.get('enabled') == 't' and properties.get('forceparse') == 't':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_forceparse_t:
                unique_propertynames_forceparse_t.add(propertyname)
                count_forceparse_t += 1

        if properties.get('enabled') == 't' and properties.get('forceparse') == 'f':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_forceparse_f:
                unique_propertynames_forceparse_f.add(propertyname)
                count_forceparse_f += 1

        if properties.get('enabled') == 't' and properties.get('forceparse') == 't' and properties.get('database') == 'events':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_forceparse_t_events:
                unique_propertynames_forceparse_t_events.add(propertyname)
                count_forceparse_t_events += 1

        if properties.get('enabled') == 't' and properties.get('forceparse') == 't' and properties.get('database') == 'flows':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_forceparse_t_flows:
                unique_propertynames_forceparse_t_flows.add(propertyname)
                count_forceparse_t_flows += 1

        if properties.get('enabled') == 't':
            propertyname = properties.get('propertyname')
            tenant_id = properties.get('tenant_id', 'N/A')
            devicetypedescription = properties.get('devicetypedescription', 'N/A')
            if propertyname:
                tenant_property_counts[tenant_id].add(propertyname)
                devicetype_properties[devicetypedescription].add(propertyname)


    tenant_counts = {}
    if len(tenant_property_counts) == 1 and '0' in tenant_property_counts:
        tenant_counts['N/A'] = len(tenant_property_counts['0'])
    else:
        for tenant_id, properties in tenant_property_counts.items():
            tenant_counts[tenant_id] = len(properties)

    devicetypedescription_counts = {devicetype: len(properties) for devicetype, properties in devicetype_properties.items()}

    return count_events, count_flows, count_events_expressions, count_flows_expressions, count_forceparse_t, count_forceparse_f, count_forceparse_t_events, count_forceparse_t_flows, tenant_counts, devicetypedescription_counts

def process_ariel_indexes(indexespath):
    count_events = -1
    count_flows = -1
    in_copy_section = False

    with open(indexespath) as file:
        for line in file:
            line = line.strip()
            if line.startswith("COPY public"):
                in_copy_section = True
                continue
            if line == "\.":
                in_copy_section = False

            if in_copy_section:
                columns = line.split('\t')
                if len(columns) > 1:
                    if columns[1] == "events":
                        count_events += 1
                    elif columns[1] == "flows":
                        count_flows += 1
    if count_flows == -1:
        count_flows = 0
    return count_events, count_flows


# File path to the word file
#file_path = 'DB_Text/ariel_property_view.20240510.txt'
#propertyviewpath = 'DB_Text/ariel_property_view.{30000000..1}.txt'

# Process the file and get the counts
count_events, count_flows, count_events_expressions, count_flows_expressions, count_forceparse_t, count_forceparse_f, count_forceparse_t_events, count_forceparse_t_flows, tenant_counts, devicetypedescription_counts = process_word_file(propertyviewpath)
ariel_index_events, ariel_index_flows = process_ariel_indexes(indexespath)

# Print the results
print(f"Count of unique enabled events: {count_events}")
print(f"Count of unique enabled flows: {count_flows}")
print(f"Count of unique expressions for enabled events: {count_events_expressions}")
print(f"Count of unique expressions for enabled flows: {count_flows_expressions}")
print(f"Count of unique enabled forceparsed: {count_forceparse_t}")
print(f"Count of unique enabled non-forceparsed: {count_forceparse_f}")
print("Tenant Counts:")
for tenant_id, count in tenant_counts.items():
    print(f"Tenant ID: {tenant_id}, Count of unique properties: {count}")

'''
print(f"Count of unique enabled forceparsed events: {count_forceparse_t_events}")
print(f"Count of unique enabled forceparsed flows: {count_forceparse_t_flows}")
print(f"Count of indexed properties for events: {ariel_index_events}")
print(f"Count of indexed properties for flows: {ariel_index_flows}")

print("Properties per log source type:")
for devicetypedescription, count in devicetypedescription_counts.items():
    print(f"Log source type: {devicetypedescription} - {count}")
'''

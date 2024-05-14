import re

# Function to read and process the word file
def process_word_file(file_path):
    # Open and read the content of the word file
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Split the content into individual records
    records = re.split(r'-\[ RECORD \d+ \]----', content)[1:]  # Ignore the first split result as it will be empty
    
    count_events = 0
    count_flows = 0
    unique_propertynames_events = set()
    unique_propertynames_flows = set()

    for record in records:
        # Split the record into lines and parse the properties
        lines = record.strip().split('\n')
        properties = {}
        for line in lines:
            if '|' in line:
                key, value = line.split('|', 1)
                properties[key.strip()] = value.strip()
        
        # Check the conditions for events
        if properties.get('enabled') == 't' and properties.get('database') == 'events':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_events:
                unique_propertynames_events.add(propertyname)
                count_events += 1
        
        # Check the conditions for flows
        if properties.get('enabled') == 't' and properties.get('database') == 'flows':
            propertyname = properties.get('propertyname')
            if propertyname and propertyname not in unique_propertynames_flows:
                unique_propertynames_flows.add(propertyname)
                count_flows += 1
    
    return count_events, count_flows

# File path to the word file
file_path = 'path/to/your/wordfile.txt'

# Process the file and get the counts
count_events, count_flows = process_word_file(file_path)

# Print the results
print(f"Count of unique enabled events: {count_events}")
print(f"Count of unique enabled flows: {count_flows}")

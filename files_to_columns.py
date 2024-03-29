import json

# File paths and their identifiers
file_paths = {
    '/first/location/conf': 'conf',
    '/second/location/file_two': 'file_two',
    '/third/location/conf_three': 'conf_three'
}

# Description mapping
key_to_description = {
    'EXAMPLE_KEY_ONE': 'Description One',
    'EXAMPLE_KEY_TW0': 'Description Two',
    'EXAMPLE_KEY_THREE': 'Description Three',
    'StringOne': 'Description Four',
    'StringTwo': 'Description Five',
    'StringThree': 'Description Six',
    # Add mappings for unique keys from conf_three if necessary
}

# Initialize a dictionary to hold the data
data = {'ID': [], 'source': [], 'key': [], 'value': [], 'description': []}

# Initialize ID counter
id_counter = 0

# Function to parse key-value pairs from the conf files
def parse_conf_file(path, identifier, is_third_file=False):
    global id_counter
    with open(path, 'r') as file:
        for line in file:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                # For the third file, check if the key-value pair is unique
                if is_third_file:
                    if (key, value) not in conf_contents:
                        add_to_data(key, value, identifier)
                else:
                    conf_contents.add((key, value))  # Store key-value pairs from the first file
                    add_to_data(key, value, identifier)

# Function to parse key-value pairs from the second file
def parse_file_two(path, identifier):
    global id_counter
    with open(path, 'r') as file:
        for line in file:
            parts = line.strip().split(' ', 1)
            key = parts[0]
            value = parts[1] if len(parts) > 1 else ''
            add_to_data(key, value, identifier)

# Helper function to add data to the structure
def add_to_data(key, value, source):
    global id_counter
    data['ID'].append(str(id_counter))
    data['source'].append(source)
    data['key'].append(key)
    data['value'].append(value)
    data['description'].append(key_to_description.get(key, ''))
    id_counter += 1

# Store the contents of the first conf file to identify unique lines in the third conf file
conf_contents = set()

# Parse the files
parse_conf_file('/first/location/conf', file_paths['/first/location/conf'])
parse_file_two('/second/location/file_two', file_paths['/second/location/file_two'])
parse_conf_file('/third/location/conf_three', file_paths['/third/location/conf_three'], is_third_file=True)

# Generate JSON output
json_output = json.dumps(data, indent=4)

# Print or save the JSON output
print(json_output)

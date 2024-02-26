import json

# File paths
conf_file_path = '/first/location/conf'
file_two_path = '/second/location/file_two'
conf_three_path = '/third/location/conf_three'

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
data = {'ID': [], 'key': [], 'key_two': [], 'key_three': [], 'value': [], 'description': []}

# Initialize ID counter
id_counter = 0

# Function to parse key-value pairs from the conf files
def parse_conf_file(path, is_third_file=False):
    global id_counter
    with open(path, 'r') as file:
        for line in file:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                # For the third file, check if the key-value pair is unique
                if is_third_file:
                    if (key, value) not in conf_contents:
                        data['ID'].append(str(id_counter))
                        data['key'].append('')  # Empty for third file
                        data['key_two'].append('')  # Empty for third file
                        data['key_three'].append(key)
                        data['value'].append(value)
                        data['description'].append(key_to_description.get(key, ''))
                        id_counter += 1
                else:
                    conf_contents.add((key, value))  # Store key-value pairs from the first file
                    data['ID'].append(str(id_counter))
                    data['key'].append(key)
                    data['key_two'].append('')  # Empty value for 'key_two' and 'key_three' columns
                    data['key_three'].append('')
                    data['value'].append(value)
                    data['description'].append(key_to_description.get(key, ''))
                    id_counter += 1

# Function to parse key-value pairs from the second file
def parse_file_two(path):
    global id_counter
    with open(path, 'r') as file:
        for line in file:
            parts = line.strip().split(' ', 1)
            key_two = parts[0]
            value = parts[1] if len(parts) > 1 else ''
            if key_two in key_to_description:
                data['ID'].append(str(id_counter))
                data['key'].append('')  # Empty value for 'key' column
                data['key_two'].append(key_two)
                data['key_three'].append('')  # Empty for second file
                data['value'].append(value)
                data['description'].append(key_to_description[key_two])
                id_counter += 1

# Store the contents of the first conf file to identify unique lines in the third conf file
conf_contents = set()

# Parse the files
parse_conf_file(conf_file_path)
parse_file_two(file_two_path)
parse_conf_file(conf_three_path, is_third_file=True)

# Generate JSON output
json_output = json.dumps(data, indent=4)

# Print or save the JSON output
print(json_output)

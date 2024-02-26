import json

# File paths
conf_file_path = '/first/location/conf'
file_two_path = '/second/location/file_two'

# Description mapping
key_to_description = {
    'EXAMPLE_KEY_ONE': 'Description One',
    'EXAMPLE_KEY_TW0': 'Description Two',
    'EXAMPLE_KEY_THREE': 'Description Three',
    'StringOne': 'Description Four',
    'StringTwo': 'Description Five',
    'StringThree': 'Description Six',
}

# Initialize a dictionary to hold the data
data = {'ID': [], 'key': [], 'key_two': [], 'value': [], 'description': []}

# Initialize ID counter
id_counter = 0

# Function to parse key-value pairs from the conf file
def parse_conf_file(path):
    global id_counter
    with open(path, 'r') as file:
        for line in file:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                data['ID'].append(str(id_counter))
                data['key'].append(key)
                data['key_two'].append('')  # Empty value for 'key_two' column
                data['value'].append(value)
                if key in key_to_description:
                    data['description'].append(key_to_description[key])
                else:
                    data['description'].append('')
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
                data['value'].append(value)
                data['description'].append(key_to_description[key_two])
                id_counter += 1

# Parse the files
parse_conf_file(conf_file_path)
parse_file_two(file_two_path)

# Generate JSON output
json_output = json.dumps(data, indent=4)

# Print or save the JSON output
print(json_output)

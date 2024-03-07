import re
import json
import gzip
import datetime
from glob import glob

# OOM Regular expression patterns
oom_patterns = [
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+\S+\s+OutOfMemoryMonitor\[\d+\]: '
        r'Discovered out-of-memory error for (?P<service>[^(]+)\(type.*'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+\S+\s+OutOfMemoryMonitor\[\d+\]: '
        r'Discovered out-of-memory error for (?P<service>\S+)\s+process\.'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[(?P<service>Thread-\d+)\]\s+java.lang.OutOfMemoryError'
    )
]

# TxSentry Regular expression patterns
txsentry_patterns = [
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*TxSentry: .* Found unmanaged process on host \d+\.\d+\.\d+\.\d+: (?P<service>/usr/bin/httpd), pid=.*'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*TxSentry: .* TX on host \d+\.\d+\.\d+\.\d+:  pid=.* query=\'(?P<query>.+)\''
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*TxSentry: .* Found a process on host \d+\.\d+\.\d+\.\d+: (?P<service>ecs-ep), pid=.*'
    )
]

# Function to process a single file
def process_file(file_path, events_oom, events_txsentry, seen_events):
    open_func = gzip.open if file_path.endswith('.gz') else open
    with open_func(file_path, 'rt') as file:
        for line in file:
            # Check for OOM patterns
            for pattern in oom_patterns:
                match = pattern.search(line)
                if match:
                    date_str = match.group('date')
                    datetime_obj = datetime.datetime.strptime(date_str, '%b %d %H:%M:%S')
                    service_name = match.group('service')
                    event_key = (datetime_obj, service_name, 'OOM')
                    if event_key not in seen_events:
                        seen_events.add(event_key)
                        events_oom.append({
                            'DateTime': datetime_obj,
                            'service': service_name,
                        })
                    break  # Move to next line if any OOM pattern matches

            # Check for TxSentry patterns
            for pattern in txsentry_patterns:
                match = pattern.search(line)
                if match:
                    date_str = match.group('date')
                    datetime_obj = datetime.datetime.strptime(date_str, '%b %d %H:%M:%S')
                    service_name = match.group('service') if 'service' in match.groupdict() else ""
                    query = match.group('query') if 'query' in match.groupdict() else ""
                    event_key = (datetime_obj, service_name, query, 'TxSentry')
                    if event_key not in seen_events:
                        seen_events.add(event_key)
                        events_txsentry.append({
                            'DateTime': datetime_obj,
                            'service': service_name,
                            'query': query,
                        })
                    break  # Move to next line if any TxSentry pattern matches

# Define headers for both event types
headers_oom = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_txsentry = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}, {"key": "query", "header": "Query"}]

def main():
    events_oom = []
    events_txsentry = []
    seen_events = set()
    # Process current and archived log files
    process_file('var/log/qradar.error', events_oom, events_txsentry, seen_events)
    for zipped_file in glob('var/log/qradar.old/qradar.error*.gz'):
        process_file(zipped_file, events_oom, events_txsentry, seen_events)
    
    # Sort and prepare events for both OOM and TxSentry
    for event_list, headers in [(events_oom, headers_oom), (events_txsentry, headers_txsentry)]:
        event_list.sort(key=lambda event: event['DateTime'])
        for idx, event in enumerate(event_list):
            event['time_date'] = event['DateTime'].strftime("%m/%d %H:%M:%S")
            event['id'] = str(idx)
            del event['DateTime']
    
    # Prepare final output
    final_output = {
        "OOM": {"data": events_oom, "headers": headers_oom},
        "TxSentry": {"data": events_txsentry, "headers": headers_txsentry}
    }
    
    print(json.dumps(final_output))

if __name__ == '__main__':
    main()

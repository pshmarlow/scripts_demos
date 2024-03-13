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
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*?\[hostcontext.hostcontext\]  \[(?P<thread_key>[^/]+)\/Sequential.*?TxSentry: .*? Found unmanaged process on host \d+\.\d+\.\d+\.\d+: (?P<service>/usr/bin/httpd), pid=.*'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*?\[hostcontext.hostcontext\]  \[(?P<thread_key>[^/]+)\/Sequential.*?TxSentry: .*? TX on host \d+\.\d+\.\d+\.\d+:  pid=.* query=\'(?P<query>.+)\''
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*?\[hostcontext.hostcontext\]  \[(?P<thread_key>[^/]+)\/Sequential.*?TxSentry: .*? Found a process on host \d+\.\d+\.\d+\.\d+: (?P<service>ecs-ep), pid=.*'
    )
]

def process_file(file_path, events_oom, events_txsentry, seen_events):
    if file_path.endswith('.gz'):
        open_func = lambda f, mode: gzip.open(f, mode='rt', encoding='utf-8', errors='ignore')
        last_event = None
    else:
        open_func = lambda f, mode: open(f, mode, encoding='utf-8', errors='ignore')

    last_event = None
    with open_func(file_path, 'rt') as file:
        for line in file:
            try:
                # Check for OOM patterns
                for pattern in oom_patterns:
                    match = pattern.search(line)
                    if match:
                        process_oom_event(match, events_oom, seen_events)
                        last_event = None  # Reset last event
                        break  # Move to next line if any OOM pattern matches
            
                # Check for TxSentry patterns
                for pattern_index, pattern in enumerate(txsentry_patterns):
                    match = pattern.search(line)
                    if match:
                        # Process TxSentry event
                        last_event = process_txsentry_event(match, events_txsentry, seen_events, last_event)
                        break  # Move to next line if any TxSentry pattern matches
            except UnicodeDecodeError as e:
                # Log the error and continue. It's redundant with 'errors=ignore', buut there was a case with very stubborn utf-8 Unicode errors
                print(f"Error decoding line in file {file_path}: {e}")
                continue

def process_oom_event(match, events_oom, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    service_name = match.group('service')
    event_key = (datetime_obj, service_name, 'OOM')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_oom.append({
            'DateTime': datetime_obj,
            'service': service_name,
        })

def process_txsentry_event(match, events_txsentry, seen_events, last_event):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    service_name = match.group('service') if 'service' in match.groupdict() else ""
    query = match.group('query') if 'query' in match.groupdict() else ""
    thread_key = match.group('thread_key') if 'thread_key' in match.groupdict() else ""
    event_key = (datetime_obj, service_name, query, thread_key, 'TxSentry')

    # Check if this event should be merged with the previous one
    if last_event and last_event['thread_key'] == thread_key and not service_name:
        # Merge this event's query with the last event's service
        last_event['query'] = query
        return None  # No new event to return since it was merged

    new_event = {
        'DateTime': datetime_obj,
        'service': service_name,
        'query': query,
        'thread_key': thread_key,
    }
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_txsentry.append(new_event)
    
    return new_event if service_name else None  # Return the new event only if it has a service

# Define headers for both event types
headers_oom = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_txsentry = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]

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
            del event['DateTime']  # Clean up DateTime for final output
            if 'thread_key' in event:  # Remove thread_key from final output
                del event['thread_key']
    
    # Prepare final output
    final_output = {
        "OOM": {"data": events_oom, "headers": headers_oom},
        "TxSentry": {"data": events_txsentry, "headers": headers_txsentry}
    }
    
    print(json.dumps(final_output))

if __name__ == '__main__':
    main()

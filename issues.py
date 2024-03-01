#! /usr/bin/python3

import re
import json
import gzip
import datetime
import glob

# OOM patterns
oom_pattern_1 = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+\S+\s+OutOfMemoryMonitor\[\d+\]: '
    r'Discovered out-of-memory error for (?P<service>[^(]+)\(type.*'
)
oom_pattern_2 = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+\S+\s+OutOfMemoryMonitor\[\d+\]: '
    r'Discovered out-of-memory error for (?P<service>\S+)\s+process\.'
)
oom_pattern_3 = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[(?P<service>Thread-\d+)\]\s+java.lang.OutOfMemoryError'
)

# TxSentry pattern
txsentry_pattern = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+.*TxSentry:.*Found a process on host .*: (?P<service>\w+), pid=.*'
)

def process_file(file_path, events_oom, events_txsentry, seen_events_oom, seen_events_txsentry):
    open_func = gzip.open if file_path.endswith('.gz') else open
    with open_func(file_path, 'rt') as file:
        for line in file:
            # Check for OOM patterns
            for pattern_index, pattern in enumerate([oom_pattern_1, oom_pattern_2, oom_pattern_3], start=1):
                match = pattern.search(line)
                if match:
                    date_str = match.group('date')
                    datetime_obj = datetime.datetime.strptime(date_str, '%b %d %H:%M:%S')
                    service_name = match.group('service')
                    if pattern_index == 3:
                        service_name = "Check logs / " + service_name
                    event_key = (datetime_obj, service_name)
                    if event_key not in seen_events_oom:
                        seen_events_oom.add(event_key)
                        events_oom.append({'DateTime': datetime_obj, 'service': service_name})
                    break
            # Check for TxSentry pattern
            match_txsentry = txsentry_pattern.search(line)
            if match_txsentry:
                date_str = match_txsentry.group('date')
                datetime_obj = datetime.datetime.strptime(date_str, '%b %d %H:%M:%S')
                service_name = match_txsentry.group('service')
                event_key = (datetime_obj, service_name)
                if event_key not in seen_events_txsentry:
                    seen_events_txsentry.add(event_key)
                    events_txsentry.append({'DateTime': datetime_obj, 'service': service_name})

# OOM headers
headers_oom = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]

# TxSentry headers
headers_txsentry = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]

def main():
    events_oom = []
    events_txsentry = []
    seen_events_oom = set()
    seen_events_txsentry = set()
    # Process files for both OOM and TxSentry errors
    process_file('var/log/qradar.error', events_oom, events_txsentry, seen_events_oom, seen_events_txsentry)
    for zipped_file in glob.glob('var/log/qradar.old/qradar.error*.gz'):
        process_file(zipped_file, events_oom, events_txsentry, seen_events_oom, seen_events_txsentry)
    # Sort and format events
    for events, headers in [(events_oom, headers_oom), (events_txsentry, headers_txsentry)]:
        events.sort(key=lambda event: event['DateTime'])
        for idx, event in enumerate(events):
            event['time_date'] = event['DateTime'].strftime("%m/%d %H:%M:%S")
            event['id'] = str(idx)
            del event['DateTime']
    # Print final output
    print(json.dumps({
        "OOM": {"data": events_oom, "headers": headers_oom},
        "TxSentry": {"data": events_txsentry, "headers": headers_txsentry}
    }))

if __name__ == '__main__':
    main()

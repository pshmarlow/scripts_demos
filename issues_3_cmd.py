import re
import json
import os
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
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[hostcontext.hostcontext\] \[(?P<thread_key>[^/]+)\/Sequential.*TxSentry.*Found unmanaged process on host .*: (?P<service>\S+), pid=(?P<pid>\d+).*'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[hostcontext.hostcontext\] \[(?P<thread_key>[^/]+)\/Sequential.*TxSentry:.*TX on host .*: pid=(?P<pid>\d+).* query=\'(?P<query>.+)\''
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[hostcontext.hostcontext\] \[(?P<thread_key>[^/]+)\/Sequential.*TxSentry:.*Found a process on host .*: (?P<service>\S+), pid=(?P<pid>\d+).*'
    )
]

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

oom_keywords = "OutOfMemoryMonitor"
txsentry_keywords = "TxSentry"
log_path = "var/log/qradar.old/qradar.error.{25..1}.gz var/log/qradar.error"
cmd = f'zgrep -hE "{oom_keywords}|{txsentry_keywords}" {log_path} 2>/dev/null'

def process_logs(events_oom, events_txsentry, seen_events):
    issues_runs = os.popen(cmd).read().strip().split('\n')
    last_event = None
    for run in issues_runs:
      
        # OOM pattern check and processing
        for pattern in oom_patterns:
            match = pattern.search(run)
            if match:
                process_oom_event(match, events_oom, seen_events)
                last_event = None
                break
        
        # TxSentry pattern check and processing
        for pattern in txsentry_patterns:
            match = pattern.search(run)
            if match:
                last_event = process_txsentry_event(match, events_txsentry, seen_events, last_event)
                break
              
# Define headers for the event types
headers_oom = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_txsentry = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]

def main():
    events_oom = []
    events_txsentry = []
    seen_events = set()
    process_logs(events_oom, events_txsentry, seen_events)
    
    for event_list, headers in [(events_oom, headers_oom), (events_txsentry, headers_txsentry)]:
        event_list.sort(key=lambda event: event['DateTime'])
        for idx, event in enumerate(event_list):
            event['time_date'] = event['DateTime'].strftime("%m/%d %H:%M:%S")
            event['id'] = str(idx)
            del event['DateTime'] 
            if 'thread_key' in event: 
                del event['thread_key']
    
    final_output = {
        "OOM": {"data": events_oom, "headers": headers_oom},
        "TxSentry": {"data": events_txsentry, "headers": headers_txsentry}
    }
    
    print(json.dumps(final_output))

if __name__ == '__main__':
    main()

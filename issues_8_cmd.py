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

# ReferenceDataProcessorThread pattern
reference_data_processor_thread_pattern = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*ReferenceDataProcessorThread - We have crossed the update threshold.*'
)

# Expensive Custom Rules pattern
expensive_rules_pattern = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*Expensive Custom Rules Based On Average Throughput: (?P<rules>.*)'
)

# Too Many Open Files pattern
too_many_open_patterns = [
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[S+\.(?P<service>\S+)\]...Too many open ...'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\s(?P<service>\S+)\[\d+\]: ...Too many open...'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+.*?\s+(?P<service>\w+)[\[\(].*Too many open files'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+.*?\[(?P<service>[^\]]+)\].*Too many open files'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+.*?\[(?P<service>\w+)\.\w+\].*Too many open files'
    )
]

# Cache Overflow pattern

cache_overflow_patterns = [
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[(?P<service>[^\.\]]+)[^\] \[.*com.q1labs.frameworks.cache.ChainAppendCache: \[WARN\] \[NOT.*- -\] \[-/- -\](?P<cache>\S+) (?P<message>.*)'
    )
]

# Dropped Receive Packets pattern
dropped_receive_pattern = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+.*Dropped receive packets on interface (?P<interface>\S+) has an average of (?P<over_5_intervals>\d+(\.\d+)?) over the past.*intervals, and has exceeded the configured threshold of (?P<threshold>\d+(\.\d+)?).*'
)

# Connect Localhost pattern
connect_localhost_pattern = re.compile(
    r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+.*\]Unable to connect to server localhost:(?P<port>\d+)'
)

#HERE

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

def process_reference_data_processor_event(match, events_reference_data_processor, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    event_key = (datetime_obj, 'ReferenceDataProcessorThread')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_reference_data_processor.append({
            'DateTime': datetime_obj
            'message': 'We have crossed...'
        })

def process_expensive_rules_event(match, events_expensive_rules, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    rules_details = match.group('rules')
    event_key = (datetime_obj, 'ExpensiveRules', rules_details)
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_expensive_rules.append({
            'DateTime': datetime_obj,
            'rules': rules_details,
        })

def process_too_many_open_event(match, events_too_many_open, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    service_name = match.group('service')
    event_key = (datetime_obj, service_name, 'TooManyOpenFiles')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_too_many_open.append({
            'DateTime': datetime_obj,
            'service': service_name,
        })

def process_cache_overflow_event(match, events_cache_overflow, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    service_name = match.group('service')
    cache_name = match.group('cache')
    additional_message = match.group('message').strip()  # Capture the additional message part
    message = cache_name + ' ' + additional_message
    event_key = (datetime_obj, service_name, cache_name, message, 'CacheOverflow')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_cache_overflow.append({
            'DateTime': datetime_obj,
            'service': service_name,
            'cache': cache_name, 
            'message': message 
        })

def process_dropped_receive_event(match, events_dropped_receive, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    interface = match.group('interface')
    over_5_intervals = match.group('over_5_intervals')
    threshold = match.group('threshold')
    event_key = (datetime_obj, interface, 'DroppedReceive')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_dropped_receive.append({
            'DateTime': datetime_obj,
            'interface': interface,
            'over_5_intervals': over_5_intervals,
            'threshold': threshold,
        })


def process_connect_localhost_event(match, events_connect_localhost, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    port = match.group('port')
    message_1 = "Unable to connect to server localhost:"
    event_key = (datetime_obj, port, 'ConnectLocalhost')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_connect_localhost.append({
            'DateTime': datetime_obj,
            'message': message_1,
            'port': port
        })
        
#HERE

#
end_date_cmd = "tail -200 /var/log/qradar.error | grep -oP '^\w{3}\s+\d+\s+(\d+:){2}\d+' | tail -1"
start_date_cmd = "zcat /var/log/qradar.old/$(ls -tr1 /var/log/qradar.old | grep -P 'qradar.error.\d+.gz$' | head -1) | head -200 | grep -oP '^\w{3}\s+\d+\s+(\d+:){2}\d+' | head -1"
#
def fetch_date(cmd):
    try:
        output = os.popen(cmd).read().strip()
        # If no output is found from zcat, use tail from qradar.error
        if not output:
            cmd = "head -200 /var/log/qradar.error | grep -oP '^\w{3}\s+\d+\s+(\d+:){2}\d+' | head -1"
            output = os.popen(cmd).read().strip()
        return datetime.datetime.strptime(output, '%b %d %H:%M:%S').replace(year=datetime.datetime.now().year).isoformat()
    except Exception as e:
        print(f"Error fetching date: {e}")
        return None



# Keywords and command for zgrep
oom_keywords = "OutOfMemoryMonitor"
txsentry_keywords = "TxSentry"
reference_data_processor_keywords = "ReferenceDataProcessorThread"
expensive_rules_keywords = "Expensive Custom Rules Based On Average Throughput"
too_many_open_files_keywords = "Too many open "
cache_overflow_keywords = " is experiencing heavy "
dropped_receive_keywords = "Dropped receive packets on interface "
connect_localhost_keywords = "Unable to connect to server localhost:"
log_path = "var/log/qradar.old/qradar.error.{25..1}.gz var/log/qradar.error"
# HERE
# BELOW
cmd = f'zgrep -hE "{oom_keywords}|{txsentry_keywords}|{reference_data_processor_keywords}|{expensive_rules_keywords}|{too_many_open_files_keywords}|{cache_overflow_keywords}|{dropped_receive_keywords}|{connect_localhost_keywords}" {log_path} 2>/dev/null'

# BELOW
def process_logs(events_oom, events_txsentry, events_reference_data_processor, events_expensive_rules, events_too_many_open, events_cache_overflow, events_dropped_receive, events_connect_localhost, seen_events):
 
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
         
        # Reference Data Processor Thread pattern check and processing
        match = reference_data_processor_thread_pattern.search(run)
        if match:
            process_reference_data_processor_event(match, events_reference_data_processor, seen_events)
            last_event = None
          
        # ExpensiveRules pattern check and processing
        match = expensive_rules_pattern.search(run)
        if match:
            process_expensive_rules_event(match, events_expensive_rules, seen_events)
            last_event = None 

        # Too Many Open Files pattern check and processing
        for pattern in too_many_open_patterns:
            match = pattern.search(run)
            if match:
                process_too_many_open_event(match, events_too_many_open, seen_events)
                last_event = None
                break

        # Cache Overflow pattern check and processing
        for pattern in cache_overflow_patterns:
            match = pattern.search(run)
            if match:
                process_cache_overflow_event(match, events_cache_overflow, seen_events)
                last_event = None
                break

        # Dropped Receive pattern check and processing
        match = dropped_receive_pattern.search(run)
        if match:
            process_dropped_receive_event(match, events_dropped_receive, seen_events)
            last_event = None

        # Connect Localhost pattern check and processing
        match = connect_localhost_pattern.search(run)
        if match:
            process_connect_localhost_event(match, events_connect_localhost, seen_events)
            last_event = None

        #HERE

# Define headers for the event types
headers_oom = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_txsentry = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_reference_data_processor = [{"key": "time_date", "header": "Time/Date"}, {"key": "message", "header": "Message"}]
headers_expensive_rules = [{"key": "time_date", "header": "Time/Date"}, {"key": "rules", "header": "Rules Details"}]
headers_too_many_open = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_cache_overflow = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}, {"key": "cache", "header": "Cache Name"}]
headers_dropped_receive = [{"key": "time_date", "header": "Time/Date"}, {"key": "interface", "header": "Interface"}, {"key": "over_5_intervals", "header": "Over 5 Intervals"}, {"key": "threshold", "header": "Threshold"}]
headers_connect_localhost = [{"key": "time_date", "header": "Time/Date"}, {"key": "message", "header": "Message"}, {"key": "port", "header": "Port"}]
# HERE

def main():
    events_oom = []
    events_txsentry = []
    events_reference_data_processor = []
    events_expensive_rules = []
    events_too_many_open = []
    events_cache_overflow = []
    events_dropped_receive = []
    events_connect_localhost = []
    # HERE
    seen_events = set()
    # BELOW
    process_logs(events_oom, events_txsentry, events_reference_data_processor, events_expensive_rules, events_too_many_open, events_cache_overflow, events_dropped_receive, events_connect_localhost, seen_events)

    # BELOW
    for event_list, headers in [(events_oom, headers_oom), (events_txsentry, headers_txsentry), (events_reference_data_processor, headers_reference_data_processor), (events_expensive_rules, headers_expensive_rules), (events_too_many_open, headers_too_many_open), (events_cache_overflow, headers_cache_overflow), (events_dropped_receive, headers_dropped_receive), (events_connect_localhost, headers_connect_localhost)]:
         event_list.sort(key=lambda event: event['DateTime'])
         for idx, event in enumerate(event_list):
             event['time_date'] = event['DateTime'].strftime("%m/%d %H:%M:%S")
             event['id'] = str(idx)
             del event['DateTime'] 
             if 'thread_key' in event: 
                 del event['thread_key']

    #
    start_date = fetch_date(start_date_cmd)
    end_date = fetch_date(last_date_cmd)
  
    final_output = {
        "metadata": {"start": start_date, "end": end_date},
        "OOM": {"data": events_oom, "headers": headers_oom},
        "TxSentry": {"data": events_txsentry, "headers": headers_txsentry},
        "ReferenceDataProcessorThread": {"data": events_reference_data_processor, "headers": headers_reference_data_processor},
        "ExpensiveRules": {"data": events_expensive_rules, "headers": headers_expensive_rules},
        "TooManyOpenFiles": {"data": events_too_many_open, "headers": headers_too_many_open},
        "CacheOverflow": {"data": events_cache_overflow, "headers": headers_cache_overflow},
        "DroppedReceive": {"data": events_dropped_receive, "headers": headers_dropped_receive},
        "ConnectLocalhost": {"data": events_connect_localhost, "headers": headers_connect_localhost}
        # HERE
    }
    
    print(json.dumps(final_output))

if __name__ == '__main__':
    main()

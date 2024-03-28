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
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[(?P<service>\S+)\].*com.q1labs.frameworks.cache.ChainAppendCache: \[WARN\].*\[(?P<cache>\S+)\] is experiencing heavy COLLISIONS exceeding configured threshold.*'
    ),
    re.compile(
        r'(?P<date>\w{3}\s+\d+\s+\d+:\d+:\d+).*\[(?P<service>\S+)\].*com.q1labs.frameworks.cache.ChainAppendCache: \[WARN\].*\[(?P<cache>\S+)\] is experiencing heavy disk reads exceeding configured threshold.*'
    )
]

# Keywords and command for zgrep
oom_keywords = "OutOfMemoryMonitor"
txsentry_keywords = "TxSentry"
reference_data_processor_keywords = "ReferenceDataProcessorThread"
expensive_rules_keywords = "Expensive Custom Rules Based On Average Throughput"
too_many_open_files_keywords = "Too many open "
cache_overflow_keywords = " is experiencing heavy "
log_path = "var/log/qradar.old/qradar.error.{25..1}.gz var/log/qradar.error"
cmd = f'zgrep -hE "{oom_keywords}|{txsentry_keywords}|{reference_data_processor_keywords}|{expensive_rules_keywords}|{too_many_open_files_keywords}|{cache_overflow_keywords}" {log_path} 2>/dev/null'

def process_cache_overflow_event(match, events_cache_overflow, seen_events):
    date_str = match.group('date')
    current_year = datetime.datetime.now().year
    date_str_with_year = f"{date_str} {current_year}"
    datetime_obj = datetime.datetime.strptime(date_str_with_year, '%b %d %H:%M:%S %Y')
    service_name = match.group('service')
    cache_name = match.group('cache')
    event_key = (datetime_obj, service_name, cache_name, 'CacheOverflow')
    if event_key not in seen_events:
        seen_events.add(event_key)
        events_cache_overflow.append({
            'DateTime': datetime_obj,
            'service': service_name,
            'cache': cache_name,
        })

def process_logs(events_oom, events_txsentry, events_reference_data_processor, events_expensive_rules, events_too_many_open, events_cache_overflow, seen_events):
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

# Define headers for the event types
headers_oom = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_txsentry = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_reference_data_processor = [{"key": "time_date", "header": "Time/Date"}]
headers_expensive_rules = [{"key": "time_date", "header": "Time/Date"}, {"key": "rules", "header": "Rules Details"}]
headers_too_many_open = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}]
headers_cache_overflow = [{"key": "time_date", "header": "Time/Date"}, {"key": "service", "header": "Service Name"}, {"key": "cache", "header": "Cache Name"}]

def main():
    events_oom = []
    events_txsentry = []
    events_reference_data_processor = []
    events_expensive_rules = []
    events_too_many_open = []
    events_cache_overflow = []
    seen_events = set()
    process_logs(events_oom, events_txsentry, events_reference_data_processor, events_expensive_rules, events_too_many_open, events_cache_overflow, seen_events)
    
    for event_list, headers in [(events_oom, headers_oom), (events_txsentry, headers_txsentry), (events_reference_data_processor, headers_reference_data_processor), (events_expensive_rules, headers_expensive_rules), (events_too_many_open, headers_too_many_open), (events_cache_overflow, headers_cache_overflow)]:
        event_list.sort(key=lambda event: event['DateTime'])
        for idx, event in enumerate(event_list):
            event['time_date'] = event['DateTime'].strftime("%m/%d %H:%M:%S")
            event['id'] = str(idx)
            del event['DateTime'] 
            if 'thread_key' in event: 
                del event['thread_key']
  
    final_output = {
        "OOM": {"data": events_oom, "headers": headers_oom},
        "TxSentry": {"data": events_txsentry, "headers": headers_txsentry},
        "ReferenceDataProcessorThread": {"data": events_reference_data_processor, "headers": headers_reference_data_processor},
        "ExpensiveRules": {"data": events_expensive_rules, "headers": headers_expensive_rules},
        "TooManyOpenFiles": {"data": events_too_many_open, "headers": headers_too_many_open},
        "CacheOverflow": {"data": events_cache_overflow, "headers": headers_cache_overflow}
    }
    
    print(json.dumps(final_output))

if __name__ == '__main__':
    main()

import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import re
import csv
from datetime import datetime, date, timedelta, timezone
import pytz
import json

def get_log_line(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        line_num = 0
        for line in file:
            line_num += 1
            yield line_num, line

def handle_single_line_event(tokens):

    # Find the start of the event data payload, indicated by the index of '{'
    start_index = next((i for i, token in enumerate(tokens) if token == '{'), -1)
    # Find the end of the event data payload, indicated by the index of '}'
    end_index = next((i for i, token in enumerate(tokens) if token == '}'), -1)

    # we don't need the braces
    start_index += 1
    end_index -= 1
    # Extract the relevant tokens between '{' to '}' and remove unnecessary whitespace
    event_tokens = [token.strip() for token in tokens[start_index:end_index + 1]]
    #print(event_tokens)

    # Put tokens back into a string
    event_string = ''.join(event_tokens)
    # Split string on ',' for a list of event data pairs
    event_list = event_string.split(',')
    # finally, create a dictionary of event data items
    event_dict = dict()
    for event in event_list:
        parts = event.split(':')
        event_dict[parts[0]] = parts[1].strip("'")

    #print(event_string)
    #print(event_list)
    #print(event_dict)

    return event_dict

def handle_multi_line_event(tokens):
    # Find the start of the event data payload, indicated by the index of '{'
    start_index = next((i for i, token in enumerate(tokens) if token == '{'), -1)

    # we don't need the braces
    start_index += 1
    # Extract the relevant tokens between '{' to '}' and remove unnecessary whitespace
    event_tokens = [token.strip() for token in tokens[start_index:]]
    #print(event_tokens)

    # Put tokens back into a string
    event_string = ''.join(event_tokens)
    # Split string on ',' for a list of event data pairs
    event_list = event_string.split(',')
    # finally, create a dictionary of event data items
    event_dict = dict()
    for event in event_list:
        parts = event.split(':')
        event_dict[parts[0]] = parts[1].strip("'")

    #print(event_string)
    #print(event_list)
    #print(event_dict)

    return event_dict

def get_record_type(tokens):
    # Check if 'Parsed' and 'Data:' are present in the tokens
    if 'Parsed' not in tokens or 'Data:' not in tokens:
        return 'NOT-EVENT'

    # Find the start of the event data payload, indicated by the index of '{'
    start_index = next((i for i, token in enumerate(tokens) if token == '{'), -1)
    # Find the end of the event data payload, indicated by the index of '}'
    end_index = next((i for i, token in enumerate(tokens) if token == '}'), -1)

    # Keywords 'Parsed' and 'Data:' are present, but there is no payload
    if start_index < 0 and end_index < 0:
        return 'INVALID-EVENT'

    # Single line event record, full payload present
    if start_index >= 0 and end_index >= 0:
        return 'SINGLE-LINE-EVENT'

    # Multi-line event record start
    if start_index >= 0 and end_index < 0:
        return 'MULTI-LINE-EVENT-BEGIN'

    # Multi-line event record end
    if start_index < 0 and end_index >= 0:
        return 'MULTI-LINE-EVENT-END'

    return 'ERROR'

def extract_code_and_name(identifier):
    """
    Extracts the code and name from a node identifier string.

    The node identifier has two parts: a code (e.g., "AAA_MGT_01") and a name (free text).
    The code always takes the form of "AAA_MGT_DD".

    Args:
        identifier (str): The identifier string to parse.

    Returns:
        tuple: A tuple containing the code and name as strings.
    """

    pattern = r"^(\w{3}_MGT_\d{2})\s+(.*)$"
    match = re.match(pattern, identifier)

    if match:
        code = match.group(1)
        name = match.group(2)
        return code.strip(), name.strip()
    else:
        return '?', '?'

def get_dst_dates(year_str):
    """
    Calculates the start and end of DST for a given year.

    Args:
        year_str (str): The year as a string (e.g., "2024").

    Returns:
        tuple: A tuple containing two datetime.date objects: (dst_start, dst_end).
               Returns (None, None) if the input year is invalid.
    """
    try:
        year = int(year_str)
    except ValueError:
        print("Error: Invalid year format. Please provide a string representing a valid year.")
        return None, None

    # Calculate the last Sunday in March
    # work backwards from April 1st until you reach a Sunday
    april_1 = date(year, 4, 1)
    start = april_1
    dow = start.weekday()
    while dow != 6:
        start = start - timedelta(days=1)
        dow = start.weekday()

    # Calculate the last Sunday in October
    # work backwards from November 1st until you reach a Sunday
    november_1 = date(year, 11, 1)
    end = november_1
    dow = end.weekday()
    while dow != 6:
        end = end - timedelta(days=1)
        dow = end.weekday()

    return start, end

"""
def parse_line(line):
    # Split the line into tokens based on whitespace
    tokens = [token.strip() for token in line.split()]
    #print(tokens)

    # Invalid record if not enough tokens to extract receipt datetime or first token is not a month
    if (len(tokens) == 0 or
            tokens[0] not in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
        raise ValueError("Invalid log record. Cannot extract datetime.")

    # Extract time of receipt parts
    month_str, day_str, time_str = tokens[0:3]
    # Convert time of receipt parts to partial datetime
    receipt_dt_str = f"{month_str}-{day_str} {time_str}"

    # We always get a time of receipt, but does this record contain a SiteBoss event?
    remainder = tokens[3:]
    event = extract_event(remainder)
    #print(event)

    if event is None:
        return None

    return {
        "receipt_dt_str": receipt_dt_str,
        "event_ts_unix": event['ts'],
        "mt": event['mt'],
        "id_val": event['id'],
        "st": event['st']
    }
"""

def valid_record(line):
    # Split the line into tokens based on whitespace
    tokens = [token.strip() for token in line.split()]
    #print(tokens)

    # Invalid record if not enough tokens to extract receipt datetime or first token is not a month
    if (len(tokens) == 0 or
            tokens[0] not in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']):
        return None, None

    # Record is valid. Extract time of receipt parts
    month_str, day_str, time_str = tokens[0:3]
    # Convert time of receipt parts to partial datetime
    receipt_dt_str = f"{month_str}-{day_str} {time_str}"
    # remaining tokens
    remainder = tokens[3:]

    return receipt_dt_str, remainder

def parse_log_file(node_info, log_file_path, output_file_path):
    """
    Parses a log file and extracts relevant data, writing it to a CSV file.

    Args:
        log_file_path (str): The path to the log file.
    """

    invalid_record_count = 0
    non_event_record_count = 0
    invalid_event_count = 0
    single_line_event_count = 0
    multi_line_event_begin_count = 0
    multi_line_event_end_count = 0
    other_record_error_count = 0
    multi_line_event = False

    with (open(output_file_path, 'w', newline='') as csvfile):
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            ['receipt_dt', 'receipt_dt_utc', 'event_ts', 'event_dt', 'event_dt_utc', 'event_receipt_s', 'time_ok', 'id',
             'ne_code', 'ne_name', 'mt', 'st'])

        for line_num, line in get_log_line(log_file_path):
            #print(f"{line_num} -> {line}")
            receipt_dt_str, remainder = valid_record(line)
            if receipt_dt_str is None:
                print(f"Error - Line: {line_num} - Invalid log record. Cannot extract datetime.")
                invalid_record_count += 1
                continue

            # If we get this far, we have a valid record - does it contain an event?
            event_type = get_record_type(remainder)
            match event_type:
                case "NOT-EVENT":
                    non_event_record_count += 1
                    continue
                case "INVALID-EVENT":
                    invalid_event_count += 1
                    continue
                case "SINGLE-LINE-EVENT":
                    single_line_event_count += 1
                    event_log = handle_single_line_event(remainder)
                    # rename event_log > event_record
                    # call process_event(event_record)
                    # multi_line_event = False
                    # continue
                case "MULTI-LINE-EVENT-BEGIN":
                    multi_line_event_begin_count += 1
                    multi_line_event = True
                    # or event_record_complete = False
                    # Have detected the start of a multi-line event
                    # each subsequent line will add to the event data record
                    # until the end of the event record is detected with a closing brace '}'
                    #event_log = handle_multi_line_event(remainder)
                    continue
                case "MULTI-LINE-EVENT-END":
                    multi_line_event_end_count += 1
                    #event_log = handle_multi_line_event(remainder)
                    continue
                case "ERROR":
                    other_record_error_count += 1
                    print("Error - unknown record type.")
                    continue

            # Receipt Datetime (Local Irish Time)
            # TODO - UTC handling needs fixing!
            receipt_dt = datetime.strptime(receipt_dt_str, "%b-%d %H:%M:%S")
            receipt_dt = receipt_dt.replace(year=2025)
            receipt_dt_str_utc = receipt_dt.replace(tzinfo=pytz.timezone('Europe/Dublin')).strftime(
                "%Y-%m-%d %H:%M:%S")
            receipt_dt_str = receipt_dt.strftime("%Y-%m-%d %H:%M:%S")

            # Event Timestamp (Unix Timestamp)
            event_ts = int(event_log['ts'])

            # Convert Unix timestamp to datetime (Local Irish Time)
            event_dt = datetime.fromtimestamp(event_ts, tz=pytz.timezone('Europe/Dublin'))
            event_dt_str = event_dt.strftime("%Y-%m-%d %H:%M:%S")
            event_dt_str_utc = event_dt.strftime("%Y-%m-%d %H:%M:%S")

            # Calculate time difference
            receipt_dt_utc = datetime.strptime(receipt_dt_str, "%Y-%m-%d %H:%M:%S")
            receipt_dt_utc = pytz.timezone('Europe/Dublin').localize(receipt_dt_utc)
            event_dt_utc = datetime.strptime(event_dt_str, "%Y-%m-%d %H:%M:%S")
            event_dt_utc = pytz.timezone('Europe/Dublin').localize(event_dt_utc)

            # receipt dt should be later than event dt - positive diff is wanted!
            time_diff = (receipt_dt_utc - event_dt_utc).total_seconds()
            time_ok = "True" if time_diff > 0 else "False"

            # Node Name
            ne_code = '?'
            ne_name = '?'
            ne_details = node_info.get(event_log['id'], '?')
            if ne_details != '?':
                ne_code, ne_name = extract_code_and_name(ne_details['name'])

            csv_writer.writerow(
                [receipt_dt_str, receipt_dt_str_utc, event_ts, event_dt_str, event_dt_str_utc, time_diff, time_ok,
                 event_log['id'], ne_code, ne_name, event_log['mt'], event_log['st']])

            print(f"\rLine: {line_num:10,}", end='')

        print("\n\n- END-")
        print(f"Total lines processed: {line_num:,}")
        print(f"Output written to: {output_file_path}")
        print("*"*10)
        print(f"Invalid Records          : {invalid_record_count:,}")
        print(f"Non-Event Records        : {non_event_record_count:,}")
        print(f"Invalid Events           : {invalid_event_count:,}")
        print(f"Single Line Events       : {single_line_event_count:,}")
        print(f"Multi-Line Event Begins  : {multi_line_event_begin_count:,}")
        print(f"Multi-Line Event Ends    : {multi_line_event_end_count:,}")
        print(f"Other Events Errors      : {other_record_error_count:,}")

def choose_file(msg, ftypes):
    """
    Opens a file dialog to select a file.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    file_path = filedialog.askopenfilename(
        title=msg,
        filetypes=ftypes
    )
    if file_path:
        return Path(file_path)
    else:
        print("No file selected.")
        return None

def load_node_info(node_info_path):
    """
    Loads a JSON file and transforms it into a dictionary of dictionaries.

    The 'id' field from each JSON entity becomes the key in the outer dictionary.
    The value associated with each key is a dictionary containing the
    remaining fields of the original JSON entity.

    Args:
        node_info_path (Path): The path to the JSON file.

    Returns:
        dict: A dictionary of dictionaries, or None if the file
              cannot be opened or the JSON is invalid.
    """
    try:
        with open(node_info_path, 'r') as f:
            data = json.load(f)  # Load the JSON data from the file
    except FileNotFoundError:
        print(f"Error: File not found at {node_info_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {node_info_path}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")  #Catch other potential errors
        return None

    result = {}
    for entity in data:
        entity_id = entity.pop('id')  # Extract 'id' and remove it from the entity
        result[entity_id] = entity    # Use the 'id' as the key, rest as value

    return result


if __name__ == "__main__":

    dst_begin, dst_end = get_dst_dates('2025')
    print(f"DST Begin: {dst_begin.strftime('%A, %B %d, %Y at %H:%M:%S')}")
    print(f"DST End  : {dst_end.strftime('%A, %B %d, %Y at %H:%M:%S')}")

    log_message = 'Choose SiteBoss log file'
    log_file_types = (("Text files", "*.txt"), ("All files", "*.*"))
    node_info_message = 'Choose Node Info JSON file'
    node_info_file_types = (("JSON files", "*.json"), ("All files", "*.*"))

    node_info_file = choose_file(node_info_message, node_info_file_types)
    log_file = choose_file(log_message, log_file_types)
    output_file = Path(log_file.parent, (log_file.stem + '_out.csv'))

    node_info = load_node_info(node_info_file)
    parse_log_file(node_info, log_file, output_file)






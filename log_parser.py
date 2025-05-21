import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import re
from datetime import datetime, date, timedelta, timezone
import pytz
import json
import duckdb
import time

EVENT_TABLE = "siteboss_events"
COLUMNS = ['receipt_dt', 'receipt_dt_utc', 'event_ts', 'event_dt', 'event_dt_utc', 'evt_to_rcpt_sec', 'time_ok',
           'ne_id', 'ne_code', 'ne_name', 'mt', 'st', 'si', 'va', 'sc', 'sv', 'ke', 'cn', 'na', 'rn', 'ss', 'it']

def create_event_table(conn, table_name, column_names):
    # Create a list of column definitions
    column_definitions = [f"{col} VARCHAR" for col in column_names]

    # Join the column definitions
    columns_str = ', '.join(column_definitions)

    # Construct the SQL CREATE OR REPLACE TABLE statement
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({columns_str})"

    # Execute the SQL statement
    conn.execute(create_table_sql)

def add_event_to_db(conn, table_name, event):
    # Extract column names and values from the event dictionary
    columns = ', '.join(event.keys())
    placeholders = ', '.join(['?'] * len(event))

    # Construct the SQL INSERT INTO statement
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Execute the SQL statement with the event values
    conn.execute(insert_query, tuple(event.values()))

    # Commit the changes
    conn.commit()

def get_log_line(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        line_num = 0
        for line in file:
            line_num += 1
            yield line_num, line

def get_single_line_event(tokens):

    # Find the start of the event data payload, indicated by the index of '{'
    start_index = next((i for i, token in enumerate(tokens) if token == '{'), -1)
    # Find the end of the event data payload, indicated by the index of '}'
    end_index = next((i for i, token in enumerate(tokens) if token == '}'), -1)

    # we don't need the braces
    start_index += 1
    end_index -= 1
    # Extract the relevant tokens between '{' to '}' and remove unnecessary whitespace
    event_tokens = [token.strip() for token in tokens[start_index:end_index + 1]]
    #print(f"In get_SLE: {event_tokens=}")

    # Put tokens back into a string
    event_string = ''.join(event_tokens)
    # Split string on ',' for a list of event data pairs
    event_list = event_string.split(',')
    # finally, create a dictionary of event data items
    event_dict = dict()
    for event in event_list:
        parts = event.split(':')
        event_dict[parts[0]] = parts[1].strip("'")

    #print(f"In get_SLE: {event_string=}")
    #print(f"In get_SLE: {event_list=}")
    #print(f"In get_SLE: {event_dict=}")

    return event_dict

def process_event_log(receipt_mdt_str, event_log):
    """

    :param receipt_mdt_str: month_date_time as a string
    :param event_log:
    :return:
    """

    # Receipt Datetime (Local Irish Time)
    # TODO - UTC handling needs fixing!
    receipt_dt = datetime.strptime(receipt_mdt_str, "%b-%d %H:%M:%S")
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

    processed_event = {
        "receipt_dt": receipt_dt_str,
        "receipt_dt_utc": receipt_dt_str_utc,
        "event_ts": event_log['ts'],
        "event_dt": event_dt_str,
        "event_dt_utc": event_dt_str_utc,
        "evt_to_rcpt_sec": str(time_diff),
        "time_ok": time_ok,
        "ne_id": event_log['id'],
        "ne_code": ne_code,
        "ne_name": ne_name,
        "mt": event_log['mt']
    }

    # add remaining event_log items to processed_event
    for key, value in event_log.items():
        if key not in ['ts', 'id', 'mt']:
            processed_event[key] = value

    return processed_event

def get_multiline_parts(tokens):
    """
    Return the event tokens in the remainder of the line that is part of a Mult-Line Event.

    :param tokens:
    :return:
    """

    # first re-combine any message tokens that were unintentionally split on white space
    #tokens = recombine_tokens(tokens)
    # print(f"Recombined tokens: {tokens}")

    # If the last token is "}" we have reached the end of the Multi-Line Event
    if tokens[-1] == "}":
        return True, tokens[2:]

    return False, tokens[2:]

def get_record_type(tokens):
    # Check if 'Parsed' and 'Data:' are present in the tokens
    if 'Parsed' not in tokens or 'Data:' not in tokens:
        return "NOT_EVENT"

    # Find the start of the event data payload, indicated by the index of '{'
    start_index = next((i for i, token in enumerate(tokens) if token == '{'), -1)
    # Find the end of the event data payload, indicated by the index of '}'
    end_index = next((i for i, token in enumerate(tokens) if token == '}'), -1)

    # Keywords 'Parsed' and 'Data:' are present, but there is no payload
    if start_index < 0 and end_index < 0:
        return "INVALID_EVENT"

    # Single line event record, full payload present
    if start_index >= 0 and end_index >= 0:
        return "SINGLE_LINE_EVENT"

    # Multi-line event record start
    if start_index >= 0 and end_index < 0:
        return "MULTI_LINE_EVENT_BEGIN"

    # Multi-line event record end
    #if start_index < 0 and end_index >= 0:
    #    return "MULTI_LINE_EVENT_END"

    return "ERROR"

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

def parse_log_file(node_info, log_file_path, db_conn):
    """
    Parses a log file and extracts relevant data, writing it to a CSV file.

    Args:
        log_file_path (str): The path to the log file.
    """

    invalid_record_count = 0
    non_event_record_count = 0
    invalid_event_count = 0
    single_line_event_count = 0
    multi_line_event_count = 0
    other_record_error_count = 0
    multi_line_event = False

    for line_num, line in get_log_line(log_file_path):
        #print(f"{line_num}")
        receipt_dt_str, remainder = valid_record(line)
        if receipt_dt_str is None:
            print(f"Error - Line: {line_num} - Invalid log record. Cannot extract datetime.")
            invalid_record_count += 1
            continue

        # This is a valid record - Is it part of a Multi-Line Event?
        if multi_line_event:
            #print(f"In MLE. Remainder at next line: {remainder=}")
            mle_end, mle_parts = get_multiline_parts(remainder)
            mle_remainder.extend(mle_parts)
            #print(f"In MLE. Remainder after adding next line: {mle_remainder=}")
            if mle_end:
                #print(f"{mle_remainder=}")
                raw_event = get_single_line_event(mle_remainder)
                #print(f"{raw_event=}")
                processed_event = process_event_log(mle_receipt_dt_str, raw_event)
                #print(f"{processed_event=}")
                add_event_to_db(conn, EVENT_TABLE, processed_event)
                multi_line_event = False
            continue


        # This record is not part of a Multi-Line Event. Does the record contain an event?
        event_type = get_record_type(remainder)
        match event_type:
            case "NOT_EVENT":
                non_event_record_count += 1
                continue
            case "INVALID_EVENT":
                invalid_event_count += 1
                continue
            case "SINGLE_LINE_EVENT":
                single_line_event_count += 1
                #print(f"SLE before call get_SLE: {remainder=}")
                raw_event = get_single_line_event(remainder)
                #print(raw_event)
                processed_event = process_event_log(receipt_dt_str, raw_event)
                #print(processed_event)
                add_event_to_db(conn, EVENT_TABLE, processed_event)
                # multi_line_event = False
                continue
            case "MULTI_LINE_EVENT_BEGIN":
                multi_line_event_count += 1
                #print("\n*** Start ML")
                mle_receipt_dt_str = receipt_dt_str
                mle_remainder = remainder
                #print(f"In MLE. At start: {mle_remainder=}")
                multi_line_event = True

                # Have detected the start of a multi-line event
                # each subsequent line will add to the event data record
                # until the end of the event record is signalled by a closing brace '}'

                continue
            case "ERROR":
                other_record_error_count += 1
                print("Error - unknown record type.")
                continue

        #print(f"\rLine: {line_num:10,}", end='')

    print("\n\n- END-")
    print(f"Total lines processed: {line_num:,}")
    #print(f"Output written to: {output_file_path}")
    print("*"*10)
    print(f"Invalid Records          : {invalid_record_count:,}")
    print(f"Non-Event Records        : {non_event_record_count:,}")
    print(f"Invalid Events           : {invalid_event_count:,}")
    print(f"Single Line Events       : {single_line_event_count:,}")
    print(f"Multi-Line Events        : {multi_line_event_count:,}")
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

    start_time = time.time()

    dst_begin, dst_end = get_dst_dates('2025')
    print(f"DST Begin: {dst_begin.strftime('%A, %B %d, %Y at %H:%M:%S')}")
    print(f"DST End  : {dst_end.strftime('%A, %B %d, %Y at %H:%M:%S')}")

    log_message = 'Choose SiteBoss log file'
    log_file_types = (("Text files", "*.txt"), ("All files", "*.*"))
    node_info_message = 'Choose Node Info JSON file'
    node_info_file_types = (("JSON files", "*.json"), ("All files", "*.*"))

    node_info_file = choose_file(node_info_message, node_info_file_types)
    log_file = choose_file(log_message, log_file_types)

    # Connect to DuckDB
    dbpath = Path(log_file.parent, (log_file.stem + '_out.db'))
    conn = duckdb.connect(database=dbpath, read_only=False)
    # Create Events table
    create_event_table(conn, EVENT_TABLE, COLUMNS)

    node_info = load_node_info(node_info_file)
    parse_log_file(node_info, log_file, conn)

    conn.close()

    end_time = time.time()
    # Calculate the duration in seconds
    duration_seconds = end_time - start_time
    hours = int(duration_seconds // (60 * 60))
    minutes = int((duration_seconds % (60 * 60)) // 60)
    seconds = int(duration_seconds % 60)

    print(f"Execution time: {hours} hours, {minutes} minutes, {seconds} seconds")






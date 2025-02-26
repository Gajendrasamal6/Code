import json
import cx_Oracle
import datetime
import re
import csv

# Database configurations
DB_CONFIG = {
    "host": "192.0.10.11",
    "port": "1521",
    "username": "source_user",
    "password": "source_pass",
    "service": "orcl"
}

TARGET_DB_CONFIG = {
    "host": "182.90.22.31",
    "port": "1521",
    "username": "target_user",
    "password": "target_pass",
    "service": "orcl"
}

# File paths
json_file_path = "path/to/metadata.json"
query_template_file = "path/to/input_queries.txt"
output_csv_file_path = "path/to/output.csv"


def execute_query(db_config, query):
    """Executes a query and returns a single result."""
    try:
        dsn = cx_Oracle.makedsn(db_config["host"], db_config["port"], service_name=db_config["service"])
        with cx_Oracle.connect(db_config["username"], db_config["password"], dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        print(f"Error executing query: {query} -> {e}")
        return None


def detect_date_format(date_str):
    """Detects the format of a given date string."""
    date_patterns = [
        ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),  # YYYY-MM-DD
        ("%Y-%d-%m", r"\d{4}-\d{2}-\d{2}"),  # YYYY-DD-MM
        ("%d-%m-%Y", r"\d{2}-\d{2}-\d{4}"),  # DD-MM-YYYY
        ("%m-%d-%Y", r"\d{2}-\d{2}-\d{4}"),  # MM-DD-YYYY
        ("%d%m%Y", r"\d{2}\d{2}\d{4}"),      # DDMMYYYY
        ("%m%d%Y", r"\d{2}\d{2}\d{4}"),      # MMDDYYYY
        ("%Y%m%d", r"\d{4}\d{2}\d{2}"),      # YYYYMMDD
        ("%Y%d%m", r"\d{4}\d{2}\d{2}")       # YYYYDDMM
    ]

    for fmt, pattern in date_patterns:
        if re.fullmatch(pattern, date_str):
            return fmt
    return None  


def extract_date_formats_from_query(query):
    """
    Extracts the expected date formats from the query itself.
    Looks for patterns like to_date('{date1}', 'YYYY-MM-DD').
    """
    format_patterns = re.findall(r"to_date\('\{date\d+\}',\s*'([^']+)'\)", query)
    return format_patterns


def format_date(date_obj, original_format):
    """Formats a date object back to its original format."""
    return date_obj.strftime(original_format)



with open(json_file_path, "r") as file:
    metadata = json.load(file)


query_templates = {}
with open(query_template_file, "r") as file:
    for line in file:
        match = re.match(r"(\d+):(.+)", line.strip())
        if match:
            query_id, query_text = match.groups()
            query_templates[query_id] = query_text.strip()


with open(output_csv_file_path, "w", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([
        "oracle creds source", "oracle creds targets", "source url", "target url",
        "source_table", "target_table", "count source", "count target",
        "status", "start time", "last run"
    ])

    for index, entry in enumerate(metadata):
        query_id = entry["id"]
        if query_id not in query_templates:
            print(f"Skipping ID {query_id}, no matching query found.")
            continue

        query_template = query_templates[query_id]
        source_table = entry["source_table"]
        target_table = entry["target_table"]


        expected_formats = extract_date_formats_from_query(query_template)


        conditions = entry.get("source_col_list_for_where_condition", "")
        replacements = {}

        if conditions:
            conditions_list = conditions.split("|")
            for idx, condition in enumerate(conditions_list):
                col_name, col_value = condition.split(",")


                detected_format = detect_date_format(col_value)
                if not detected_format:
                    print(f"Skipping column {col_name}, unrecognized date format: {col_value}")
                    continue


                query_date_format = expected_formats[idx] if idx < len(expected_formats) else "%Y-%m-%d"


                parsed_date = datetime.datetime.strptime(col_value, detected_format).strftime("%Y-%m-%d")


                replacements[f"{{col{idx+1}}}"] = col_name
                replacements[f"{{date{idx+1}}}"] = parsed_date


            for placeholder, value in replacements.items():
                query_template = query_template.replace(placeholder, value)


        if re.search(r"\{col\d+\}|\{date\d+\}", query_template):
            print(f"Skipping execution: Query {query_id} still has placeholders that were not replaced.")
            continue  

        count_source = execute_query(DB_CONFIG, query_template)
        count_target = execute_query(TARGET_DB_CONFIG, query_template)
        status = "matched" if count_source == count_target else "Not matched"


        csv_writer.writerow([
            f"{DB_CONFIG['username']},****",
            f"{TARGET_DB_CONFIG['username']},****",
            DB_CONFIG["host"], TARGET_DB_CONFIG["host"],
            source_table, target_table,
            count_source, count_target,
            status, datetime.datetime.now().strftime("%I:%M %p"),
            datetime.datetime.now().strftime("%I:%M %p %d-%m-%Y")
        ])

print(f"JSON file dynamically updated at {json_file_path}")
print(f"CSV output saved at {output_csv_file_path}")

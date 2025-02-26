import json
import cx_Oracle
import datetime
import re
import csv


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


json_file_path = "path/to/metadata.json"
query_template_file = "path/to/input_queries.txt"
output_csv_file_path = "path/to/output.csv"
updated_json_file = "path/to/updated_metadata.json"


def execute_query(db_config, query):
    try:
        dsn = cx_Oracle.makedsn(db_config["host"], db_config["port"], service_name=db_config["service"])
        conn = cx_Oracle.connect(db_config["username"], db_config["password"], dsn)
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        print(f"Error executing query: {query} -> {e}")
        return None


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

    updated_metadata = []
    for entry in metadata:

        query_id = entry["id"]
        if query_id not in query_templates:
            print(f"Skipping ID {query_id}, no matching query found.")
            continue

        query_template = query_templates[query_id]
        source_table = entry["source_table"]
        target_table = entry["target_table"]
        conditions = entry["source_col_list_for_where_condition"].split("|")

        
        replacements = {"{source_table}": source_table, "{target_table}": target_table}

        
        for idx, condition in enumerate(conditions):
            col_name, col_value = condition.split(",")
            replacements[f"{{col{idx+1}}}"] = col_name
            replacements[f"{{date{idx+1}}}"] = col_value

        
        for placeholder, value in replacements.items():
            query_template = query_template.replace(placeholder, value)

        
        start_time = datetime.datetime.now().strftime("%I:%M %p")
        count_source = execute_query(DB_CONFIG, query_template)
        count_target = execute_query(TARGET_DB_CONFIG, query_template)

        
        status = "matched" if count_source == count_target else "Not matched"

        # Find max column value for each condition
        new_conditions = []
        for condition in conditions:
            col_name, _ = condition.split(",")
            max_query = f"SELECT MAX({col_name}) FROM {source_table}"
            max_value = execute_query(DB_CONFIG, max_query)
            new_conditions.append(f"{col_name},{max_value}")

        # Update metadata dynamically
        entry["source_col_list_for_where_condition"] = "|".join(new_conditions)
        updated_metadata.append(entry)


        last_run = datetime.datetime.now().strftime("%I:%M %p %d-%m-%Y")

        # Write row to CSV file
        csv_writer.writerow([
            f"{DB_CONFIG['username']},{DB_CONFIG['password']}",
            f"{TARGET_DB_CONFIG['username']},{TARGET_DB_CONFIG['password']}",
            DB_CONFIG["host"], TARGET_DB_CONFIG["host"],
            source_table, target_table,
            count_source, count_target,
            status, start_time, last_run
        ])


with open(updated_json_file, "w") as file:
    json.dump(updated_metadata, file, indent=4)

print(f"Updated JSON saved at {updated_json_file}")
print(f"CSV output saved at {output_csv_file_path}")

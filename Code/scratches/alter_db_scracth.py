import mysql.connector
from Code.Constant import *


def update_variety_table(xml_file, db_config):
    try:
        import xml.etree.ElementTree as ET
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Parse XML
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Namespace management
        ns = {'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
              'xsd': 'http://www.w3.org/2001/XMLSchema'}

        # Extract all parameter keys
        keys = set()
        for param_item in root.findall('.//CropParameterItem', ns):
            for item in param_item.findall('.//Item', ns):
                key = item.find('./Key/string', ns).text
                keys.add(key)

        # Check if column exists and add missing columns
        for key in keys:
            column_name = key
            check_query = f"""
            SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = '{db_config['database']}' 
              AND TABLE_NAME = 'variety' 
              AND COLUMN_NAME = '{column_name}';
            """
            cursor.execute(check_query)
            column_exists = cursor.fetchone()[0]

            if column_exists == 0:  # If the column doesn't exist
                alter_query = f"""
                ALTER TABLE parameters
                ADD COLUMN `{column_name}` DOUBLE NULL;
                """
                try:
                    cursor.execute(alter_query)
                    print(f"Added column: {column_name}")
                except mysql.connector.Error as e:
                    print(f"Error adding column {column_name}: {e}")


        # Insert data into the table
        for param_item in root.findall('.//CropParameterItem', ns):
            name = param_item.attrib.get('name', 'Unknown')
            values = {item.find('./Key/string', ns).text:
                          float(item.find('./Value/double', ns).text)
                      for item in param_item.findall('.//Item', ns)}

            columns = ['name'] + list(values.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"""
            INSERT INTO parameters ({', '.join(columns)})
            VALUES ({placeholders})
            """
            data = [name] + list(values.values())

            try:
                cursor.execute(insert_query, data)
            except mysql.connector.Error as e:
                print(f"Error inserting data for {name}: {e}")

        connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    ##ALTER/CREATE obs_table
# import pandas as pd
# import os
#
#
# # Function to infer SQL data type
# def infer_sql_type(series):
#     if pd.api.types.is_float_dtype(series):
#         return "FLOAT"
#     elif pd.api.types.is_datetime64_any_dtype(series):
#         return "DATE"
#     elif pd.api.types.is_bool_dtype(series):
#         return "BOOLEAN"
#     else:
#         max_len = series.astype(str).str.len().max()
#         return f"VARCHAR({max(max_len, 255)})"  # Adjust VARCHAR length if needed
#
#
# # Function to generate SQL schema for a table
# def generate_sql_schema(table_name, df):
#     schema = f"CREATE TABLE {table_name} (\n"
#     schema += "    TRT_NAME VARCHAR(255) NOT NULL,\n"
#     schema += "    CUL_ID VARCHAR(255) NOT NULL,\n"
#     for col in df.columns:
#         if col not in ['TRT_NAME', 'CUL_ID']:
#             col_type = infer_sql_type(df[col])
#             schema += f"    {col} {col_type},\n"
#     schema = schema.rstrip(",\n") + "\n);\n"  # Remove the last comma and close the table
#     return schema
#
#
# # Main function to process Excel files and generate SQL schemas
# def process_excel_files(directory):
#     all_sheets = {}  # Dictionary to group DataFrames by sheet name
#
#     # Read all Excel files in the directory
#     for file in os.listdir(directory):
#         print(file)
#         if file.endswith(".xlsx"):
#             excel_path = os.path.join(directory, file)
#             sheets = pd.read_excel(excel_path, sheet_name=None)
#             for sheet_name, df in sheets.items():
#                 if sheet_name not in all_sheets:
#                     all_sheets[sheet_name] = []
#                 all_sheets[sheet_name].append(df)
#
#     # Combine DataFrames by sheet name and generate schemas
#     sql_schemas = {}
#     for sheet_name, dfs in all_sheets.items():
#         combined_df = pd.concat(dfs, ignore_index=True)
#         sql_schemas[sheet_name] = generate_sql_schema(sheet_name, combined_df)
#
#     return sql_schemas
#
#
# # Example usage
# if __name__ == "__main__":
#     # Replace with the directory containing your Excel files
#     directory = "path_to_excel_files"
#     schemas = process_excel_files("C:/Users/royerpie/Documents/rootDoc/automate/observations/PseudoAGMIP")
#
#     for table_name, schema in schemas.items():
#         print(f"-- Schema for {table_name}\n{schema}\n")





xml_file = "C:/Users/royerpie/Documents/rootDoc/Working_Immuable/myProject/fake/myDummies/1-Project/dummy_parm.sqparm"

update_variety_table(xml_file, db_config)
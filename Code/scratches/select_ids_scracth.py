import pickle

import pandas as pd

import Code.Constant as Constant
import Code.functions.HandleDB as HandleDB
from Code.Import import *
import os


def get_obs_list(db_config):

    observed = pd.DataFrame(columns=['TRT_NAME', 'CUL_ID', 'source_database'])
    # Connect to the database
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Execute the query to show all tables
    cursor.execute('SHOW TABLES')
    table_list = cursor.fetchall()

    # Loop through all the tables
    for table in table_list:
        table_name = table[0]
        print(table_name)

        # If the table name contains 'obs', select TRT_NAME and CUL_ID
        if 'obs' in table_name:
            cursor.execute(f'SELECT TRT_NAME, CUL_ID, source_database FROM {table_name}')
            pair = cursor.fetchall()

            # Convert the result (pair) into a DataFrame and append it to the observed DataFrame
            if pair:  # Check if there is any data to append
                pair_df = pd.DataFrame(pair, columns=['TRT_NAME', 'CUL_ID', 'source_database'])
                observed = pd.concat([observed, pair_df], ignore_index=True)

    # Close the database connection
    cursor.close()
    conn.close()

    # Optionally, print the final observed DataFrame
    return observed.drop_duplicates()

mine = get_obs_list(Constant.db_config)

print(mine['CUL_ID'].unique())
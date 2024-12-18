from Code import Constant
from Code.functions.HandleDB import *
from Code.Import import *


def main_project_no_run(project_file, project_name, db_config, model_version):
    """
    Main function to process a project file, retrieve associated file paths, and insert data into the database.

    Parameters:
    project_file (str): Path to the project file (.sqpro).
    project_name (str): Name of the project.
    db_config (dict): Database connection configuration.

    Returns:
    int: The project ID inserted into the database.
    """
    project_ID = None
    cursor = None
    db_connection = None

    try:
        # Load project data into the database and get project ID
        project_ID, _ = load_project_db(project_file, db_config, project_name)
        print(f"Project '{project_name}' loaded with ID {project_ID}")

        # Retrieve associated paths from the database
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)

        select_project_query = """
        SELECT dir_path, ManagementFileName AS man_file, SoilFileName AS soil_file, SiteFileName AS site_file, MaizeVarietyFileName AS var_file, RunFileName AS run_file, MaizeNonVarietyFileName AS nonvariety_file, RunFileName AS run_file
        FROM project
        WHERE id = %s
        """
        cursor.execute(select_project_query, (project_ID,))
        project_data = cursor.fetchone()

        if not project_data:
            raise ValueError(f"Project with ID {project_ID} not found in the database.")

        # Extract paths
        dir_path = project_data['dir_path']
        man_file = os.path.join(dir_path, project_data['man_file'])
        soil_file = os.path.join(dir_path, project_data['soil_file'])
        site_file = os.path.join(dir_path, project_data['site_file'])
        var_file = os.path.join(dir_path, project_data['var_file'])
        nonvar_file = os.path.join(dir_path, project_data['nonvariety_file'])
        run_file = os.path.join(dir_path, project_data['run_file'])

        # Process management data
        man_ids, man_file = load_man_db(man_file, db_config, project_name, project_ID, debug=False)
        print(f"Management data processed. File: {man_file}, IDs: {man_ids}")
        # Process soil data
        soil_ids, soil_file = load_soil_db(soil_file, db_config, project_name, project_ID)
        print(f"Soil data processed. File: {soil_file}, IDs: {soil_ids}")
        # Process site data
        site_ids, site_file = load_site_db(site_file, db_config, project_name, project_ID)
        print(f"Site data processed. File: {site_file}, IDs: {site_ids}")
        var_ids, var_file = load_parameters_db(var_file, db_config, model_version)
        print(f"Varietal data processed. File: {var_file}, IDs: {var_ids}")
        nonvar_ids, nonvar_file = load_parameters_db(nonvar_file, db_config, model_version)
        print(f"Non varietal data processed. File: {nonvar_file}, IDs: {nonvar_ids}")
        # run_ids, run_file = load_run_items_db(run_file, db_config, project_name, project_ID)
        # print(f"Run data processed. File: {run_file}, IDs: {run_ids}")

            ## clean and check for duplicates part;
                ##for project
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)

        project_fk = get_foreign_keys(cursor, project_ID)
        project_cols_to_check = get_columns_to_check(cursor, 'project', project_fk, {'ID', 'created_at', 'updated_at'})
        curr_project_value = get_values_of_row_by_id(cursor, 'project', project_ID, project_cols_to_check)
        # print('curr row', curr_project_value)
        project_duplicates = find_duplicates(cursor, 'project', curr_project_value.keys(), curr_project_value.values())
        # print('all identical ids ', project_duplicates)
        ids_to_remove = prepare_ids_to_delete(project_duplicates, None)

            ## Keep as project the oldest

        # print(ids_to_remove)
        if len(ids_to_remove) > 1:
            project_ID = ids_to_remove[0]
            ids_to_remove = ids_to_remove[1:]
            delete_project_query = f"""
                               DELETE FROM project
                               WHERE ID IN ({','.join(map(str, ids_to_remove))});
                           """
            # print(delete_project_query)
            output = cursor.execute(delete_project_query)
            # print(output)
            db_connection.commit()

            ##for parameters

        parameters_col_to_check = get_columns_to_check(cursor,'parameters', None, {'ID', 'created_at', 'updated_at'})
        # print(var_ids)
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)
        for item, key in var_ids.items():

            curr_parameter_value = get_values_of_row_by_id(cursor, 'parameters', key, parameters_col_to_check)
            parameters_duplicate = find_duplicates(cursor, 'parameters', curr_parameter_value.keys(), curr_parameter_value.values())
            # print(parameters_duplicate)
            ids_to_remove = prepare_ids_to_delete(parameters_duplicate, None)
            ids_to_remove = ids_to_remove[1:]
            # print(ids_to_remove)
            if len(ids_to_remove) > 0:
                delete_project_query = f"""
                                           DELETE FROM parameters
                                           WHERE ID IN ({','.join(map(str, ids_to_remove))});
                                       """
                # print(delete_project_query)
                output = cursor.execute(delete_project_query)
                # print(output)
                db_connection.commit()

        for item, key in nonvar_ids.items():
            curr_parameter_value = get_values_of_row_by_id(cursor, 'parameters', key, parameters_col_to_check)
            parameters_duplicate = find_duplicates(cursor, 'parameters', curr_parameter_value.keys(), curr_parameter_value.values())
            ids_to_remove = prepare_ids_to_delete(parameters_duplicate, None)
            ids_to_remove = ids_to_remove[1:]
            # print(ids_to_remove)
            if len(ids_to_remove) > 0:
                delete_project_query = f"""
                                           DELETE FROM parameters
                                           WHERE ID IN ({','.join(map(str, ids_to_remove))});
                                       """
                # print(delete_project_query)
                output = cursor.execute(delete_project_query)
                # print(output)
                db_connection.commit()
        # print(var_ids)

    except Exception as e:
        print(f"Error occurred: {e}")
        raise
    finally:
        # Ensure cursor and db_connection are closed properly
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

    return project_ID

# Example usage (make sure Constant.db_config is a valid dictionary)
if __name__ == "__main__":
    try:
        # Assuming `Constant.db_config` is a dictionary like {'host': 'localhost', 'user': 'root', ...}
        db_config = Constant.db_config
        project_file = "C:/Users/royerpie/Documents/rootDoc/Working_Immuable/myProject/cross/2.2.0/APEX_Theophile_1.0.0/1-Project/Apex_inra.sqpro"
        project_name = "Apex"
        model_version = '2.2.0'

        project_id = main_project_no_run(project_file, project_name, db_config,model_version)

        print(f"Project ID: {project_id}")

    except Exception as e:
        print(f"An error occurred: {e}")




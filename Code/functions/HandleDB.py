from Code.Constant import *
from Code.Import import *
import pandas as pd
import numpy as np


        ##Load XML to  DB


def load_project_db(xml_file, db_config, name):
    """
    Function to read the XML project file and insert data into the 'projet' table.

    :param xml_file: Path to the XML file
    :param db_config: Dictionary with database connection details
    :param name: Project name
    :return: Tuple of inserted project ID and project name
    """
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract the file names from the XML
    inputs = root.find('Inputs')
    file_name = os.path.basename(xml_file)
    dir_path = os.path.dirname(os.path.abspath(xml_file))
    # print(dir_path)
    # Helper function to replace `None` or `?` with 'NA'


    # Prepare project data, sanitizing the input values
    project_data = {
        'project_name': name,
        'file_name': file_name,
        'dir_path': dir_path,
        'OptimizationFileName': sanitize(inputs.find('OptimizationFileName').text if inputs.find('OptimizationFileName') is not None else None),
        'ObservationFileName': sanitize(inputs.find('ObservationFileName').text if inputs.find('ObservationFileName') is not None else None),
        'ManagementFileName': sanitize(inputs.find('ManagementFileName').text if inputs.find('ManagementFileName') is not None else None),
        'NonVarietyFileName': sanitize(inputs.find('NonVarietyFileName').text if inputs.find('NonVarietyFileName') is not None else None),
        'MaizeNonVarietyFileName': sanitize(inputs.find('MaizeNonVarietyFileName').text if inputs.find('MaizeNonVarietyFileName') is not None else None),
        'GlobNonVarietyFileName': sanitize(inputs.find('GlobNonVarietyFileName').text if inputs.find('GlobNonVarietyFileName') is not None else None),
        'RunOptionFileName': sanitize(inputs.find('RunOptionFileName').text if inputs.find('RunOptionFileName') is not None else None),
        'SiteFileName': sanitize(inputs.find('SiteFileName').text if inputs.find('SiteFileName') is not None else None),
        'SoilFileName': sanitize(inputs.find('SoilFileName').text if inputs.find('SoilFileName') is not None else None),
        'VarietyFileName': sanitize(inputs.find('VarietyFileName').text if inputs.find('VarietyFileName') is not None else None),
        'MaizeVarietyFileName': sanitize(inputs.find('MaizeVarietyFileName').text if inputs.find('MaizeVarietyFileName') is not None else None),
        'RunFileName': sanitize(inputs.find('RunFileName').text if inputs.find('RunFileName') is not None else None),
        'OutputFormatFileName': sanitize(inputs.find('OutputFormatFileName').text if inputs.find('OutputFormatFileName') is not None else None),
        'EnvirotypingFileName': sanitize(inputs.find('EnvirotypingFileName').text if inputs.find('EnvirotypingFileName') is not None else None),
        'ModelConfigurationFileName': sanitize(inputs.find('ModelConfigurationFileName').text if inputs.find('ModelConfigurationFileName') is not None else None),
        'DatFileName': sanitize(inputs.find('DatFileName').text if inputs.find('DatFileName') is not None else None),
        'BcvsFileName': sanitize(inputs.find('BcvsFileName').text if inputs.find('BcvsFileName') is not None else None),
        'IvsFileName': sanitize(inputs.find('IvsFileName').text if inputs.find('IvsFileName') is not None else None),
        'Comments': sanitize(inputs.find('Comments').text if inputs.find('Comments') is not None else None),
    }

    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Insert the data into the 'projet' table
        insert_query = """
            INSERT  IGNORE INTO `project` (
                `project_name`,`file_name`,`dir_path`, `OptimizationFileName`, `ObservationFileName`, `ManagementFileName`, `NonVarietyFileName`,
                `MaizeNonVarietyFileName`, `GlobNonVarietyFileName`, `RunOptionFileName`, `SiteFileName`, `SoilFileName`,
                `VarietyFileName`, `MaizeVarietyFileName`, `RunFileName`, `OutputFormatFileName`, `EnvirotypingFileName`,
                `ModelConfigurationFileName`, `DatFileName`, `BcvsFileName`, `IvsFileName`, `Comments`
            )
            VALUES (
                %(project_name)s,%(file_name)s,%(dir_path)s, %(OptimizationFileName)s, %(ObservationFileName)s, %(ManagementFileName)s, %(NonVarietyFileName)s,
                %(MaizeNonVarietyFileName)s, %(GlobNonVarietyFileName)s, %(RunOptionFileName)s, %(SiteFileName)s, %(SoilFileName)s,
                %(VarietyFileName)s, %(MaizeVarietyFileName)s, %(RunFileName)s, %(OutputFormatFileName)s, %(EnvirotypingFileName)s,
                %(ModelConfigurationFileName)s, %(DatFileName)s, %(BcvsFileName)s, %(IvsFileName)s, %(Comments)s
            )
           
        """

        cursor.execute(insert_query, project_data)
        conn.commit()

        project_id = cursor.lastrowid  # Retrieve the ID of the last inserted row
        # print("Project data inserted successfully!")
        return project_id, name

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None, name

    finally:
        cursor.close()
        conn.close()

def load_date_app_db(cursor, man_ID, management_item):
    """
    Insert data into the 'date_application' table.

    Parameters:
    cursor: The MySQL cursor object for executing queries.
    man_ID (int): The ID from the 'management' table.
    management_item (Element): The XML element representing a ManagementItem.
    """
    # Extract the date_application-related data from the management_item
    date_applications = management_item.findall('DateApplications/DateApplication')

    # Loop through all DateApplications and insert them
    for date_application in date_applications:
        nitrogen = date_application.find('Nitrogen')
        fertilizer_name = date_application.find('FertilizerName')
        ammonium_fraction = date_application.find('AmmoniumFraction')
        water = date_application.find('Water')
        water_mm = date_application.find('WaterMM')
        application_date = date_application.find('Date')

        # Prepare the data for inserting into the date_application table
        date_application_data = {
            'man_ID': man_ID,  # Link to the management entry via the ID
            'Nitrogen': nitrogen.text if nitrogen is not None else None,
            'FertilizerName': fertilizer_name.text if fertilizer_name is not None else None,
            'AmmoniumFraction': ammonium_fraction.text if ammonium_fraction is not None else None,
            'Water': water.text if water is not None else None,
            'WaterMM': water_mm.text if water_mm is not None else None,
            'Date': application_date.text if application_date is not None else None,
        }

        # Prepare the SQL query for inserting into the date_application table
        # The created_at column is handled automatically by SQL, but updated_at needs to be set
        date_application_placeholders = ', '.join(['%s'] * len(date_application_data))  # Use %s for placeholders
        date_application_columns = ', '.join(date_application_data.keys())  # Get the column names
        date_application_insert_query = f"INSERT INTO date_application ({date_application_columns}) VALUES ({date_application_placeholders})"

        # Execute the insert query for date_application
        # print(date_application_insert_query)  # Optionally print the query for debugging
        cursor.execute(date_application_insert_query, tuple(date_application_data.values()))

def load_man_db(xml_file, db_config, name, project_ID=None, debug=False):
    """
    Parse the given XML file and insert data into the 'management' and 'date_application' tables.
    Prevents duplicate rows by checking for existing records.
    """


    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract the ManagementItems
    management_items = root.findall('.//ManagementItem')

    if not management_items:
        print("No ManagementItems found in the XML file.")
        return {}

    result = {}  # To store ManagementItem names and their IDs

    try:
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor()

        # Get column names from the 'management' table
        cursor.execute("DESCRIBE management")
        columns = [col[0] for col in cursor.fetchall()]

        file_name = os.path.basename(xml_file)
        dir_path = os.path.dirname(xml_file)

        # Define the columns to use for checking duplicates
        unique_columns = ['name', 'file_name', 'dir_path', 'ManagementItem']

        for management_item in management_items:
            management_name = management_item.attrib['name']
            if debug:
                print(f"Processing ManagementItem: {management_name}")

            # Prepare data dictionary
            data = {
                col: (management_item.find(col).text if management_item.find(col) is not None else None)
                for col in columns
            }
            data.update({
                'name': name,
                'file_name': file_name,
                'ManagementItem': management_name,
                'dir_path': dir_path,
                'project_ID': project_ID
            })
            data.pop('created_at', None)
            data.pop('updated_at', None)




            # Prepare the INSERT query
            placeholders = ', '.join(['%s'] * len(data))
            column_names = ', '.join(data.keys())
            insert_query = f"INSERT INTO management ({column_names}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(data.values()))

            # Retrieve the inserted ID
            man_ID = cursor.lastrowid
            result[management_name] = man_ID

            # Insert associated date application data
            load_date_app_db(cursor, man_ID, management_item)

        db_connection.commit()
        if debug:
            print("Management and Date Application data inserted successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

    return result, file_name

def load_soil_layer_db(cursor, soil_ID, soil_item):
    """
    Insert data into the 'soil_layer' table.

    Parameters:
    cursor: The MySQL cursor object for executing queries.
    soil_ID (int): The ID from the 'soil' table.
    soil_item (Element): The XML element representing a SoilItem.
    """
    # Extract the SoilLayers related to the soil_item
    soil_layers = soil_item.findall('LayersArray/SoilLayer')

    # Loop through all SoilLayers and insert them
    for soil_layer in soil_layers:
        # Extract values for each relevant element inside SoilLayer
        growth = soil_layer.find('growth')
        ramif = soil_layer.find('ramif')
        iCMeca = soil_layer.find('iCMeca')
        oCMeca = soil_layer.find('oCMeca')
        clay = soil_layer.find('Clay')
        clay_user = soil_layer.find('ClayUser')
        clay_est = soil_layer.find('ClayEst')
        silt = soil_layer.find('Silt')
        silt_user = soil_layer.find('SiltUser')
        silt_est = soil_layer.find('SiltEst')
        sand = soil_layer.find('Sand')
        sand_user = soil_layer.find('SandUser')
        sand_est = soil_layer.find('SandEst')
        kql = soil_layer.find('Kql')
        kql_user = soil_layer.find('KqlUser')
        kql_est = soil_layer.find('KqlEst')
        ssat = soil_layer.find('SSAT')
        ssat_user = soil_layer.find('SSATUser')
        ssat_est = soil_layer.find('SSATEst')
        sdul = soil_layer.find('SDUL')
        sdul_user = soil_layer.find('SDULUser')
        sdul_est = soil_layer.find('SDULEst')
        sll = soil_layer.find('SLL')
        sll_user = soil_layer.find('SLLUser')
        sll_est = soil_layer.find('SLLEst')
        corg = soil_layer.find('COrg')
        depth = soil_layer.find('Depth')
        norg_layer = soil_layer.find('NorgLayer')
        ko = soil_layer.find('Ko')
        bd = soil_layer.find('Bd')
        bd_user = soil_layer.find('BdUser')
        bd_est = soil_layer.find('BdEst')
        texture = soil_layer.find('Texture')
        residual = soil_layer.find('Residual')
        residual_user = soil_layer.find('ResidualUser')
        residual_est = soil_layer.find('ResidualEst')
        saturated = soil_layer.find('Saturated')
        saturated_user = soil_layer.find('SaturatedUser')
        saturated_est = soil_layer.find('SaturatedEst')
        alpha = soil_layer.find('Alpha')
        alpha_user = soil_layer.find('AlphaUser')
        alpha_est = soil_layer.find('AlphaEst')
        n = soil_layer.find('N')
        n_user = soil_layer.find('NUser')
        n_est = soil_layer.find('NEst')
        air_entry = soil_layer.find('AirEntry')
        air_entry_user = soil_layer.find('AirEntryUser')
        air_entry_est = soil_layer.find('AirEntryEst')
        b = soil_layer.find('B')
        b_user = soil_layer.find('BUser')
        b_est = soil_layer.find('BEst')

        # Prepare the data for inserting into the soil_layer table
        soil_layer_data = {
            'soil_ID': soil_ID,  # Link to the soil entry via the ID
            'growth': growth.text if growth is not None else None,
            'ramif': ramif.text if ramif is not None else None,
            'iCMeca': iCMeca.text if iCMeca is not None else None,
            'oCMeca': oCMeca.text if oCMeca is not None else None,
            'Clay': clay.text if clay is not None else None,
            'ClayUser': clay_user.text if clay_user is not None else None,
            'ClayEst': clay_est.text if clay_est is not None else None,
            'Silt': silt.text if silt is not None else None,
            'SiltUser': silt_user.text if silt_user is not None else None,
            'SiltEst': silt_est.text if silt_est is not None else None,
            'Sand': sand.text if sand is not None else None,
            'SandUser': sand_user.text if sand_user is not None else None,
            'SandEst': sand_est.text if sand_est is not None else None,
            'Kql': kql.text if kql is not None else None,
            'KqlUser': kql_user.text if kql_user is not None else None,
            'KqlEst': kql_est.text if kql_est is not None else None,
            'SSAT': ssat.text if ssat is not None else None,
            'SSATUser': ssat_user.text if ssat_user is not None else None,
            'SSATEst': ssat_est.text if ssat_est is not None else None,
            'SDUL': sdul.text if sdul is not None else None,
            'SDULUser': sdul_user.text if sdul_user is not None else None,
            'SDULEst': sdul_est.text if sdul_est is not None else None,
            'SLL': sll.text if sll is not None else None,
            'SLLUser': sll_user.text if sll_user is not None else None,
            'SLLEst': sll_est.text if sll_est is not None else None,
            'COrg': corg.text if corg is not None else None,
            'Depth': depth.text if depth is not None else None,
            'NorgLayer': norg_layer.text if norg_layer is not None else None,
            'Ko': ko.text if ko is not None else None,
            'Bd': bd.text if bd is not None else None,
            'BdUser': bd_user.text if bd_user is not None else None,
            'BdEst': bd_est.text if bd_est is not None else None,
            'Texture': texture.text if texture is not None else None,
            'Residual': residual.text if residual is not None else None,
            'ResidualUser': residual_user.text if residual_user is not None else None,
            'ResidualEst': residual_est.text if residual_est is not None else None,
            'Saturated': saturated.text if saturated is not None else None,
            'SaturatedUser': saturated_user.text if saturated_user is not None else None,
            'SaturatedEst': saturated_est.text if saturated_est is not None else None,
            'Alpha': alpha.text if alpha is not None else None,
            'AlphaUser': alpha_user.text if alpha_user is not None else None,
            'AlphaEst': alpha_est.text if alpha_est is not None else None,
            'N': n.text if n is not None else None,
            'NUser': n_user.text if n_user is not None else None,
            'NEst': n_est.text if n_est is not None else None,
            'AirEntry': air_entry.text if air_entry is not None else None,
            'AirEntryUser': air_entry_user.text if air_entry_user is not None else None,
            'AirEntryEst': air_entry_est.text if air_entry_est is not None else None,
            'B': b.text if b is not None else None,
            'BUser': b_user.text if b_user is not None else None,
            'BEst': b_est.text if b_est is not None else None
        }

        # Prepare the SQL query for inserting into the soil_layer table
        soil_layer_placeholders = ', '.join(['%s'] * len(soil_layer_data))  # Use %s for placeholders
        soil_layer_columns = ', '.join(soil_layer_data.keys())  # Get the column names
        soil_layer_insert_query = f"INSERT IGNORE INTO soil_layer ({soil_layer_columns}) VALUES ({soil_layer_placeholders})"

        # Execute the insert query for soil_layer
        # print(soil_layer_insert_query)  # Optionally print the query for debugging
        cursor.execute(soil_layer_insert_query, tuple(soil_layer_data.values()))

def load_soil_db(xml_file, db_config, name, project_ID=None, debug=False):
    """
    Parse the given XML file and insert data into the 'soil' table.

    Parameters:
    xml_file (str): Path to the XML file.
    db_config (dict): Database connection configuration.
    name (str): Project name.
    project_ID (int, optional): Project ID, if available. Defaults to None.
    debug (bool): If True, print debug information. Defaults to False.

    Returns:
    dict: A dictionary with SoilItem names as keys and their respective IDs as values.
    """
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract the SoilItems
    soil_items = root.findall('.//SoilItem')

    if not soil_items:
        print("No SoilItems found in the XML file.")
        return {}

    result = {}  # To store SoilItem names and their IDs

    try:
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor()

        # Get column names from the 'soil' table
        cursor.execute("DESCRIBE soil")
        columns = [col[0] for col in cursor.fetchall()]

        # Get the file name and directory path from the XML file
        file_name = os.path.basename(xml_file)
        dir_path = os.path.dirname(xml_file)

        # Insert data for each SoilItem
        for soil_item in soil_items:
            soil_name = soil_item.attrib['name']
            if debug:
                print(f"Processing SoilItem: {soil_name}")

            data = {
                col: (soil_item.find(col).text if soil_item.find(col) is not None else None)
                for col in columns
            }

            # Add metadata
            data.update({
                'name': name,
                'file_name': file_name,
                'SoilItem': soil_name,
                'dir_path': dir_path,
                'project_ID': project_ID
            })

            # Remove auto-handled fields
            data.pop('created_at', None)
            data.pop('updated_at', None)

            # Prepare and execute the SQL query
            placeholders = ', '.join(['%s'] * len(data))
            column_names = ', '.join(data.keys())
            insert_query = f"INSERT IGNORE INTO soil ({column_names}) VALUES ({placeholders})"
            if debug:
                print(insert_query)

            cursor.execute(insert_query, tuple(data.values()))

            # Retrieve the inserted ID
            soil_ID = cursor.lastrowid
            result[soil_name] = soil_ID

            # Insert associated soil layer data
            load_soil_layer_db(cursor, soil_ID, soil_item)

        db_connection.commit()
        if debug:
            print("Soil and Soil Layer data inserted successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

    return result, file_name

def load_site_db(xml_file, db_config, name, project_ID=None, debug=False):
    """
    Parse the given XML file and insert data into the 'site' table.

    Parameters:
    xml_file (str): Path to the XML file.
    db_config (dict): Database connection configuration.
    name (str): Project name.
    project_ID (int, optional): Project ID, if available. Defaults to None.
    debug (bool): If True, print debug information. Defaults to False.

    Returns:
    dict: A dictionary with SiteItem names as keys and their respective IDs as values.
    """
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract the SiteItems
    site_items = root.findall('.//SiteItem')

    if not site_items:
        print("No SiteItems found in the XML file.")
        return {}

    result = {}  # To store SiteItem names and their IDs

    try:
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor()

        # Get column names from the 'site' table
        cursor.execute("DESCRIBE site")
        columns = [col[0] for col in cursor.fetchall()]

        # Get the file name and directory path from the XML file
        file_name = os.path.basename(xml_file)
        dir_path = os.path.dirname(xml_file)

        # Insert data for each SiteItem
        for site_item in site_items:
            site_name = site_item.attrib['name']
            site_format = site_item.attrib.get('format', None)



            data = {
                col: (site_item.find(col).text if site_item.find(col) is not None else None)
                for col in columns
            }
            weather_files = site_item.findall('.//WeatherFiles/WeatherFile')
            for weather_file in weather_files:
                file_path = weather_file.attrib.get('file', None)
                if file_path:
                    # print(file_path)
                    weather_dir = os.path.dirname(file_path)
                    weather_file_name = os.path.basename(file_path)
                    if debug:
                        print(f"Associated WeatherFile: {weather_file_name} in {weather_dir}")
                    # Logic to handle weather files can go here, if needed

            weatherfile = os.path.join(dir_path, file_path)
            if debug:
                print(f"Processing SiteItem: {site_name}")
                print('isititi?', weatherfile)
            #
            # Add metadata
            data.update({
                'name': name,
                'file_name': file_name,
                'dir_path': dir_path,
                'SiteItem': site_name,
                'Site_format': site_format,
                'project_ID': project_ID,
                'WeatherFile': weatherfile
            })
            # Process associated weather files

            # Remove auto-handled fields
            data.pop('created_at', None)
            data.pop('updated_at', None)

            # Prepare and execute the SQL query
            placeholders = ', '.join(['%s'] * len(data))
            column_names = ', '.join(data.keys())
            insert_query = f"INSERT IGNORE INTO site ({column_names}) VALUES ({placeholders})"
            # print(insert_query)
            if debug:
                print(insert_query)

            cursor.execute(insert_query, tuple(data.values()))

            # Retrieve the inserted ID
            site_ID = cursor.lastrowid
            result[site_name] = site_ID


        db_connection.commit()
        if debug:
            print("Site data inserted successfully!")

    except mysql.connector.Error as err:
        print("in laod_site_db")
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

    return result, file_name

def load_parameters_db(xml_file, db_config, model_version=None, debug=False):
    """
    Load parameters from a .sqvarm or .sqparm file into the database.

    Parameters:
    - xml_file (str): Path to the XML file.
    - db_config (dict): Database connection configuration.
    - model_version (str, optional): Version of the model. Defaults to None.
    - debug (bool): If True, print debug information. Defaults to False.

    Returns:
    - dict: A dictionary with CropParameterItem names as keys and their respective IDs as values.
    """


    # Determine the type of file: varietal or non-varietal
    file_type = "varietal" if xml_file.endswith(".sqvarm") else "non-varietal"
    file_name = os.path.basename(xml_file)
    dir_path = os.path.dirname(xml_file)

    # Parse the XML file
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return {}

    # Extract CropParameterItems
    items_array = root.find(".//ItemsArray")
    if not items_array:
        print("No CropParameterItems found in the XML file.")
        return {}

    crop_items = items_array.findall(".//CropParameterItem")
    if not crop_items:
        print("No valid CropParameterItem elements in the XML file.")
        return {}

    result = {}  # To store CropParameterItem names and their IDs

    try:
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor()

        # Get column names from the relevant table (once)
        table = "parameters"
        cursor.execute(f"DESCRIBE {table}")
        columns = [col[0] for col in cursor.fetchall()]

        # Process each CropParameterItem
        for item in crop_items:
            name = item.get("name")
            if not name:
                continue

            if debug:
                print(f"Processing {file_type} parameter: {name}")

            # Metadata for the CropParameterItem

            # Prepare data for insertion
            data = {}
            param_values = item.find(".//ParamValue")

            for col in columns:
                # Search for a parameter with the given key
                value = None
                if param_values is not None:
                    for param_item in param_values.findall("Item"):
                        key_element = param_item.find("Key/string")
                        if key_element is not None and key_element.text == col:
                            value_element = param_item.find("Value/double")
                            value = value_element.text if value_element is not None else None
                            break
                data[col] = value

            # Combine metadata and parameters
            data["name" ] =  name
            data["model_version"] =  model_version
            data["file_name"] = file_name
            data["dir_path"] = dir_path
            data["type"] =  file_type
            data.pop("created_at", None)
            data.pop("updated_at", None)

            # Prepare SQL query for insertion (with ON DUPLICATE KEY UPDATE)
            column_names = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            query = f"""
            INSERT INTO {table} ({column_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE updated_at = NOW(), {', '.join([f'{key} = VALUES({key})' for key in data.keys()])}
            """

            try:
                # print(tuple(data.values()))
                cursor.execute(query, tuple(data.values()))
                db_connection.commit()

                # Retrieve last inserted ID (after insert/update)
                item_id = cursor.lastrowid

                # If no row was inserted (i.e., update occurred), fetch the ID
                if item_id == 0:  # Update, not insert
                    cursor.execute(f"SELECT ID FROM {table} WHERE name = %s", (name,))
                    result_set = cursor.fetchone()
                    if result_set:
                        item_id = result_set[0]

                result[name] = item_id

                if debug:
                    print(f"Inserted/Updated {file_type} parameter: {name}")

            except mysql.connector.Error as e:
                print(f"Error inserting parameter '{name}': {e}")
                db_connection.rollback()

    except mysql.connector.Error as e:
        print(f"Database error: {e}")

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        if debug:
            print("Database connection closed.")

    return result, file_name

def  load_run_db(cursor, run_item_ID, multi):
    indiv_runs = multi.findall(f".//MultiRunsArray/MultiRunItem")
    if not indiv_runs:
        print("No  Run in multi runs found in the XML file.")
        return {}

    try:


        # Get column names from the 'soil' table
        cursor.execute("DESCRIBE run")
        columns = [col[0] for col in cursor.fetchall()]
        # print(columns)
        for run in indiv_runs:
            data = {
            col: (run.find(col).text if run.find(col) is not None else None)
            for col in columns
            }
            data.update({
                'run_item_ID': run_item_ID,
                'run_type': 'Multi'
            })
            data.pop('created_at', None)
            data.pop('updated_at', None)

            placeholders = ', '.join(['%s'] * len(data))
            column_names = ', '.join(data.keys())
            insert_query = f"INSERT INTO run ({column_names}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(data.values()))


    except mysql.connector.Error as err:
        print("in load_run_db")
        print(f"Error: {err}")
        return {}

def load_run_items_db(xml_file, db_config, name, project_ID=None, debug=False):
    """
    Parse the given XML file and insert data into the 'soil' table.

    Parameters:
    xml_file (str): Path to the XML file.
    db_config (dict): Database connection configuration.
    name (str): Project name.
    project_ID (int, optional): Project ID, if available. Defaults to None.
    debug (bool): If True, print debug information. Defaults to False.

    Returns:
    dict: A dictionary with SoilItem names as keys and their respective IDs as values.
    """
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract the SoilItems
    run_items = root.findall('.//RunItem')

    if not run_items:
        print("No RunITems found in the XML file.")
        return {}

    result = {}  # To store run_items names and their IDs

    try:
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor()

        # Get column names from the 'soil' table
        cursor.execute("DESCRIBE run_items")
        columns = [col[0] for col in cursor.fetchall()]
        # print(columns)
        # Get the file name and directory path from the XML file
        file_name = os.path.basename(xml_file)
        dir_path = os.path.dirname(xml_file)

        # Insert data for each SoilItem
        for run_item in run_items:
            run_name = run_item.attrib['name']
            if debug:
                print(f"Processing RunItem: {run_name}")
            multi = run_item.find('Multi')

            data = {
                col: (multi.find(col).text if multi.find(col) is not None else None)
                for col in columns
            }

            # Add metadata
            data.update({
                'name': name,
                'file_name': file_name,
                'RunItem_name': run_name,
                'dir_path': dir_path,
                'project_ID': project_ID
            })

            # Remove auto-handled fields
            data.pop('created_at', None)
            data.pop('updated_at', None)

            # Prepare and execute the SQL query
            placeholders = ', '.join(['%s'] * len(data))
            column_names = ', '.join(data.keys())
            insert_query = f"INSERT INTO run_items ({column_names}) VALUES ({placeholders})"
            if debug:
                print(insert_query)

            cursor.execute(insert_query, tuple(data.values()))

            # Retrieve the inserted ID
            run_item_ID = cursor.lastrowid
            result[run_name] = run_item_ID

            # Insert associated run element ; meaning the content of differnt  MultiRunItem within  MultiRunsArray
            load_run_db(cursor, run_item_ID, multi)

        db_connection.commit()
        if debug:
            print("Run_items a nd associated runs data inserted successfully!")

    except mysql.connector.Error as err:
        print("in load_run_items_db")
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

    return result, file_name




        ### Load Obsezrvation



def load_obs_db(db_config, excel_file, daily_dict, summary_dict, mapping_dict):
    """
    Load data from an Excel file into the database using mysql.connector.

    Args:
        db_config (dict): Database configuration dictionary with keys:
            'host', 'user', 'password', 'database'.
        excel_file (str): Path to the Excel file to be loaded.
        daily_dict (dict): Dictionary specifying date columns for daily data.
        summary_dict (dict): Dictionary specifying date columns for summary data.
    """
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cursor = connection.cursor()

        # Read Excel file
        excel_data = pd.ExcelFile(excel_file)
        sheet_names = excel_data.sheet_names

        # Get the source database value from the Excel file's basename (without extension)
        source_database_value = os.path.basename(excel_file).split('.')[0]

        for sheet_name in sheet_names:
            # Load the data into a DataFrame
            df = excel_data.parse(sheet_name)

            # Remove unwanted columns (e.g., unnamed columns or 'ID')
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            if 'ID' in df.columns:
                df = df.drop(columns=['ID'])

            for col in df.columns:
                # print(col, df[col].dtype)
                if df[col].dtype == 'datetime64[ns]':
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            # Identify non-empty columns (columns with at least one non-NaN value)
            non_empty_columns = [col for col in df.columns if df[col].notna().any()]

            # Include the 'source_database' column (this column is not dependent on the DataFrame)
            non_empty_columns.append('source_database')

            # Prepare column names and placeholders for SQL based on non-empty columns
            columns = non_empty_columns
            placeholders = ", ".join(["%s"] * len(non_empty_columns))

            # Insert query to be executed
            insert_query = f"""
                INSERT INTO `{sheet_name}` ({', '.join(columns)})
                VALUES ({placeholders})
            """
            print(insert_query)

            # Convert DataFrame to list of tuples for insertion, appending the source_database value
            data_tuples = []
            for row in df[non_empty_columns[:-1]].itertuples(index=False):
                row_data = tuple(
                    None if pd.isna(val) else val for val in row
                ) + (source_database_value,)  # Append the source_database value
                data_tuples.append(row_data)

            # Execute the insertion for each row
            for row in data_tuples:
                cursor.execute(insert_query, row)

            print(f"Data from sheet '{sheet_name}' successfully loaded into '{sheet_name}'.")

        # Commit the changes to the database
        connection.commit()

    except Error as e:
        print(excel_file, sheet_name)
        print(f"Error: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

## Handle Save

def save_management_xml(man_ids,  db_config):
    try:
        # Connect to the database
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)

        # Select management rows
        format_strings = ','.join(['%s'] * len(man_ids))
        query = f"SELECT * FROM management WHERE ID IN ({format_strings})"
        cursor.execute(query, man_ids)
        man_rows = cursor.fetchall()
        if not man_rows:
            raise ValueError(f"No data found in management for IDs: {man_ids}")

        # Select Date Application rows
        ### Here, build the XML tree from man_rows x date_app_rows
        man_useful_cols = get_columns_to_check(cursor, 'management', None,
                                               ['ID', 'project_ID', 'file_name', 'dir_path', 'created_at', 'updated_at',
                                                'ManagementItem', 'name'])
        root = ET.Element("ManagementFile", xmlns)
        items_array = ET.SubElement(root, "ItemsArray")
        print(man_rows)
        print(man_useful_cols)

        for row in man_rows:
            ManagementItem = ET.SubElement(items_array, "ManagementItem", {"name": row['ManagementItem']})
            curr_id = str(row['ID'])
            for col, value in row.items():
                if pd.notna(value) and col in man_useful_cols:  # Ignore 'ManagementItem' column for element name
                    # If the value is a date, format it correctly
                    if isinstance(value, (datetime, str)) and value:
                        try:
                            # Try converting value to datetime if it's a string (in case it's not already)
                            if isinstance(value, str):
                                value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            # Convert to the desired format
                            value = value.strftime('%Y-%m-%dT%H:%M:%S')
                        except Exception as e:
                            print(f"Error formatting date value {value}: {e}")
                    # If the value is a float and has no decimal part, convert to int
                    elif isinstance(value, float) and value.is_integer():
                        value = int(value)
                    # Dynamically create an element for each column with a non-None value
                    ET.SubElement(ManagementItem, col).text = str(value)

            # Query for date applications
            query = f"SELECT * FROM date_application WHERE man_ID = {curr_id}"
            print(query)
            cursor.execute(query)
            date_app = cursor.fetchall()
            if not date_app:
                print(f"No date_app for ID: {curr_id}, name {row['ManagementItem']}")
            else:
                date_useful_col = get_columns_to_check(cursor, 'date_application', None,
                                                       ['ID', 'man_ID', 'created_at', 'updated_at'])
                DateApplications = ET.SubElement(ManagementItem, 'DateApplications')
                for drow in date_app:
                    DateApplication = ET.SubElement(DateApplications, 'DateApplication')
                    for col, value in drow.items():
                        if pd.notna(value) and col in date_useful_col:
                            # If the value is a date, format it correctly
                            if isinstance(value, (datetime, str)) and value:
                                try:
                                    # Try converting value to datetime if it's a string (in case it's not already)
                                    if isinstance(value, str):
                                        value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                                    # Convert to the desired format
                                    value = value.strftime('%Y-%m-%dT%H:%M:%S')
                                except Exception as e:
                                    print(f"Error formatting date value {value}: {e}")
                            # If the value is a float and has no decimal part, convert to int
                            elif isinstance(value, float) and value.is_integer():
                                value = int(value)
                            # Dynamically create an element for each column with a non-None value
                            ET.SubElement(DateApplication, col).text = str(value)

        # Convert the tree to a string and pretty-print it
        xml_str = ET.tostring(root, encoding="unicode", method="xml")
        pretty_xml_str = pretty_print_xml(xml_str)
        print(pretty_xml_str)

    except mysql.connector.Error as err:
        print("in save_management_xml")
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        return pretty_xml_str

def save_site_xml(site_ids, db_config):
    try:
        # Connect to the database
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)

        # Select management rows
        format_strings = ','.join(['%s'] * len(site_ids))
        query = f"SELECT * FROM site WHERE ID IN ({format_strings})"
        cursor.execute(query, site_ids)
        site_rows = cursor.fetchall()
        if not site_rows:
            raise ValueError(f"No data found in management for IDs: {site_ids}")

        # Select Date Application rows
        ### Here, build the XML tree from man_rows x date_app_rows
        site_useful_cols = get_columns_to_check(cursor, 'site', None,
                                               ['ID', 'project_ID', 'file_name', 'dir_path', 'created_at', 'updated_at',
                                                'SiteItem', 'name', 'Site_format', 'WeatherFile'])
        root = ET.Element("SiteFile", xmlns)
        items_array = ET.SubElement(root, "ItemsArray")
        print(site_rows)
        print(site_useful_cols)

        for row in site_rows:
            SiteItem = ET.SubElement(items_array, "SiteItem", {"name": row['SiteItem'], 'format': row['Site_format']})
            curr_id = str(row['ID'])
            for col, value in row.items():
                if pd.notna(value) and col in site_useful_cols:  # Ignore 'ManagementItem' column for element name
                    # If the value is a date, format it correctly
                    if isinstance(value, (datetime, str)) and value:
                        try:
                            # Try converting value to datetime if it's a string (in case it's not already)
                            if isinstance(value, str):
                                value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            # Convert to the desired format
                            value = value.strftime('%Y-%m-%dT%H:%M:%S')
                        except Exception as e:
                            print(f"Error formatting date value {value}: {e}")
                    # If the value is a float and has no decimal part, convert to int
                    elif isinstance(value, float) and value.is_integer():
                        value = int(value)
                    # Dynamically create an element for each column with a non-None value
                    ET.SubElement(SiteItem, col).text = str(value)
            WeatherFiles = ET.SubElement(SiteItem, 'WeatherFiles')
            ET.SubElement(WeatherFiles, 'WeatherFile', {'file': row['WeatherFile']})

        # Convert the tree to a string and pretty-print it
        xml_str = ET.tostring(root, encoding="unicode", method="xml")
        print(xml_str)
        pretty_xml_str = pretty_print_xml(xml_str)
        print(pretty_xml_str)

    except mysql.connector.Error as err:
        print("in save_management_xml")
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        return pretty_xml_str

def save_soil_xml(soil_ids,  db_config):
    try:
        # Connect to the database
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)

        # Select soil  rows
        format_strings = ','.join(['%s'] * len(soil_ids))
        query = f"SELECT * FROM soil WHERE ID IN ({format_strings})"
        cursor.execute(query, soil_ids)
        soil_rows = cursor.fetchall()
        if not soil_rows:
            raise ValueError(f"No data found in management for IDs: {soil_ids}")


        ### Here, build the XML tree
        soil_useful_cols = get_columns_to_check(cursor, 'soil', None,
                                               ['ID', 'project_ID', 'file_name', 'dir_path', 'created_at', 'updated_at',
                                                'SoilItem', 'name'])
        root = ET.Element("SoilFile", xmlns)
        items_array = ET.SubElement(root, "ItemsArray")
        print(soil_rows)
        print(soil_useful_cols)

        for row in soil_rows:
            SoilItem = ET.SubElement(items_array, "SoilItem", {"name": row['SoilItem']})
            curr_id = str(row['ID'])
            for col, value in row.items():
                if pd.notna(value) and col in soil_useful_cols:
                    # If the value is a date, format it correctly
                    if isinstance(value, (datetime, str)) and value:
                        try:
                            # Try converting value to datetime if it's a string (in case it's not already)
                            if isinstance(value, str):
                                value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            # Convert to the desired format
                            value = value.strftime('%Y-%m-%dT%H:%M:%S')
                        except Exception as e:
                            print(f"Error formatting date value {value}: {e}")
                    # If the value is a float and has no decimal part, convert to int
                    elif isinstance(value, float) and value.is_integer():
                        value = int(value)
                    # Dynamically create an element for each column with a non-None value
                    ET.SubElement(SoilItem, col).text = str(value)

            # Query for date applications
            query = f"SELECT * FROM soil_layer WHERE soil_ID = {curr_id}"
            print(query)
            cursor.execute(query)
            soil_layer = cursor.fetchall()
            if not soil_layer:
                print(f"No soil_layer for ID: {curr_id}, name {row['SoilItem']}")
            else:
                layers_useful_col = get_columns_to_check(cursor, 'soil_layer', None,
                                                       ['ID', 'soil_ID', 'created_at', 'updated_at'])
                LayersArray = ET.SubElement(SoilItem, 'LayersArray')
                for drow in soil_layer:
                    SoilLayer = ET.SubElement(LayersArray, 'SoilLayer')
                    for col, value in drow.items():
                        if pd.notna(value) and col in layers_useful_col:
                            # If the value is a date, format it correctly
                            if isinstance(value, (datetime, str)) and value:
                                try:
                                    # Try converting value to datetime if it's a string (in case it's not already)
                                    if isinstance(value, str):
                                        value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                                    # Convert to the desired format
                                    value = value.strftime('%Y-%m-%dT%H:%M:%S')
                                except Exception as e:
                                    print(f"Error formatting date value {value}: {e}")
                            # If the value is a float and has no decimal part, convert to int
                            elif isinstance(value, float) and value.is_integer():
                                value = int(value)
                            # Dynamically create an element for each column with a non-None value
                            ET.SubElement(SoilLayer, col).text = str(value)

        # Convert the tree to a string and pretty-print it
        xml_str = ET.tostring(root, encoding="unicode", method="xml")
        pretty_xml_str = pretty_print_xml(xml_str)
        print(pretty_xml_str)


    except mysql.connector.Error as err:
        print("in save_soil_xml: ")
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        return pretty_xml_str

def save_parameters_xml(parameters_ids,  db_config, type):
    try:
        # Connect to the database
        db_connection = mysql.connector.connect(**db_config)
        cursor = db_connection.cursor(dictionary=True)
        parameters_useful_col = get_columns_to_check(cursor, 'parameters', None,
                                                     ['ID', 'type', 'created_at', 'updated_at', 'model_version',
                                                      'dir_path', 'file_name', 'name'])

        # For varietal parameters
        format_strings = ','.join(['%s'] * len(parameters_ids))
        query = f"SELECT * FROM parameters WHERE ID IN ({format_strings}) and type = '{type}'"
        cursor.execute(query, parameters_ids)
        var_parameters_rows = cursor.fetchall()
        if not var_parameters_rows:
            print(f"No varietal parameters found for  {parameters_ids}")
        else:
            print("varietal",len(var_parameters_rows))
            root = ET.Element("MaizeVarietyFile", xmlns)
            items_array = ET.SubElement(root, "ItemsArray")
            for row in var_parameters_rows:
                CropParameterItem = ET.SubElement(items_array, "CropParameterItem", attrib ={"name": row['name']})
                ParamValue = ET.SubElement(CropParameterItem, "ParamValue")
                for col,value in row.items():
                    if pd.notna(value) and col in parameters_useful_col:
                        print(col,value)
                        item = ET.SubElement(ParamValue, "Item")

                        Key = ET.SubElement(item,'Key')
                        ET.SubElement(Key,'string').text = col

                        val = ET.SubElement(item, 'Value')
                        ET.SubElement(val, 'double').text = str(value)
                        print(ET.tostring(item, encoding="unicode", method="xml"))

            xml_str = ET.tostring(root, encoding="unicode", method="xml")
            pretty_xml_str = pretty_print_xml(xml_str)
            print(pretty_xml_str)
            return pretty_xml_str
        # For non-varietal parameters
        format_strings = ','.join(['%s'] * len(parameters_ids))
        query2 = f"SELECT * FROM parameters WHERE ID IN ({format_strings}) and type = 'non-varietal'"

        cursor.execute(query2, parameters_ids)
        nonvar_parameters_rows = cursor.fetchall()
        if not nonvar_parameters_rows:
            print(f"No varietal parameters found for  {parameters_ids}")
        else:
            print('Non varietal',len(nonvar_parameters_rows))

    except mysql.connector.Error as err:
        print("in save_soil_xml: ")
        print(f"Error: {err}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        return pretty_xml_str

#### Handle Duplicate

def get_foreign_keys(cursor, table_name):
    """
    Retrieve foreign key relationships for the specified table.

    Parameters:
    cursor (mysql.connector.cursor): Database cursor.
    table_name (str): Name of the table to fetch foreign key information for.

    Returns:
    list: List of foreign key relationships with table name and column name.
    """
    query = f"""
    SELECT kcu.TABLE_NAME AS table_name, kcu.COLUMN_NAME AS column_name
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    WHERE rc.REFERENCED_TABLE_NAME = '{table_name}'
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_columns_to_check(cursor, table_name, foreign_keys, exclude_columns = None):
    """
    Retrieve the columns to check for duplicates in a table, excluding foreign key columns
    and ID, created_at, and updated_at columns.

    Parameters:
    cursor (mysql.connector.cursor): Database cursor.
    table_name (str): Name of the table to fetch column information for.
    foreign_keys (list): List of foreign keys for this table.

    Returns:
    list: Columns to check for duplicates.
    """
    query = f"SHOW COLUMNS FROM {table_name}"
    cursor.execute(query)
    columns = cursor.fetchall()
    # print(columns)
    # Extract the column names, excluding the ones we don't need to check
    if not exclude_columns :
        exclude_columns = {'ID', 'created_at', 'updated_at'}
    if foreign_keys:
        exclude_columns.update([fk['column_name'] for fk in foreign_keys])  # Exclude foreign keys

    columns_to_check = [
        col['Field'] for col in columns if col['Field'] not in exclude_columns
    ]

    return columns_to_check

def get_values_of_row_by_id(cursor, table_name, project_id, columns_to_check):
    """
    Retrieve the values of the newly inserted project for the specified columns.
    """
    query = f"""
    SELECT {', '.join(columns_to_check)}
    FROM {table_name}
    WHERE ID = %s
    """
    cursor.execute(query, (project_id,))
    return cursor.fetchone()  # Fetch values for the columns

def find_duplicates(cursor, table_name, columns_to_check, values):
    """
    Find duplicate rows in the table that match the specified values.
    This handles None, 'NA', or similar placeholders by converting them to SQL NULL equivalents.
    """
    # Replace None, 'NA', 'Null', and 'None' with a special marker
    values = [None if v in [None, 'NA', 'Null', 'None'] else v for v in values]

    # Build the conditions for the query, using "IS NULL" for None values
    conditions = " AND ".join([
        f"{col} IS %s" if v is None else f"{col} = %s"
        for col, v in zip(columns_to_check, values)
    ])

    # Construct the query
    query = f"""
    SELECT ID, created_at
    FROM {table_name}
    WHERE {conditions}
    ORDER BY created_at ASC
    """

    # Debug: Print the query and values to check for correctness
    # print(f"Find Duplicates Query: {query}")
    # print(f"Values: {values}")

    # Execute the query with unpacked values
    cursor.execute(query, tuple(v if v is not None else None for v in values))  # Pass the values as a tuple

    # Fetch all duplicates
    result = cursor.fetchall()

    # Debug: Check the result
    # print(f"Duplicate Results: {result}")

    return result

def prepare_ids_to_delete(duplicates, project_id):
    """
    Determine which rows to delete, keeping the oldest row and the newly inserted project row.
    """
    ids_to_keep = {project_id}  # Include project_ID as part of the rows to keep
    duplicates = sorted(duplicates, key=lambda x: x['created_at'])  # Sort by `created_at`
    return [row['ID'] for row in duplicates if row['ID'] not in ids_to_keep]

### Other functions prettifier like

def pretty_print_xml(xml_str):
    # Parse and pretty print the XML string
    reparsed = minidom.parseString(xml_str)
    return reparsed.toprettyxml(indent="  ")

def sanitize(value):
    return None if value in ('None', '?', None) else value

def convert_to_sql_date_format(df, daily_dict, summary_dict, mapping_dict):
    """
    Convert specified columns in the DataFrame to SQL-readable date format (YYYY-MM-DD).
    """
    for col in df.columns:
        if col in mapping_dict:
            real_col = mapping_dict[col]

            # Check if the real column should be converted to a date
            if (real_col in daily_dict and daily_dict[real_col] == 'yyyy-mm-dd') or 'DAT' in real_col:
                # print(f"Converting column '{col}' ({real_col}) to datetime")
                # Convert to datetime and handle invalid date entries by setting them to NaT
                df[col] = pd.to_datetime(df[col], errors='coerce')

                # Replace NaT with None (for SQL NULL compatibility)
                df[col] = df[col].where(df[col].notna(), None)

                # Format the date in YYYY-MM-DD format for SQL compatibility
                df[col] = df[col].dt.strftime('%Y-%m-%d').where(df[col].notna(), None)

            elif real_col in summary_dict and summary_dict[real_col] == 'yyyy-mm-dd':
                # print(f"Converting column '{col}' ({real_col}) to datetime")
                df[col] = pd.to_datetime(df[col], errors='coerce')
                # Replace NaT with None (for SQL NULL compatibility)
                df[col] = df[col].where(df[col].notna(), None)

                # Format the date in YYYY-MM-DD format for SQL compatibility
                df[col] = df[col].dt.strftime('%Y-%m-%d').where(df[col].notna(), None)
        else:
            print(f"Column '{col}' is not in mapping_dict")
            if len(df[col].notna()) >0:
                print(df[col])

    return df
import xml.etree.ElementTree as ET
import os
import pandas as pd


class InputFileSQ:
    def __init__(self, model_version, input_dir_path, name, extension):
        self.input_dir_path = input_dir_path
        self.xml_file_path = os.path.join(input_dir_path, name + extension)  # Ensure correct file path
        self.name = name
        self.model_version = model_version
        self.xml_tree = self.load_xml()

        # Extract base directory name and split parts
        base_parent_dir = os.path.basename(os.path.dirname(input_dir_path))
        parts = base_parent_dir.split('_')

        # Validate and assign DB_name, modeller_name, and original_model_version
        if len(parts) >= 3:
            self.DB_name = parts[0]
            self.modeller_name = parts[1]
            self.original_model_version = parts[2]
        else:
            raise ValueError(
                "Directory name format is incorrect; expected 'DB_modeller_originalSQversion'. "
                f"Got '{base_parent_dir}' with parts: {parts}"
            )

    def load_xml(self):
        """Load the XML file, with error handling for file-not-found or invalid XML formats."""
        try:
            # Attempt to parse the XML file
            tree = ET.parse(self.xml_file_path)
            return tree
        except FileNotFoundError:
            raise FileNotFoundError(f"XML file not found at path: {self.xml_file_path}")
        except ET.ParseError:
            raise ET.ParseError(f"Failed to parse XML file at path: {self.xml_file_path}. "
                                "The file may be improperly formatted.")

    def rewrite_xml(self, output_file):
        """Rewrite the XML file with a specific declaration format."""
        with open(output_file, 'wb') as f:
            # Manually write the XML declaration without encoding
            f.write(b'<?xml version="1.0"?>\n')
            # Write the rest of the XML tree without specifying encoding
            self.xml_tree.write(f, encoding='utf-8', xml_declaration=False)

class Project(InputFileSQ):
    def __init__(self, model_version, input_dir_path, name, extension = '.sqpro'):
        # Initialize the parent class (InputFileSQ)
        super().__init__(model_version, input_dir_path, name, extension)
        self.files = self.parse_xml()  # Call to parse_xml() after initializing parent class

    def parse_xml(self):
        """Parse XML data for Project, with error handling for missing elements."""
        try:
            root = self.xml_tree.getroot()
            inputs_element = root.find('Inputs')

            if inputs_element is None:
                raise ValueError(f"Missing 'Inputs' element in the XML file: {self.xml_file_path}")

            # Create a dictionary for each child element of `Inputs`
            self.files = {child.tag: child.text for child in inputs_element if child.text != '?'}
            return self.files
        except AttributeError:
            raise AttributeError(f"Failed to parse XML structure in file: {self.xml_file_path}. "
                                 "Ensure the XML format matches the expected schema.")
        except Exception as e:
            raise RuntimeError(f"An error occurred while parsing the XML in Project: {e}")

    def execute(self):
        """Run the project logic."""
        print("Executing project with the following inputs:")
        self.display_inputs()

    def display_inputs(self):
        """Display parsed inputs."""
        for key, value in self.files.items():
            print(f"{key}: {value}")

    def load_normalized(self, list_to_load, root_dir):
        for i in list_to_load:
            extension = self.files[i].split('.')[-1]  # Get the file extension from self.files[i]

            # Search through root_dir for a file that matches the extension
            for file in os.listdir(root_dir):
                if file.endswith(extension):
                    # Set self.files[i] to the absolute path of the found file
                    self.files[i] = f"..\\..\\{file}"
                    break


    def save_xml(self, output_file):
        """Save the Project XML file with the current values in `self.files`."""
        # Create the root element
        root = ET.Element('ProjectFile')

        # Create the Inputs element
        inputs_elem = ET.SubElement(root, 'Inputs')

        # Add each item in `self.files` as a child element of Inputs
        for key, value in self.files.items():
            child_elem = ET.SubElement(inputs_elem, key)
            child_elem.text = str(value)

        self.xml_tree = ET.ElementTree(root)
        # Create an ElementTree from the root and write to the file
        self.rewrite_xml(output_file)

class Run(InputFileSQ):
    def __init__(self, model_version, input_dir_path, name, extension = '.sqrun'):
        # Initialize the parent class (InputFileSQ)
        super().__init__(model_version, input_dir_path, name, extension)
        self.runs = self.parse_xml()  # Initialize runs attribute by parsing XML

    def parse_xml(self):
        """Parse XML data for Run, with error handling for missing or malformed elements."""
        try:
            root = self.xml_tree.getroot()
            runs_data = {}

            # Look for RunItem elements
            run_items = root.findall('.//RunItem')
            if not run_items:
                raise ValueError(f"No 'RunItem' elements found in the XML file: {self.xml_file_path}")

            for run_item in run_items:
                run_name = run_item.get('name')
                if run_name is None:
                    raise ValueError(f"RunItem missing 'name' attribute in XML file: {self.xml_file_path}")

                run_values = []

                # Process each MultiRunItem under the Multi tag
                multi_element = run_item.find('Multi')
                if multi_element is not None:
                    for multiItem in multi_element.findall('.//MultiRunItem'):
                        value = {gchild.tag: gchild.text for gchild in multiItem if gchild.text != '?'}
                        run_values.append(value)
                else:
                    raise ValueError(f"RunItem '{run_name}' is missing the 'Multi' element in XML file: {self.xml_file_path}")

                # Convert list of dictionaries to a DataFrame and store in runs_data
                runs_data[run_name] = pd.DataFrame(run_values)

            return runs_data
        except AttributeError:
            raise AttributeError(f"Failed to parse XML structure in file: {self.xml_file_path}. "
                                 "Ensure the XML format matches the expected schema.")
        except Exception as e:
            raise RuntimeError(f"An error occurred while parsing the XML in Run: {e}")

    def execute(self):
        """Run the project logic."""
        print("Executing run with the following inputs:")
        self.display_inputs()


    def display_inputs(self):
        """Display parsed inputs."""
        for key, value in self.runs.items():
            print(f"{key}: {value}")

    def split_RUN_all_by_site(self):

        keys_to_remove = [key for key in self.runs if key != 'RUN_all']

        # Remove all keys except 'name'
        for key in keys_to_remove:
            self.runs.pop(key)
        """Group runs by SiteItem and create separate DataFrames for each group."""
        grouped_runs = {}

        for run_name, df in self.runs.items():
            # Group by ManagementItem
            for site_item, group_df in df.groupby('SiteItem'):
                # Create a new RunItem for each ManagementItem
                grouped_runs[f"{site_item}"] = group_df.reset_index(drop=True)
        self.runs = grouped_runs
        return grouped_runs  # Return the grouped runs

    def create_run_file_element(self, output_run_dir, output_format_item=None):
        """
        Create the root RunFile element with optional modification of OutputFormatItem.

        Args:
            output_run_dir (str): Directory path for <RunFile> structure.
            output_format_item (str, optional): Value to set for all <OutputFormatItem>.

        Returns:
            xml.etree.ElementTree.Element: The root element for the XML structure.
        """
        # Create the root RunFile element
        run_file_elem = ET.Element('RunFile')

        # Create ItemsArray element
        items_array_elem = ET.SubElement(run_file_elem, 'ItemsArray')

        # Loop through self.runs to create RunItems
        for run_name, df in self.runs.items():
            run_item_elem = ET.SubElement(items_array_elem, 'RunItem', attrib={'name': run_name})

            # Create Multi element
            multi_elem = ET.SubElement(run_item_elem, 'Multi')

            # Add OutputDirectory and OutputPattern
            output_directory_elem = ET.SubElement(multi_elem, 'OutputDirectory')
            output_directory_elem.text = output_run_dir  # Adjust as needed
            if output_format_item:
                summary_name =  output_format_item + "_" + run_name + '.summ'
            else:
                summary_name = run_name
            output_pattern_elem = ET.SubElement(multi_elem, 'OutputPattern')
            output_pattern_elem.text = summary_name  # Adjust as needed

            # Modify or add <OutputFormatItem> for each RunItem if requested
            if output_format_item:
                output_format_elements = multi_elem.findall('../OutputFormatItem')
                if output_format_elements:
                    for elem in output_format_elements:
                        elem.text = output_format_item
                else:
                    # Create <OutputFormatItem> if none exist
                    output_format_elem = ET.SubElement(multi_elem, 'OutputFormatItem')
                    output_format_elem.text = output_format_item

            # Create MultiRunsArray element
            multi_runs_array_elem = ET.SubElement(multi_elem, 'MultiRunsArray')

            # Iterate over DataFrame rows and create MultiRunItem elements
            for _, row in df.iterrows():
                multi_run_item_elem = ET.SubElement(multi_runs_array_elem, 'MultiRunItem')

                # For each row, create sub-elements with the tag as the column and the text as the value
                for col, value in row.items():
                    sub_elem = ET.SubElement(multi_run_item_elem, col)
                    sub_elem.text = str(value)
                if output_format_item:
                    output_format_elements = multi_run_item_elem.findall('OutputFormatItem')
                    if output_format_elements:
                        for elem in output_format_elements:
                            elem.text = output_format_item
                    else:
                        # Create <OutputFormatItem> if none exist
                        output_format_elem = ET.SubElement(multi_elem, 'OutputFormatItem')
                        output_format_elem.text = output_format_item

                    export_normal = multi_run_item_elem.findall('ExportNormalRuns')
                    if export_normal:
                        for elem in export_normal:
                            elem.text = 'false'
                    else:
                        # Create <OutputFormatItem> if none exist
                        export_normal = ET.SubElement(multi_elem, 'ExportNormalRuns')
                        export_normal.text = 'false'

            # Add DailyOutputPattern
            if output_format_item:
                daily_output_pattern_elem = ET.SubElement(multi_elem, 'DailyOutputPattern')
                daily_output_pattern_elem.text = "Default_$(VarietyName)_$(ManagementName).daily"
            else:
                daily_output_pattern_elem = ET.SubElement(multi_elem, 'DailyOutputPattern')
                daily_output_pattern_elem.text = "$(VarietyName)_$(ManagementName)"


        return run_file_elem

    def update_xml(self, output_run_dir, output_format_item=None):
        """Save the newly created XML structure."""
        # Create the new root element
        root = self.create_run_file_element(output_run_dir,output_format_item)

        # Modify or add the <OutputFormatItem> if requested

        self.xml_tree = ET.ElementTree(root)

    def save_xml(self, output_file, output_run_dir, output_format_item=None):

        # Create an ElementTree object
        self.update_xml(output_run_dir,output_format_item)

        self.rewrite_xml(output_file)

    def create_filtered_run_file_element(self, filtered_runs, output_run_dir):
        """
        Create an XML structure with filtered MultiRunItem elements.

        Args:
            filtered_runs (dict): Filtered runs from extract_multirunitem_by_variety.
            output_run_dir (str): Output directory for the XML structure.

        Returns:
            xml.etree.ElementTree.Element: The root element for the filtered XML structure.
        """
        run_file_elem = ET.Element('RunFile')
        items_array_elem = ET.SubElement(run_file_elem, 'ItemsArray')

        for run_name, df in filtered_runs.items():
            run_item_elem = ET.SubElement(items_array_elem, 'RunItem', attrib={'name': run_name})

            multi_elem = ET.SubElement(run_item_elem, 'Multi')

            output_directory_elem = ET.SubElement(multi_elem, 'OutputDirectory')
            output_directory_elem.text = output_run_dir

            output_pattern_elem = ET.SubElement(multi_elem, 'OutputPattern')
            output_pattern_elem.text = run_name

            multi_runs_array_elem = ET.SubElement(multi_elem, 'MultiRunsArray')
            for _, row in df.iterrows():
                multi_run_item_elem = ET.SubElement(multi_runs_array_elem, 'MultiRunItem')
                for col, value in row.items():
                    sub_elem = ET.SubElement(multi_run_item_elem, col)
                    sub_elem.text = str(value)

        return run_file_elem

    def extract_multirunitem_by_varieties(self, variety_values):
        """
        Extract specific MultiRunItem elements based on a list of VarietyItem values.

        Args:
            variety_values (list): A list of VarietyItem values to filter by.

        Returns:
            dict: A dictionary with RunItem names as keys and filtered DataFrames as values.
        """
        filtered_runs = {}

        # Iterate over each RunItem and its DataFrame
        for run_name, df in self.runs.items():
            # Filter rows where VarietyItem matches any value in the given list
            filtered_df = df[df['VarietyItem'].isin(variety_values)]

            # Add the filtered DataFrame to the result if it has any rows
            if not filtered_df.empty:
                filtered_runs[run_name] = filtered_df.reset_index(drop=True)

        return filtered_runs


class Variety(InputFileSQ):
    def __init__(self, model_version, input_dir_path, name, extension = '.sqvarm'):
        super().__init__(model_version, input_dir_path, name, extension)
        self.parameters = self.parse_xml()  # Initialize runs attribute by parsing XML

    def parse_xml(self):
        root = self.load_xml()  # Load the XML data
        param_list = []  # List to hold dictionaries for each CropParameterItem

        for crop_param_item in root.findall('.//CropParameterItem'):
            run_values = {}  # Dictionary to hold parameters for the current CropParameterItem
            run_name = crop_param_item.get('name')  # Get the name attribute

            # Iterate over the ParamValue to collect parameter keys and values
            for param_value in crop_param_item.findall('.//ParamValue'):
                for item in param_value.findall('.//Item'):
                    key = item.find('./Key/string').text  # Extract the parameter key
                    value = item.find('./Value/double').text  # Extract the parameter value
                    run_values[key] = float(value) if value is not None else None  # Store in the dictionary

            # Add the run_name as a column to the parameter dictionary
            run_values['CropParameterItem'] = run_name

            param_list.append(run_values)  # Append the parameter dictionary to the list

        # Convert the list of dictionaries into a DataFrame
        param_df = pd.DataFrame(param_list)

        return param_df


    def save_xml(self, output_path):
        """
        Save the Variety DataFrame back to an XML file in the original structure.

        Args:
            output_path (str): The path to save the XML file.
        """
        # Create the root element
        root = ET.Element("MaizeVarietyFile",
                          attrib={"xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                                  "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance"})

        # Create the ItemsArray element
        items_array = ET.SubElement(root, "ItemsArray")

        # Iterate over each row in the DataFrame
        for _, row in self.parameters.iterrows():
            # Create a CropParameterItem element
            crop_param_item = ET.SubElement(items_array, "CropParameterItem", name=row["CropParameterItem"])

            # Optionally, add comments (modify as needed)
            comments = ET.SubElement(crop_param_item, "Comments")
            comments.text = "Parameter details for variety {}".format(row["CropParameterItem"])

            # Create the ParamValue element
            param_value = ET.SubElement(crop_param_item, "ParamValue")

            # Iterate over all columns except 'CropParameterItem'
            for key, value in row.items():
                if key == "CropParameterItem":
                    continue

                # Create the Item element
                item = ET.SubElement(param_value, "Item")

                # Add the Key element
                key_element = ET.SubElement(item, "Key")
                key_string = ET.SubElement(key_element, "string")
                key_string.text = key

                # Add the Value element
                value_element = ET.SubElement(item, "Value")
                value_double = ET.SubElement(value_element, "double")
                value_double.text = str(value) if pd.notna(value) else "0"  # Handle NaN values

        # Convert the XML tree to a string and save it
        tree = ET.ElementTree(root)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)
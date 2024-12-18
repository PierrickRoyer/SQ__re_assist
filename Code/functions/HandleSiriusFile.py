from Code.Import import *
from Code.Constant import *
from Code.classes.InputFileSQ import *
import mysql.connector
import xml.etree.ElementTree as ET

    ### OUTPUT
def concatenate_sirius_summary(input_files, output_file):
    header = []
    column_headers = []
    data_lines = []
    first_file = True

    # Loop through each file in the input files list
    for file_path in input_files:
        with open(file_path, 'r') as file:
            lines = file.readlines()

            # For the first file, store the header and column definitions
            if first_file:
                header = lines[:15]
                column_headers = lines[15:17]
                first_file = False

            # Append only the data lines (after line 17)
            data_lines.extend(lines[17:])

    # Write everything to the output file
    with open(output_file, 'w') as output:
        output.writelines(header)  # Write the header once
        output.writelines(column_headers)  # Write column headers once
        output.writelines(data_lines)  # Write all concatenated data

def iterate_over_project_output(root_dir):
    model_version = os.path.basename(root_dir)
    output = defaultdict(dict)

    for base, dirs, files in os.walk(root_dir):
        # Check if we're in a `3-Output` directory
        if os.path.basename(os.path.dirname(base)) == "3-Output":
            run_name = os.path.basename(base)
            print(run_name)
            output[run_name] = {
                'Summary': None,  # Initialize Summary key for each run
                'Daily': []  # Initialize Daily key as a list
            }
            # Gather all `.sqbrs` files in this directory
            sqbrs_files = [os.path.join(base, file) for file in files if file.endswith('.sqbrs')]
            default_summ_files = [os.path.join(base, file) for file in files if file.endswith('.summ.txt')]
            if sqbrs_files and f"{run_name}_summary.sqbrs" not in files:
                # Define the path for the concatenated output file
                concatenated_file_path = os.path.join(base, f"{run_name}_summary.sqbrs")

                # Run the concatenation function
                concatenate_sirius_summary(sqbrs_files, concatenated_file_path)

            if sqbrs_files:

                # Create a single SummaryOutput object with the concatenated file
                summ = SummaryOutput(model_version, base, f"{run_name}_summary", '.sqbrs')
                output[run_name]['Summary'] = summ

            # Process `.sqsro` files as DailyOutput objects
            for file in files:
                if file.endswith('.sqsro'):
                    name = file.replace('.sqsro', '')
                    day = DailyOutput(model_version, base, name, '.sqsro')
                    default = open(os.path.join(base, f'Default_{name}.daily.txt')).readlines()

                    # Extract column names (assumes the 6th line contains column headers)
                    columns = default[8].strip().split('\t')

                    # Extract data (assumes data starts from the 8th line)

                    data = [line.strip().split('\t') for line in default[10:] if line.startswith('SQ')]

                    # Create DataFrame
                    comple_default = pd.DataFrame(data=data, columns=columns)
                    day.data = day.data.merge(comple_default, left_on = 'DATE', right_on = 'Date')
                    # Display the DataFrame
                   #print(day.data[['LeafNumber','LNlig','LNfullyexp']])

                    output[run_name]['Daily'].append(day)

    return output

def load_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read().split('\n\n')  # Split into parts by double newline

        if len(content) < 3:
            raise ValueError("File format is incorrect; expected three sections separated by double newlines.")

        # Assign each part to the corresponding key in the dictionary
        header = content[0],
        parse_meta(content[1])
        summ_data =  parse_data(content[2])  # Process data into a DataFrame

def parse_data(self, data_section):
    # Split data section by lines and use the second line as column headers
    lines = data_section.strip().split('\n')

    # The first line might be a description or ignored, use the second line as column headers
    column_headers = lines[1].split('\t')
    data_rows = [line.split('\t') for line in lines[2:]]  # Data rows start after the column headers

    # Create DataFrame
    df = pd.DataFrame(data_rows, columns=column_headers)

    return df

def parse_meta( data):
    # Split each line, remove ":" from keys, and create dictionary entries from key-value pairs
    meta = {line.split('\t', 1)[0].replace(':', '').strip(): line.split('\t', 1)[1].strip()
                 for line in data.strip().split('\n')}
    return meta





    ##INPUT
def iterate_over_project_input(root_dir):

    model_version = os.path.basename(root_dir)
    projects = list()
    varieties = list()
    runs = list()
    # Traverse through the main directory and subdirectories
    for base, dirs, files in os.walk(root_dir):
        # Check if we're in a `1-Project` directory
        if os.path.basename(base) == "1-Project":
            # Extract the parent directory name to get DatabaseName, ModellerName, and version
            parent_dir_name = os.path.basename(os.path.dirname(base))
            print(parent_dir_name)

            # Initialize empty dictionaries for each project type found


            # Find the `.sqpro`, `.sqvar`, and `.sqrun` files in this directory
            for file in files:
                file_path = os.path.join(base, file)

                if file.endswith('.sqpro'):
                    print(file_path)
                    # Initialize a Project object
                    project = Project(model_version, base, file.replace('.sqpro', ''), '.sqpro')

                    projects.append(project)

                elif file.endswith('.sqvarm'):
                    # Initialize a Variety object
                    variety = Variety(model_version, base, file.replace('.sqvarm', ''), '.sqvarm')
                    varieties.append(variety)

                elif file.endswith('.sqrun'):
                    # Initialize a Run object
                    run = Run(model_version, base, file.replace('.sqrun', ''), '.sqrun')
                    runs.append(run)

    return  projects, varieties, runs


def sync_files_A_and_B(file_A, file_B, output_file_B, log_file_path,targetVersion):
    # Read parameters from both files
    params_A, _ = read_genotype_parameters(file_A)
    params_B, tree_B = read_genotype_parameters(file_B)

    # Remove parameters from B that are not in A and track changes
    modified_tree_B, removed_params = remove_extra_parameters_in_B(tree_B, params_A)

    # Calculate the average values of parameters in A that need to be added to B
    avg_values_to_add = {}
    for param in (set(params_A[next(iter(params_A))].keys()) - set(params_B[next(iter(params_B))].keys())):
        avg_values_to_add[param] = sum(genotype[param] for genotype in params_A.values() if param in genotype) / len(params_A)

    # Add missing parameters to B from A and track changes
    modified_tree_B, added_params = add_missing_parameters_in_B(modified_tree_B, avg_values_to_add)

    # Apply NFinal treatment to B
    if targetVersion =='2.0.0_Nfinal':
        modified_tree_B = treat_NFinal(modified_tree_B)

    os.makedirs(os.path.dirname(output_file_B), exist_ok=True)
    #os.makedirs(os.path.dirname(log_file_path), exist_ok = True)
    # Rewrite the modified XML tree for B to the specified output file
    rewrite_xml(modified_tree_B, output_file_B)

    # Log modifications
    #log_modifications(log_file_path, removed_params, added_params)



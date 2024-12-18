from Code.Import import *

def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
        print(f'{file_path} loaded')
    return data


pathMyOutput = "C:/Users/royerpie/Documents/rootDoc/automate/myOutput/"
pathOldOutput = "C:/Users/royerpie/Documents/rootDoc/automate/outputToReproduce/"
pathSimvsSimOutput = 'C:/Users/royerpie/Documents/rootDoc/results/simVsSimDf/'
pathObs ='C:/Users/royerpie/Documents/rootDoc/automate/observations/'
pathOldPierrickPG = pathOldOutput + '\PG_Pierrick_2.0.0/'
pathParameters =  "C:/Users/royerpie/Documents/rootDoc/automate/parameters/"
pathProjectCross = "C:/Users/royerpie\Documents/rootDoc\Working_Immuable\myProject\cross/"
pathObservationAgmip = "C:/Users/royerpie/Documents/rootDoc/automate/observations/PseudoAGMIP/"

#varJugurta = ["SilkD","ADAT","MDAT","LAIX","LAIA","HnoAM","GWGM"]
# pathPickleByExpe = "C:/Users/royerpie/Documents/rootDoc/automate/script/SQ_Maize_Assistant/data_pickle/by_project"
# pathPickleThrash = "C:/Users/royerpie/Documents/rootDoc/automate/script/SQ_Maize_Assistant/data_pickle/by_project_not_great"
# pathPickle = "C:/Users/royerpie/Documents/rootDoc/automate/script/SQ_Maize_Assistant/data_pickle"


sq_path_git     = 'C:/Users/royerpie/Source/Repos/SiriusCode/Code/SiriusConsole/bin/Debug/SiriusQuality-Console.exe'
sq_path_2_1_2   = "C:/Users/royerpie/Documents/rootDoc/automate/modeles/2.1/2.1.2_code_Loic/Code/SiriusConsole/bin/Debug/SiriusQuality-Console.exe"
sq_path_2_1_1   = "C:/Users/royerpie/Documents/rootDoc/automate/modeles/2.1/2.1.1_code_Loic/Code/SiriusConsole/bin/Debug/SiriusQuality-Console.exe"
sq_path_2_2_dernier_sirius = "C:/Users/royerpie/Documents/rootDoc/automate/modeles/2.2/Dernier_Sirius/Code/SiriusConsole/bin/Debug/SiriusQuality-Console.exe"
path_data          = 'C:/Users/royerpie/Documents/rootDoc/automate/script/sq_re_assist/Data'

xmlns = {
    "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "xmlns:xsd": "http://www.w3.org/2001/XMLSchema"
}
xml_declaration = '<?xml version="1.0"?>'

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'bail!99',
    'database': 'siriusquality12_24'
}


daily_dict      = load_pickle(os.path.join(path_data,"icasa_units_daily_dic.pkl"))
summary_dict    = load_pickle(os.path.join(path_data,"icasa_units_summary_dic.pkl"))
mapping_dict    = load_pickle(os.path.join(path_data,"mapping_forward.pkl"))

print(daily_dict)
print(summary_dict)
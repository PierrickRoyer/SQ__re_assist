from Code.Import import *
from Code import Constant
from Code.classes.InputFileSQ import *
from Code.functions import RunSirius
model_version = '2.1.1'
project_dir = 'C:/Users/royerpie/Documents/rootDoc/Immuable/myProject/reproduce/PG_Jugurta_2.1.1/1-Project'
name_pro = 'PG_1_2.1.1'
# name_var = 'PG_1_2.1.1'
name_run = 'PG_Jugurta'
#
thisRun = Run(model_version,project_dir,name_run)
#
thisRun.split_RUN_all_by_site()
# thisRun.save_xml(os.path.join(project_dir,name_run + 'split_Env.sqrun'),'../3-Output')
# print(thisRun.name)
runList = list(thisRun.runs.keys())
#
# thisPro.files['RunFileName'] = os.path.join(project_dir,name_run + 'split_Env.sqrun')
# thisPro.files['MaizeVarietyFileName'] = thisVar.name
# thisPro.save_xml(os.path.join(project_dir,name_pro + '.sqpro'))
# print(os.path.join(project_dir,name_pro))
SQpath = Constant.sq_path_2_1_1
sqproPath = os.path.join(project_dir,name_pro + '.sqpro')
# RunSirius.execute_parallel_runs(SQpath, sqproPath, runList)
# RunSirius.logging.basicConfig(level=RunSirius.logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# RunSirius.execute_parallel_runs(SQpath, sqproPath, runList, max_workers=5)
num_workers = len(runList)  # You can change this number based on the resources available
workers = []
workers = RunSirius.start_worker(workers, SQpath, sqproPath, runList, num_workers)




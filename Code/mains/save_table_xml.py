from Code.functions.HandleDB import *
from Code.Constant import *

soil_string = save_soil_xml([410,411],  db_config)
management_string = save_management_xml([419,420],  db_config)
non_var_xml = save_parameters_xml([720,721,722],  db_config, 'non-varietal')
var_xml = save_parameters_xml([720,721,722],  db_config, 'varietal')
site_string = save_site_xml([362,363], db_config)


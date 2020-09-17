# Necessary packages
import sys
sys.path.append('../')
from prov_acquisition.prov_libraries import provenance as pr
from prov_acquisition.prov_libraries import provenance_lib as pr_lib
import pandas as pd
import argparse
import time
import numpy as np
import os

def main(input_path, opt):
	# Specify where to save the processed files as savepath
	output_path = os.path.join('prov_results', os.path.basename(input_path))
	savepath = os.path.join(output_path, 'ST_prov')

	df = pd.read_csv(input_path)
	#Trade columnds
	#['T_ID', 'T_DTS', 'T_ST_ID', 'T_TT_ID', 'T_IS_CASH', 'T_S_SYMB', 'T_QTY', 'T_BIDPRICE', 'C_ID', 'T_EXEX_NAME', 'T_TRADE_PRICE', 'T_CHRG', 'T_COMM', 'T_TAX', 'ActionType', 'ActionTS', 'C_TAX_ID', 'C_L_NAME', 'C_F_NAME', 'C_M_NAME', 'C_GNDR', 'C_TIER', 'C_DOB', 'C_ADLINE1', 'C_ADLINE2', 'C_ZIPCODE', 'C_CITY', 'C_STATE_PROV', 'C_CTRY', 'C_CTRY_1', 'C_AREA_1', 'C_LOCAL_1', 'C_EXT_1', 'C_CTRY_2', 'C_AREA_2', 'C_LOCAL_2', 'C_EXT_2', 'C_CTRY_3', 'C_AREA_3', 'C_LOCAL_3', 'C_EXT_3', 'C_EMAIL_1', 'C_EMAIL_2', 'C_LCL_TX_ID', 'C_NAT_TX_ID']

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		p = pr_lib.Provenance(df, savepath)
	
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Input prov entities created and saved')

	#SPACE TRANSFORMATION: add column on trade T_BOOL
	#Check Null Commission
	df['T_BOOL'] = [0 if s == None else 1 for s in df.T_COMM] 

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Space Transformation done: Check Null Commission, T_BOOL column added')

	#GET PROVENANCE
	d = p.get_prov_space_transformation(df, ['T_COMM'])
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov Space Transformation saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', dest='inputpath', required=True, help='Input file path')
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.inputpath, args.opt)



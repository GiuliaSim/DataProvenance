# Necessary packages
import sys
sys.path.append('../')
from prov_acquisition.prov_libraries import provenance as pr
from prov_acquisition.prov_libraries import provenance_lib as pr_lib
import pandas as pd
import argparse
import time
import os

def main(input_path, opt):
	# Specify where to save the processed files as savepath
	output_path = os.path.join('prov_results', os.path.basename(input_path))
	savepath = os.path.join(output_path, 'I_prov')

	df = pd.read_csv(input_path)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		p = pr_lib.Provenance(df, savepath)
	
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Input prov entities created and saved')

	#IMPUTATION: fill NaN elements of T_COMM column with avg
	AVG_comm = df['T_COMM'].mean()
	df['T_COMM'] = df['T_COMM'].fillna(AVG_comm)
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Imputation done: fill NaN elements of T_COMM column')

	#GET PROVENANCE
	d = p.get_prov_imputation(df, ['T_COMM'])
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov Imputation saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', dest='inputpath', required=True, help='Input file path')
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.inputpath, args.opt)



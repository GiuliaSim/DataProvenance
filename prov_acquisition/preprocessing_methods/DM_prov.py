# Necessary packages
import sys
sys.path.append('../')
from prov_acquisition.prov_libraries import provenance as pr
from prov_acquisition.prov_libraries import provenance_lib as pr_lib
import pandas as pd
import argparse
from random import randrange
import time
import os

def main(input_path, opt):
	# Specify where to save the processed files as savepath
	output_path = os.path.join('prov_results', os.path.basename(input_path))
	savepath = os.path.join(output_path, 'DM_prov')

	df = pd.read_csv(input_path)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		p = pr_lib.Provenance(df, savepath)
	
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Input prov entities created and saved')


	#DIMENSIONALITY REDUCTION: randomly removes one column from df
	columns = df.columns
	random_col = randrange(len(columns)-1)
	to_delete = columns[random_col]
	df = df.drop([to_delete], axis=1)
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Dimensionality Reduction done: ' + to_delete + ' column deleted')

	#GET PROVENANCE
	d = p.get_prov_dim_reduction(df)
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov Dimensionality Reduction saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', dest='inputpath', required=True, help='Input file path')
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.inputpath, args.opt)



# Necessary packages
import sys
sys.path.append('../')
from prov_acquisition.prov_libraries import provenance as pr
from prov_acquisition.prov_libraries import provenance_lib as pr_lib
import pandas as pd
import argparse
import time
import os
import numpy as np

def main(input_path, opt):
	# Specify where to save the processed files as savepath
	output_path = os.path.join('prov_results', os.path.basename(input_path))
	savepath = os.path.join(output_path, 'VT_prov')

	df = pd.read_csv(input_path)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		p = pr_lib.Provenance(df, savepath)
	
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Input prov entities created and saved')


	#VALUE TRANSFORMATION: remove invalid date of birth [C_DOB].
	#DOB < Batch_Date - 100 or DOB > Batch_Date
	batch_Date_from = '1917-07-07'
	batch_Date_to = '2017-07-07'
	df['C_DOB'] = [g if g >= batch_Date_from and g <= batch_Date_to else np.nan for g in df.C_DOB]

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Value Transformation done: removed invalid DOB')

	#GET PROVENANCE
	d = p.get_prov_value_transformation(df, ['C_DOB'])
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov Value Transformation saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', dest='inputpath', required=True, help='Input file path')
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.inputpath, args.opt)



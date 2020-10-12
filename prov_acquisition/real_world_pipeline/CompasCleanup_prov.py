# Necessary packages
import sys
sys.path.append('../')
from prov_acquisition.prov_libraries import provenance as pr
from prov_acquisition.prov_libraries import provenance_lib as pr_lib
import pandas as pd
import numpy as np
import time
import argparse
import os

def main(opt):
	input_path = 'real_world_pipeline/Datasets/compas.csv'
	output_path = 'prov_results'
	
	# Specify where to save the processed files as savepath
	savepath = os.path.join(output_path, 'Compas')

	df = pd.read_csv(input_path, header=0)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		savepath = os.path.join(savepath, 'FP')
		p = pr_lib.Provenance(df, savepath)
	
	
	# OPERATION O
	# select relevant columns
	df =df[['age', 'c_charge_degree', 'race', 'sex', 'priors_count', 'days_b_screening_arrest', 'two_year_recid', 'c_jail_in', 'c_jail_out']]

	d = p.get_prov_dim_reduction(df)

	# OPERATION 1
	# Remove missing values
	df = df.dropna()

	d = p.get_prov_dim_reduction(df)

	# OPERATION 2
	# Make race binary
	df.race = [0 if r != 'Caucasian' else 1 for r in df.race]

	d = p.get_prov_feature_transformation(df, ['race'])

	# OPERATION 3
	# Make two_year_recid the label
	df = df.rename({'two_year_recid': 'label'}, axis=1)

	# reverse label for consistency with function defs: 1 means no recid (good), 0 means recid (bad)
	df.label = [0 if l == 1 else 1 for l in df.label]

	d = p.get_prov_feature_transformation(df, ['label'])

	# OPERATION 4
	# convert jailtime to days
	df['jailtime'] = (pd.to_datetime(df.c_jail_out) - pd.to_datetime(df.c_jail_in)).dt.days

	#Get provenance of space transformation
	d = p.get_prov_space_transformation(df, ['c_jail_out','c_jail_in'])

	# OPERATION 5
	#drop jail in and out dates
	df = df.drop(['c_jail_in', 'c_jail_out'], axis=1)

	d = p.get_prov_dim_reduction(df)
	
	# OPERATION 6
	# M: misconduct, F: felony
	df.c_charge_degree = [0 if s == 'M' else 1 for s in df.c_charge_degree]

	d = p.get_prov_feature_transformation(df, ['c_charge_degree'])

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.opt)
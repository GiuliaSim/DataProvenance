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
	input_path = 'real_world_pipeline/Datasets/census.csv'
	filename_ext = os.path.basename(input_path)
	filename, ext = os.path.splitext(filename_ext)
	output_path = 'prov_results'
	
	# Specify where to save the processed files as savepath
	savepath = os.path.join(output_path, filename)

	df = pd.read_csv(input_path)

	# Assign names to columns
	names = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'label']

	df.columns = names

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		savepath = os.path.join(savepath, 'FP')
		p = pr_lib.Provenance(df, savepath)
	
	#OPERATION 0
	# Cleanup names from spaces
	col = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'native-country', 'label']

	for c in col:
	    df[c] = df[c].map(str.strip)

	#PROVENANCE 0
	d = p.get_prov_feature_transformation(df, col, 'Cleanup names from spaces')


	#OPERATION 1
	# Replace ? character for NaN value
	df = df.replace('?', np.nan)

	#PROVENANCE 1
	d = p.get_prov_value_transformation(df, df.columns)


	#OPERATION 2-3
	# One-hot encode categorical variables
	col = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'native-country']

	for c in col:	    
	    dummies = []
	    dummies.append(pd.get_dummies(df[c]))
	    df_dummies = pd.concat(dummies, axis = 1)
	    df = pd.concat((df, df_dummies), axis = 1)
	    df = df.drop([c], axis = 1)
	    #PROVENANCE 2-3
	    d = p.get_prov_space_transformation(df, [c])

	#OPERATION 4
	# Assign sex and label binary values 0 and 1
	df.sex = df.sex.replace('Male', 1)
	df.sex = df.sex.replace('Female', 0)
	df.label = df.label.replace('<=50K', 0)
	df.label = df.label.replace('>50K', 1)

	#PROVENANCE 4
	col = ['sex', 'label']
	d = p.get_prov_feature_transformation(df, col, 'Assign sex and label binary values 0 and 1')

	#OPERATION 5
	# Drop fnlwgt variable
	df = df.drop(['fnlwgt'], axis=1)
	#PROVENANCE 5
	d = p.get_prov_dim_reduction(df)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.opt)
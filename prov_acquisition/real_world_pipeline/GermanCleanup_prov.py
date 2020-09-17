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
	input_path = 'real_world_pipeline/Datasets/german.csv'
	output_path = 'prov_results'
	
	# Specify where to save the processed files as savepath
	savepath = os.path.join(output_path, 'German')

	df = pd.read_csv(input_path, header=0)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	# Create a new provenance document
	if opt: 
		p = pr.Provenance(df, savepath)
	else:
		savepath = os.path.join(savepath, 'FP')
		p = pr_lib.Provenance(df, savepath)
	
	#OPERATION 0
	# Turn criptic values into interpretable form
	df = df.replace({'checking': {'A11': 'check_low', 'A12': 'check_mid', 'A13': 'check_high',
	                              'A14': 'check_none'},
	                 'credit_history': {'A30': 'debt_none', 'A31': 'debt_noneBank',
	                                    'A32': 'debt_onSchedule','A33': 'debt_delay',
	                                    'A34': 'debt_critical'},
	                 'purpose': {'A40': 'pur_newCar', 'A41': 'pur_usedCar',
	                             'A42': 'pur_furniture', 'A43': 'pur_tv',
	                             'A44': 'pur_appliance', 'A45': 'pur_repairs',
	                             'A46': 'pur_education', 'A47': 'pur_vacation',
	                             'A48': 'pur_retraining', 'A49': 'pur_business',
	                             'A410': 'pur_other'},
	                 'savings': {'A61': 'sav_small', 'A62': 'sav_medium', 'A63': 'sav_large',
	                             'A64': 'sav_xlarge', 'A65': 'sav_none'},
	                 'employment': {'A71': 'emp_unemployed', 'A72': 'emp_lessOne',
	                                'A73': 'emp_lessFour', 'A74': 'emp_lessSeven',
	                                'A75': 'emp_moreSeven'},
	                 'other_debtors': {'A101': 'debtor_none', 'A102': 'debtor_coApp',
	                                   'A103': 'debtor_guarantor'},
	                 'property': {'A121': 'prop_realEstate', 'A122': 'prop_agreement',
	                              'A123': 'prop_car', 'A124': 'prop_none'},
	                 'other_inst': {'A141': 'oi_bank', 'A142': 'oi_stores', 'A143': 'oi_none'},
	                 'housing': {'A151': 'hous_rent', 'A152': 'hous_own', 'A153': 'hous_free'},
	                 'job': {'A171': 'job_unskilledNR', 'A172': 'job_unskilledR',
	                         'A173': 'job_skilled', 'A174': 'job_highSkill'},
	                 'phone': {'A191': 0, 'A192': 1},
	                 'foreigner': {'A201': 1, 'A202': 0},
	                 'label': {2: 0}})
	col = ['checking', 'credit_history', 'purpose', 'savings', 'employment', 'other_debtors', 'property', 'other_inst', 'housing', 'job', 'phone', 'foreigner', 'label']

	d = p.get_prov_feature_transformation(df, col)

	#OPERATION 1
	# More criptic values translating
	df['status'] = np.where(df.personal_status == 'A91', 'divorced',
	                        np.where(df.personal_status == 'A92', 'divorced', 
	                                 np.where(df.personal_status == 'A93', 'single',
	                                          np.where(df.personal_status == 'A95', 'single',
	                                                   'married'))))

	# Translate gender values
	df['gender'] = np.where(df.personal_status == 'A92', 0,
	                        np.where(df.personal_status == 'A95', 0,
	                                 1))

	d = p.get_prov_space_transformation(df, ['personal_status'])

	#OPERATION 2
	# Drop personal_status column
	df = df.drop(['personal_status'], axis=1)

	d = p.get_prov_dim_reduction(df)

	#OPERATION 3-13
	# One-hot encode categorical columns
	col = ['checking', 'credit_history', 'purpose', 'savings', 'employment', 'other_debtors', 'property',
	       'other_inst', 'housing', 'job', 'status']
	for c in col:
	    dummies = []
	    dummies.append(pd.get_dummies(df[c]))
	    df_dummies = pd.concat(dummies, axis = 1)
	    df = pd.concat((df, df_dummies), axis = 1)
	    df = df.drop([c], axis = 1)
	    d = p.get_prov_space_transformation(df, [c])

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.opt)
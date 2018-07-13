import pandas as pd
import statistics
import matplotlib.pyplot as plt
import numpy as np
# from sklearn import datasets, linear_model
# from sklearn.metrics import mean_squared_error, r2_score

class Plotter():
	def __init__(self):
		self.color_codes = ['#1a1aff', '#00cc00', '#a300cc', '#808000', '#527a7a', '#663300', '#33ff33', '#ff8080']
		self.red_hex_code = '#ff0000'
	
	def AlkDMIonStatsPlot(self, df):
		DataSets_lst = df['DataSet'].unique()
		fig = plt.figure(figsize=(18,9))
		
		for n, dset in enumerate(DataSets_lst):
			df_sliced = df[df['DataSet'] == dset].copy()
			instrument = df_sliced['inst'].iloc[2]
			offset = df_sliced['offset_volts'].iloc[2]
			dv = df_sliced['Det_Volts'].iloc[2]
			curve_label = 'Inst: {i} - Offset: +{v} v = {d} v'.format(i=instrument, v=offset, d=dv)
			
			plt.scatter(df_sliced['Cumulative_Inj'], df_sliced['ave_api'], color=self.color_codes[n], label=curve_label)
		
		plt.ylabel('Ave. Aera Per Ion')
		plt.xlabel('Sample Injections')
		plt.title('Tracking Area Per Ion via Detector Measurement\nOver ~48 Hours of Continuous Sample Acquisition')
			
		plt.legend(loc='upper right')
		plt.show()
import pandas as pd #@UnusedImport
import statistics #@UnusedImport
import matplotlib.pyplot as plt
import matplotlib #@UnusedImport
import numpy as np #@UnusedImport

class Plotter():
	def __init__(self):
		self.color_codes = ['#1a1aff', '#00cc00', '#a300cc', '#808000', '#527a7a', '#663300', '#33ff33', '#ff8080']
		self.red_hex_code = '#ff0000'
	
	def AlkDMIonStatsPlot(self, df):
		DataSets_lst = df['DataSet'].unique()
		fig = plt.figure(figsize=(15.5,9))
		ax = fig.add_subplot(1,1,1)
		
		for n, dset in enumerate(DataSets_lst):
			df_sliced = df[df['DataSet'] == dset].copy()
			instrument = df_sliced['inst'].iloc[2]
			offset = df_sliced['offset_volts'].iloc[2]
			dv = df_sliced['Det_Volts'].iloc[2]
			curve_label = 'Inst: {i} - Offset: +{v} v = {d} v'.format(i=instrument, v=offset, d=dv)
			
			ax.plot(df_sliced['Cumulative_Inj'], df_sliced['ave_api'], color=self.color_codes[n], label=curve_label)
		
# 		for key,spine in ax.spines.items(): #@UnusedVariable
# 			spine.set_visible(False)
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)
		
		plt.ylabel('Ave. Aera Per Ion')
		plt.xlabel('Sample Injections')
		plt.title('Tracking Area Per Ion via Detector Measurement\nOver ~48 Hours of Continuous Sample Acquisition')

# 		plt.tick_params(bottom="off", top="off", left="off", right="off")
		legend_h_offset, legend_v_offset = 1.25, 0.75
		plt.legend(loc='center right', bbox_to_anchor=(legend_h_offset, legend_v_offset))
		plt.savefig('DM_API_Analysis', bbox_inches='tight')
		plt.show()
		
		
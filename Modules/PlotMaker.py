import pandas as pd #@UnusedImport
import matplotlib.pyplot as plt
import matplotlib #@UnusedImport
import numpy as np #@UnusedImport

class Plotter():
	def __init__(self):
		self.red_hex_code = '#ff0000'

	def AlkDMIonStatsSplitPlot(self, df):
		PV1_DataSets_lst = df[df['inst'] == 'PV1']['DataSet'].unique()
		PV2_DataSets_lst = df[df['inst'] == 'PV2']['DataSet'].unique()
		inst_sets = [PV1_DataSets_lst,PV2_DataSets_lst]
		ax_title = ['Peg-BT PV1', 'Peg-BT PV2']
		
		
		fig = plt.figure(figsize=(25,9))
		ax1 = fig.add_subplot(1,2,1)
		ax2 = fig.add_subplot(1,2,2)		
		ax1.set_prop_cycle('color',plt.cm.spectral(np.linspace(0.1,0.9,4))) #@UndefinedVariable
		ax2.set_prop_cycle('color',plt.cm.spectral(np.linspace(0.1,0.9,4))) #@UndefinedVariable
		ax = [ax1,ax2]
		
		for a in range(2):
			
			ax[a].spines['right'].set_visible(False)
			ax[a].spines['top'].set_visible(False)
			ax[a].set_ylabel('Area Per Ion via Detector Measurement')
			ax[a].set_xlabel('Alkane Standard\nSample Injection Count')
			ax[a].set_title(ax_title[a])
			
			for dset in inst_sets[a]:
				df_sliced = df[df['DataSet'] == dset].copy()
				offset = df_sliced['offset_volts'].iloc[2]
				dv = df_sliced['Det_Volts'].iloc[2]
				curve_label = 'Offset: +{v} v = {d} v'.format(v=offset, d=dv)
				ax[a].plot(df_sliced['Cumulative_Inj'], df_sliced['ave_api'], label=curve_label)
				
			ax[a].legend(loc='center', bbox_to_anchor=(0.17,-0.1))
		
# 		plt.suptitle('Tracking Area Per Ion via Detector Measurement\nOver ~48 Hours of Continuous Sample Acquisition', fontsize=14)
		plt.savefig('DM_API_Analysis', bbox_inches='tight')
		plt.show()


	
	def AlkDMIonStatsPlot(self, df):
		DataSets_lst = df['DataSet'].unique()
		fig = plt.figure(figsize=(15.5,9))
		ax = fig.add_subplot(1,1,1)
		ax.set_prop_cycle('color',plt.cm.spectral(np.linspace(0.1,1.00,8))) #@UndefinedVariable
		
		for dset in DataSets_lst:
			df_sliced = df[df['DataSet'] == dset].copy()
			instrument = df_sliced['inst'].iloc[2]
			offset = df_sliced['offset_volts'].iloc[2]
			dv = df_sliced['Det_Volts'].iloc[2]
			curve_label = 'Inst: {i} - Offset: +{v} v = {d} v'.format(i=instrument, v=offset, d=dv)
			
			ax.plot(df_sliced['Cumulative_Inj'], df_sliced['ave_api'], label=curve_label)
		
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)
		
		plt.ylabel('Ave. Aera Per Ion')
		plt.xlabel('Sample Injections')
		plt.title('Tracking Area Per Ion via Detector Measurement\nOver ~48 Hours of Continuous Sample Acquisition')

		legend_h_offset, legend_v_offset = 1.25, 0.75
		plt.legend(loc='center right', bbox_to_anchor=(legend_h_offset, legend_v_offset))
		plt.savefig('DM_API_Analysis', bbox_inches='tight')
		plt.show()
		
	def GenericIndividualPlotMaker(self, xdata_lst, ydata_lst, legendlbl_lst, xlbl, ylbl, plot_title, png_filename, legend_h_offset=1.25, legend_v_offset=0.75, legend_location='center'):
		# xdata & ydata: both are a list of lists each containing the corresponding axis data.  These are the requirement of these two
			# data set to prevent an error:
				# Sublists with the same index are a matching x vs y set that will be plotted.  They MUST be the same length to prevent an error.
				# There must be the same number of sub lists to prevent an error.
		# legendlbl_lst: a list of legend labels for each x vs y plot.  Again there must be the same number of items in this list as x/y pairs.
		# The rest are self explainatory
		fig = plt.figure(figsize=(15.5,9))
		ax = fig.add_subplot(1,1,1)
		
		for i in range(len(xdata_lst)):
			ax.plot(xdata_lst[i], ydata_lst[i], color=self.color_codes[i], label=legendlbl_lst[i])
			
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)
		
		plt.ylabel(ylbl)
		plt.xlabel(xlbl)
		plt.title(plot_title)

		plt.legend(loc=legend_location, bbox_to_anchor=(legend_h_offset, legend_v_offset))
		plt.savefig(png_filename, bbox_inches='tight')
		
		# (x_data, all_y_data, legendlbl_lst, xlbl, plot_titles, figure_title, all_png_filenames)
	def GenericCombinedPlotMaker(self, xdata_lst, ydata_lst, legendlbl_lst, xlbl, ylbl_lst, fig_title, png_filename, legend_h_offset=0.9, legend_v_offset=2.4, legend_location='center'):
		# xdata_lst: is a list of lists each containing the corresponding x-axis data.  The x-axis data is the same for all ax_n objects
			# Generic example: [Series_1_x-axis_data_lst, Series_n_x-axis_data_lst...]
		# ydata_lst: is a list of lists of lists containing all the y-axis data.
			# Generic example: [ax_1[Series_1_y-axis_data_lst, Series_n_y-axis_data_lst...], ax_n[ax_1[Series_1_y-axis_data_lst, Series_n_y-axis_data_lst...]...]	
			# data set to prevent an error:
				# Sublists with the same index are a matching x vs y set that will be plotted.  They MUST be the same length to prevent an error.
				# There must be the same number of sub lists to prevent an error.
		# legendlbl_lst: a list of legend labels for each x vs y plot.  Again there must be the same number of items in this list as x/y pairs.
		# The rest are self explainatory
		fig = plt.figure(figsize=(25,9))
		ax = []
		
		for a in range(4):
			ax.append(fig.add_subplot(2,2,1+a))
			ax[a].set_prop_cycle('color',plt.cm.spectral(np.linspace(0.25,0.84,2))) #@UndefinedVariable
			
			for s in range(len(xdata_lst)):
				ax[a].plot(xdata_lst[s], ydata_lst[a][s], label=legendlbl_lst[s])
				ax[a].spines['right'].set_visible(False)
				ax[a].spines['top'].set_visible(False)
				ax[a].set_ylabel(ylbl_lst[a])
				
				
				if (a == 2 or a == 3) and s == 1:
					plt.xlabel(xlbl)
				elif (a == 0 or a == 1) and s == 1:
					ax[a].set_xticklabels([])
					ax[a].spines['bottom'].set_visible(False)
					ax[a].xaxis.set_ticks_position('none')
					
		plt.suptitle(fig_title, fontsize=20)
		plt.legend(loc=legend_location, bbox_to_anchor=(legend_h_offset, legend_v_offset))
		plt.savefig(png_filename, bbox_inches='tight')
		
	def Manual_OFN20fg_IDL(self):
		fig = plt.figure(figsize=(25,9))
		ax = fig.add_subplot(1,1,1)
		ax.set_prop_cycle('color',plt.cm.spectral(np.linspace(0.25,0.84,2))) #@UndefinedVariable
		
		xdata = [0,150,250,350]
		ydata = [[0.036614, 0.009674, 0.0056418, 0.004696],[0.0083151, 0.0044855, 0.0046082, 0.0033099]]
		legendlbl_lst = ['Peg BT - PV1', 'Peg BT - PV2']
		
		for s in range(len(ydata)):
			ax.plot(xdata, ydata[s], label=legendlbl_lst[s])
			
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)
		ax.set_ylabel('IDL pg')
		ax.set_xlabel('Optimized Detector Voltage Offset (volts)')
		plt.legend()
		plt.suptitle('IDL vs Detector Voltage Offset\nOFN 0.02 pg On Column\nQuant Mass = 271.99', fontsize=20)
		plt.savefig('OFN_20fg_IDL_Plot', bbox_inches='tight')
		
	def Manual_GO_Plot(self):
		fig = plt.figure(figsize=(25,9))
		ax = fig.add_subplot(1,1,1)
		ax.set_prop_cycle('color',plt.cm.spectral(np.linspace(0.25,0.84,2))) #@UndefinedVariable
		
		xdata = [0,150,250,350]
		ydata = [[-7.7, 26.5, 42.8, 66.1],[-8, 4.1, 13.5, 48.4]]
		legendlbl_lst = ['Peg BT - PV1', 'Peg BT - PV2']
		
		for s in range(len(ydata)):
			ax.plot(xdata, ydata[s], label=legendlbl_lst[s])
			
		ax.spines['right'].set_visible(False)
		ax.spines['top'].set_visible(False)
		ax.set_ylabel('Change in Optimized Detector Voltage')
		ax.set_xlabel('Optimized Detector Voltage Offset (volts)')
		plt.legend()
# 		plt.suptitle('Change in Optimized Detector Voltage\nFrom the Beginning to the End of a Data Set', fontsize=20)
		plt.savefig('GO_Delta_Plot', bbox_inches='tight')
		plt.show()
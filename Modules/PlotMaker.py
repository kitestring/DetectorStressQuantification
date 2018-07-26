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
		# plt.show()
		
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
		fig = plt.figure(figsize=(18,9))
		
		ax = []
# 		ax_row = [1,2,1,2]
# 		ax_column = [1,1,2,2]
		
		for a in range(4):
			ax.append(fig.add_subplot(2,2,1+a))
			ax[a].set_prop_cycle('color',plt.cm.spectral(np.linspace(0.1,1.3,10)))
			
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
					
		plt.suptitle(fig_title, fontsize=16)
		plt.legend(loc=legend_location, bbox_to_anchor=(legend_h_offset, legend_v_offset))
		plt.savefig(png_filename, bbox_inches='tight')
		plt.show()
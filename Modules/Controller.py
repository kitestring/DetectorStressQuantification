from csvExtracter import Extract
from CLI import ConcoleGUI 
from SQL import Postgres
from PlotMaker import Plotter
import credentials
import os
import pprint
import pandas as pd #@UnusedImport

class Controls():
	
	def __init__(self):
		self.cli = ConcoleGUI()
		self.csvDirectory = os.getcwd()
		self.db = Postgres(credentials.userinfo['db'], credentials.userinfo['id'], credentials.userinfo['pw'])
		
		
		self.commandDict = {
			'help': [self.basicInstructions, "Lists each command."],
			'exit': [self.exitProgram, "Exits the program."],
			'mine': [self.extractData, 'Mines the data from all the csv files found in the\n\t\tcurrent working directory.'],
			'osen': [self.getOFNSensitivityData, 'Generate the OFN sensitivity plots and tables.'],
			'olin': [self.getOFNLinearityData, 'Generate the OFN sensitivity plots and tables.'],
			'ostat': [self.getOFNIonStats, 'Generate OFN Ion Stats data visualizations'],
			'algo': [self.getAlkaneGOIonStats, "Generate Alkane GO Ion Stats data visualizations"],
			'aldm': [self.getAlkaneDMIonStats, "Generate Alkane DM Ion Stats data visualizations"],
			'acp': [self.initialize_AnalyteCombinedPlotResults, "Generate Analyte combined single plot data visualizations"],
			'asp': [self.getAnalyteIndividualPlotsResults, "Analyte Single plots data visualizations"]
			}
		
		self.runProgram = True
		
	def run(self):
		# returns the boolean value for runProgram
		return self.runProgram
	
	def exitProgram(self):
		# sets the runProgram boolean to false
		self.db.close_all_connections()
		self.runProgram = False
		self.giveUserFeedback('Exiting Program...\nGoodbye')

	def runUserCommand(self, prompt):
		# Prompts the user for a command using the getRawUserInput method then
		# sends the command to the exectuteCommand method
		self.exectuteCommand(self.cli.userInput(prompt))
		
	def getRawUserInput(self, prompt):
		# Prompts the user for a command using the userInput
		# method which the GUI class has.
		return self.cli.userInput(prompt)
		
	def giveUserFeedback(self, text):
		# Outputs feedback to the user via text to the concoleOutput
		# this is handled by the concoleOutput method with the GUI class has
		self.cli.concoleOutput(text)
	
	def basicInstructions(self):
		# Gets the program instructions string from the getCommandsWithDescriptions method
		# then sends the string to the giveUserFeedback method
		text = self.getCommandsWithDescriptions()
		self.giveUserFeedback(text)
		
	def getCommandsWithDescriptions(self):
		# Iterates through the commandDict to build a string
		# that lists each command with a description of what it does
		keyList = self.commandDict.keys()
		keyList = sorted(keyList)
		commandString = 'Command\t\tDescription'
		for key in keyList:
			commandString += '\n%s\t\t%s' % (key, self.commandDict[key][1])
		
		return commandString
	
	def getAlkaneDMIonStats(self):
		# Get the reps versus concentration data
		alk_InjectionReps_df = self.db.AlkInjectionReps()
		df_cleaner = Extract(self.csvDirectory)
		
		# Extract the DataSet name and concentration from Sample column creating 2 new columns
		alk_InjectionReps_df.rename(index=str, columns={"s_name": "Sample"}, inplace=True)
		Series_ParcedSample = alk_InjectionReps_df.apply(df_cleaner.ParcePeakTableSampleName, axis=1)
		df_ParcedSample_Split = Series_ParcedSample.str.split(pat=',', expand=True)
		df_ParcedSample_Split.columns = ['DataSet','Conc_pg']
		alk_InjectionReps_df = pd.concat([alk_InjectionReps_df, df_ParcedSample_Split], axis=1)
		alk_InjectionReps_df['Conc_pg'] = alk_InjectionReps_df['Conc_pg'].astype(dtype='float64')
		
		# Create a new column (Cumulative_Injections) by summing up the reps column for each DataSet
		injections = []
		active_dataset = ''
		
		for r in alk_InjectionReps_df.iterrows():
			
			# This resets the counting for each DataSet
			if r[1]['DataSet'] != active_dataset:
				active_dataset = r[1]['DataSet']
				injections_subtotal = 0
				
			injections_subtotal += int(r[1]['reps'])
			injections.append(injections_subtotal)
			
		alk_InjectionReps_df['Cumulative_Inj'] = injections
		alk_InjectionReps_df.drop(columns=['setname','seq','reps'], inplace=True)
		
		# Get the average API & detector voltage data
		alk_ave_DM_df = self.db.AlkAveDMData()
		alk_ave_DM_df.rename(index=str, columns={"setname": "DataSet"}, inplace=True)
		
		# Combine the reps versus concentration data and the average API & detector voltage data
		# for each data set.
		combined_df = df_cleaner.CombineAlkConcRepsAndDMIonStats(alk_InjectionReps_df.copy(), alk_ave_DM_df.copy())
		
		# correct combined_df['inst'] where is == 0
		combined_df['inst'] = combined_df.apply(df_cleaner.CorrectInstrumentLbl, axis=1)
		
		self.VisualizeAlkDMIonStats(combined_df)
		
	def VisualizeAlkDMIonStats(self, df):
		plot_builder = Plotter()
		plot_builder.AlkDMIonStatsSplitPlot(df)
		
	def getAlkaneGOIonStats(self):
		alk_GO_ion_stats = self.db.AlkGOIonStats()
		self.printDataStructure(alk_GO_ion_stats)
		self.DataFrameToCSV(alk_GO_ion_stats,'Alkane_GO_IonStats_Results',True)
	
	def DataFrameToCSV(self, df, FileName, include_index):
		df.to_csv(FileName + '.csv', index=include_index, encoding='utf-8')
	
	def getOFNIonStats(self):
		ofn_ion_stats = self.db.OFNIonStats()
		self.printDataStructure(ofn_ion_stats)
		self.DataFrameToCSV(ofn_ion_stats, 'OFN_Ion_Stats_Results', True)
	
	def getOFNLinearityData(self):
		ofn_linearity_df = self.db.OFNLinearityData()
		self.printDataStructure(ofn_linearity_df)
		self.DataFrameToCSV(ofn_linearity_df, 'OFN_Linearity_Results', True)

	def getAnalyteIndividualPlotsResults(self):
		conc_pg = 0.5
		
		Analyte_tup = ('OFN','Perfluoronaphthalene')
		Analyte_id = 'OFN'
		Quant_Masses = 'Quant Mass = 271.99'
		
		Analyte_tup =  ('Tetradecane','Tetradecane (C14)')
		Analyte_id = 'Tetradecane'
		Quant_Masses = 'Quant Masses = SUM(57.07, 71.09, 81.10)'
		
		file_prefix = '{a}_{c}pg'.format(a=Analyte_id,c=conc_pg)
	
		AnalyteResults = self.db.Analyte_SingleConcentration_Results(conc_pg, Analyte_tup)
		AnalyteResults.fillna(0,inplace=True)
		self.printDataStructure(AnalyteResults)
		self.DataFrameToCSV(AnalyteResults, file_prefix + "_TabulatedResults", True)
		
		# X-axis data & label and legend labels for all plots
		plot_builder = Plotter()
		x_data = [AnalyteResults[AnalyteResults['inst'] == 'PV1']['det_offset'].tolist(), AnalyteResults[AnalyteResults['inst'] == 'PV2']['det_offset'].tolist()]
		legendlbl_lst = ['Peg BT - PV1','Peg BT - PV2']
		xlbl = 'Optimized Detector Voltage Offset (volts)'
		
		# Y-axis data & label, pngfile name, and chart title for each plot
		df_columns = ['ave_area', 'ave_height', 'ave_quant_sn', 'ave_similarity']
		plot_titles = ['Average Area', 'Average Height', 'Average Quant S/N', 'Average Similarity']
		png_filename_suffix = ['_Area_Plot', '_Height_Plot', '_Quant_SN_Plot', '_Similarity_Plot']
		dp_type = ['Target Analyte Finding', 'Target Analyte Finding', 'Target Analyte Finding', 'Non-Targeted Deconvolution']
		
		# Iterate to make each of the 4 plot types (Area, Height, Quant S/N, & Similarity)
		for i in range(4):
			y_data = [AnalyteResults[AnalyteResults['inst'] == 'PV1'][df_columns[i]].tolist(), AnalyteResults[AnalyteResults['inst'] == 'PV2'][df_columns[i]].tolist()]
			ylbl = plot_titles[i]
			plot_title = '{t} - {a} {c} pg\n{q}\n{d}'.format(t=plot_titles[i], a=Analyte_id, c=conc_pg, q=Quant_Masses, d=dp_type[i])
			png_filename = (file_prefix + png_filename_suffix[i]).replace('.','_')
			print('Building: ', png_filename)
			plot_builder.GenericIndividualPlotMaker(x_data, y_data, legendlbl_lst, xlbl, ylbl, plot_title, png_filename, legend_h_offset=1.0, legend_v_offset=1.0)
		
		print('\nPlots complete')
	
	def initialize_AnalyteCombinedPlotResults(self):
		analyte_dict = {
			'OFN': [('Perfluoronaphthalene','OFN'),'OFN','Quant Mass = 271.99'],
			'C14': [('Tetradecane','Tetradecane (C14)'),'Tetradecane', 'Quant Masses = SUM(57.07, 71.09, 81.10)'],
			'C20': [('Eicosane','Eicosane (C20)'),'Eicosane', 'Quant Masses = SUM(57.07, 71.09, 81.10)'],
			'C30': [('Triacontane','Triacontane (C30)'),'Triacontane', 'Quant Masses = SUM(57.07, 71.09, 81.10)']
			}
		selection_message = "Enter an integer which corresponds to your selection"
		analyte_selection = self.getSingleSelection(selection_message, list(analyte_dict.keys()))
		
		
		if analyte_selection == 'Invalid Selection':
			self.giveUserFeedback("Invalid Selection\nAction Aborted")
		else:
			
			unique_conc_lst = self.db.UniqueConcentrations(analyte_dict[analyte_selection][0][1])['concentration_pg'].tolist()
			concentration_selection = self.getSingleSelection(selection_message, unique_conc_lst)
			
			if concentration_selection == 'Invalid Selection':
				self.giveUserFeedback("Invalid Selection\nAction Aborted")
			else:
				self.getAnalyteCombinedPlotResults(concentration_selection, analyte_dict[analyte_selection])
		
	def getAnalyteCombinedPlotResults(self, conc_pg, analyte_data):
# 		conc_pg = 0.1000
				
# 		Analyte_tup = ('OFN','Perfluoronaphthalene')
# 		Analyte_id = 'OFN'
# 		Quant_Masses = 'Quant Mass = 271.99'
		
		Analyte_tup =  analyte_data[0]
		Analyte_id = analyte_data[1]
		Quant_Masses = analyte_data[2]
		
# 		Analyte_tup =  ('Eicosane','Eicosane (C20)')
# 		Analyte_id = 'Eicosane'
# 		Quant_Masses = 'Quant Masses = SUM(57.07, 71.09, 81.10)'

# 		Analyte_tup =  ('Triacontane','Triacontane (C30)')
# 		Analyte_id = 'Triacontane'
# 		Quant_Masses = 'Quant Masses = SUM(57.07, 71.09, 81.10)'		
		
		file_prefix = '{a}_{c}pg'.format(a=Analyte_id,c=conc_pg)
	
		AnalyteResults = self.db.Analyte_SingleConcentration_Results(conc_pg, Analyte_tup)
		AnalyteResults.fillna(0,inplace=True)
		self.printDataStructure(AnalyteResults)
		self.DataFrameToCSV(AnalyteResults, file_prefix + "_TabulatedResults", True)
		
		# X-axis data & label and legend labels for all plots
		plot_builder = Plotter()
		x_data = [AnalyteResults[AnalyteResults['inst'] == 'PV1']['det_offset'].tolist(), AnalyteResults[AnalyteResults['inst'] == 'PV2']['det_offset'].tolist()]
		legendlbl_lst = ['Peg BT - PV1','Peg BT - PV2']
		xlbl = 'Optimized Detector Voltage Offset (volts)'
		
		# Y-axis data & label, pngfile name, and chart title for each plot
		df_columns = ['ave_area', 'ave_height', 'ave_quant_sn', 'ave_similarity']
		y_axis_lbls = ['Average Area', 'Average Height', 'Average Quant S/N', 'Average Similarity']
		figure_title = '{a} {c} pg\n{q}'.format(a=Analyte_id, c=conc_pg, q=Quant_Masses)
		png_filename = (file_prefix + "_plots").replace('.','_')
		
		# Initialize plot data structures
		all_y_data = []
		
		# Iterate to make the data structures for each of the 4 plot types (Area, Height, Quant S/N, & Similarity)
		for i in range(4):
			all_y_data.append([AnalyteResults[AnalyteResults['inst'] == 'PV1'][df_columns[i]].tolist(), AnalyteResults[AnalyteResults['inst'] == 'PV2'][df_columns[i]].tolist()])
			
		plot_builder.GenericCombinedPlotMaker(x_data, all_y_data, legendlbl_lst, xlbl, y_axis_lbls, figure_title, png_filename)
		
		
		
	def getOFNSensitivityData(self):
		ofn_sensitivity_df = self.db.OFNSensitivityData_20fg()
		self.printDataStructure(ofn_sensitivity_df)
		self.DataFrameToCSV(ofn_sensitivity_df, 'OFN_Sensitivity_Results', True)
	
	def extractData(self):
		# Gets a dictionary containing all the csv data.  Each value is a pandas dataframe
		# {'PeakTable':df_PeakTable, 'Sample':df_Sample, 'IDL':df_IDL, 'MS':df_MS, 'GC':df_GC}
		# df_PeakTable - Columns: 
			# 'Name', 'Type', 'Area', 'Height', 'FWHH (s)', 'Similarity',
			# 'RT_1D', 'RT_2D', 'Peak S/N',
			# 'Quant S/N', 'Sample', 'Concentration_pg'
		# df_Sample - Columns:
			# 'index', 'Type', 'Name', 'Status', 'Chromatographic Method',
			# 'MS Method', 'DateTime', 'DataSet', 'Instrument', 'DetectorVoltage', 'AreaPerIon'
		# df_IDL - Columns:
			# 'Concentration', 'IDL'
		# df_GC - Columns:
			# 'GC_Method_id', 'SplitRatio', 'Chromatography', 'RunTime_min'
		# df_MS - Columns:
			# 'MS_Method_id', 'AcquisitionRate', 'MassRange_Bottom', 'MassRange_Top',
			# 'ExtractionFrequency', 'DetectorOffset_Volts'
		# df_DR - ColumnsL
			# 'OrdersOfMagnitude', 'ConcRange_pg_Low',
			# 'ConcRange_pg_High', 'Correlation_Coefficient_r'

		csvExtractor = Extract(self.csvDirectory)
		DF_Dict, DataSet_id = csvExtractor.extract_csv_data()
		
		# DataSetCheck
		
		# If DataSet check pass then upload data
		
# 		self.printDataStructure(DF_Dict['PeakTable'][['Area','Height']])
# 		exit()
			
		self.db.UploadData(DF_Dict, DataSet_id)
		
	def printDataStructure(self, ds):
		pp = pprint.PrettyPrinter(indent=4,width=200,depth=20)
		pp.pprint(ds)
		
	def exectuteCommand(self, command):
		# using the user input as a key, gets the corresponding value from the 
		# commandDict.  The value is a list that corresponds to a given command
		# The list is formatted as follows: [self.method, "Command description"]
		# If the key is not found "bad input" the user is given an
		# invalid command feedback statement.
		method = self.commandDict.get(command.lower(), 'Invalid Command')
		
		if method != 'Invalid Command':
			method[0]()
		else:
			self.giveUserFeedback('Invalid Command\nFor a list of the commands type "help"')
			
	def getSingleSelection(self, selection_message, selectionLst):
		# Allows the user to select an item by selecting a corresponding integer.
		# Returns the selected item as a string, unless the selection is invalid, then "Invalid Selection" is returned
		selection_dict = {}
		for n, item in enumerate(selectionLst):
			selection_dict[str(n)] = item
			
			
		text = '\n%s\t%s' % ('No.', 'Selection')
		for key, value in selection_dict.items():
			text = text + '\n%s\t%s' % (key, value)
		self.giveUserFeedback(text)
		
		selection = self.getRawUserInput(selection_message)
		return selection_dict.get(selection, "Invalid Selection")	

# create a new controls object and prompt the user with the welcome message
# then populate with the program instructions			
app = Controls()
app.giveUserFeedback('\n\n\n\n\n\nWelcome to DetectorStressAnalyzer\nBelow is the list of commands.')
app.basicInstructions()

# enter while loop that accepts user input until the user
# "exits" the program which will cause the run() method to return false
while(app.run()):
	app.runUserCommand("Enter Command")

	
	
	
	
	
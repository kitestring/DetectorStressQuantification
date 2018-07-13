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
			'aldm': [self.getAlkaneDMIonStats, "Generate Alkane DM Ion Stats data visualizations"]
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
		self.VisualizeAlkDMIonStats(combined_df)
		
	def VisualizeAlkDMIonStats(self, df):
		plot_builder = Plotter()
		plot_builder.AlkDMIonStatsPlot(df)
		
	def getAlkaneGOIonStats(self):
		alk_GO_ion_stats = self.db.AlkGOIonStats()
		self.printDataStructure(alk_GO_ion_stats)
	
	def getOFNIonStats(self):
		ofn_ion_stats = self.db.OFNIonStats()
		self.printDataStructure(ofn_ion_stats)
	
	def getOFNLinearityData(self):
		ofn_linearity_df = self.db.OFNLinearityData()
		self.printDataStructure(ofn_linearity_df)
		
	def getOFNSensitivityData(self):
		ofn_sensitivity_df = self.db.OFNSensitivityData_20fg()
		self.printDataStructure(ofn_sensitivity_df)
	
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

# create a new controls object and prompt the user with the welcome message
# then populate with the program instructions			
app = Controls()
app.giveUserFeedback('\n\n\n\n\n\nWelcome to DetectorStressAnalyzer\nBelow is the list of commands.')
app.basicInstructions()

# enter while loop that accepts user input until the user
# "exits" the program which will cause the run() method to return false
while(app.run()):
	app.runUserCommand("Enter Command")

	
	
	
	
	
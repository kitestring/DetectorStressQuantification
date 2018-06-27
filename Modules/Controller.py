from csvExtracter import Extract
from CLI import ConcoleGUI 
from SQL import Postgres
import credentials
import os
import pprint
import pandas as pd

class Controls():
	
	def __init__(self):
		self.cli = ConcoleGUI()
		self.csvDirectory = os.getcwd()
		self.db = Postgres(credentials.userinfo['db'], credentials.userinfo['id'], credentials.userinfo['pw'])
		
		
		self.commandDict = {
			'help': [self.basicInstructions, "Lists each command."],
			'exit': [self.exitProgram, "Exits the program."],
			'mine': [self.extractData, 'Mines the data from all the csv files found in the\n\t\tcurrent working directory.']
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

		csvExtractor = Extract(self.csvDirectory)
		DF_Dict, DataSet_id = csvExtractor.extract_csv_data()
		
		# DataSetCheck
		
		# If DataSet check pass then upload data
# 		x = DF_Dict['IDL'][['IDL','Concentration']].iloc[0].tolist()
# 		print(self.db.UploadTableRow_ReturnSerialID('IDL', x, 'IDL_id'))

# 		self.db.IsMethodUnique('GC', '1D OFN - 250-1 Split')
# 		self.db.IsMethodUnique('MS', '1D OFN +250 Volts')
		
		
# 		x = self.db.DataUploadTest()
# 		print(x*10)
		
# 		self.db.QueryTest()

		print(DF_Dict['GC'])
		
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

	
	
	
	
	
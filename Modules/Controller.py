from CLI import ConcoleGUI 
import os

class Controls():
	
	def __init__(self):
		self.cli = ConcoleGUI()
		runDescription = '''Generates a plot that shows the total number of hours for\n\t\ta given category\\sub-category per 7 day period.'''
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
		pass
		
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
app.giveUserFeedback('\n\n\n\n\n\nWelcome to SciData\nBelow is the list of commands.')
app.basicInstructions()

# enter while loop that accepts user input until the user
# "exits" the program which will cause the run() method to return false
while(app.run()):
	app.runUserCommand("Enter Command")

	
	
	
	
	
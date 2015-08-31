import sqlite3
import os, time

class SQLiteDBManager(object):
	"""A class which is meant to be a front end to the database for smartsubmit.
	The manager handles two tables. First, SampleFiles, which stores information
	on where the sample files are stored. Second, Disks, which stores information
	about which disks are available for storing the sample files."""

	def __new__(self, arg):
		"""This method makes sure we're sent a valid connection before we construct the object"""
		if isinstance(arg, sqlite3.Connection):
			return super(SQLiteDBManager, self).__new__(self)
		else:
			print("You have not provided a valid sqlite3 connection, construction of object aborted.")
			return None

	def __init__(self, connection, databasePath, workingDir):
		self.connection=connection
		self.cursor = connection.cursor()
		self.dbpath = databasePath

		if not workingDir[-1:] == '/': #add trailing / to path if needed
			workingDir+='/'
		
		self.workingDir = workingDir

	def __repr__(self):
		self.cursor.execute("SELECT * FROM sqLite_master where type='table'")
		tableNames = [ x[2] for x in self.cursor.fetchall()] #Third var in the master_table is tbl_name
		output = "List of Tables: %s" % tableNames
		return output

	def addSampleFileToTable(self, sample, localPath, hadoopPath, machine, IOSlotID):
		
		if not isinstance(sample, basestring):
			print("Expecting a string for the sample name")
			return None
		if not isinstance(sample, basestring):
			print("Expecting a string for the local path")
			return None
		if not isinstance(sample, basestring):
			print("Expecting a string for the Hadoop path" )
			return None
		if not isinstance(sample, basestring):
			print("Expecting a string for the machine")
			return None
		if not isinstance(sample, basestring):
			print("Expecting a string for the IOSlot Identifier")
			return None

		query = "INSERT INTO SampleFiles VALUES('%s', '%s', '%s', '%s', '%s')" % (sample, hadoopPath, localPath, IOSlotID, machine)
		
		if self.cursor.execute(query):
			return self.cursor.fetchall()
		else:
			print("There was an error adding the sample: %s to the database." % hadoopPath)
			return None

	def absorbDir(self, hadoopPath):
		
		if not hadoopPath[-1:] == '/': #add trailing / to path if needed
			hadoopPath+='/'

		try: #Read the directory
			fileList = os.listdir(hadoopPath) 
		except FileNotFoundError:
			print("Could not open directory %s, check that the path is typed correctly." % hadoopPath)


		#Construct list of samples that have ".root" in their name
		toBeAbsorbed=[sampleName for sampleName in fileList if ".root" in sampleName]
		
		print("Here's the list of samples that will go in the database")
		for infile in toBeAbsorbed:
			#Move each file to new locations. 

	def updateDirs(self):
		#compute the list of dictionaries
	
	def makeNewDatabase(self):
		"""This method creates a connection to a new sqlite3 database, named based on 
		the time it was created. The old database file is not effected in any way, but will
		no longer be used to manage the files."""

		newName = time.strftime("database_generated_at_%m_%d_%Y_%H_%M_%S")
		newName += ".db"

		print("Generating new database at %s%s" %(self.workingDir, newName))

		self.dbPath = self.workingDir+newName
		self.connection=sqlite3.connect(self.dbPath)

		updateDirectoryTable()
		makeSampleTable()
	def makeSampleTable(self):
		"""Creates the table SampleFiles, this method should not be used often, it is mainly
		here to define the schema for the table as well as help with debugging."""
		try:
			self.cursor.execute("CREATE TABLE SampleFiles(Sample varchar(200), HadoopPath varchar(500), LocalPath varchar(500), CondorID varchar(50), Machine varchar(100));")
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error creating the table: %s" err)
			return False

	def dropSamples(self):
		self.cursor.execute("DROP * FROM SampleFiles")
		return self.cursor.fetchall()

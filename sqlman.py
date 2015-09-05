import sqlite3
import os, time

class sqlman(object):
	"""SQLite Database Manager: A class which is meant to be a front end to the database for smartsubmit. The manager handles two tables. First, SampleFiles, which stores information on where the sample files are stored. Second, Disks, which stores information about which disks are available for storing the sample files."""

	def __new__(self, connection, databasePath, workingDir):
		"""This method makes sure we're sent a valid connection before we construct the object"""
		if isinstance(connection, sqlite3.Connection):
			return super(sqlman, self).__new__(self)
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
		"""Returns the list of tables"""
		self.cursor.execute("SELECT * FROM sqLite_master where type='table'")
		tableNames = [ x[2] for x in self.cursor.fetchall()] #Third var in the master_table is tbl_name
		output = "List of Tables: %s" % tableNames
		return output

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
			pass
			#Move each file to new locations. 

	def x(self, SQLCommand):
		"""proxy to self.cursor.execute(SQLCommand), returns the output of self.cursor.fetchall() on success, None on failure."""
		try:
			self.cursor.execute(SQLCommand)
			self.connection.commit()
			return self.cursor.fetchall()

		except sqlite3.OperationalError as err:
			print("The command: %s could not be executed. \n Caught error: %s" % (SQLCommand, err))

	def makeNewDatabase(self):
		"""This method creates a connection to a new sqlite3 database, named based on the time it was created. The old database file is not effected in any way, but will no longer be used to manage the files."""

		newName = time.strftime("database_generated_at_%m_%d_%Y_%H_%M_%S")
		newName += ".db"

		print("Generating new database at %s%s" %(self.workingDir, newName))

		self.dbPath = self.workingDir+newName
		self.connection=sqlite3.connect(self.dbPath)
		self.cursor = self.connection.cursor()

	def makeSampleTable(self):
		"""Creates the table SampleFiles, this method should not be used often, it is mainly here to define the schema for the table as well as help with debugging."""
		try:
			self.cursor.execute("CREATE TABLE SampleFiles(Sample_ID INTEGER PRIMARY KEY AUTOINCREMENT, Sample varchar(200), LocalPath varchar(500), HadoopPath varchar(500), CondorID varchar(50), Machine varchar(100), Disk);")
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error creating the table: %s" % err)
			return False

	def makeDiskTable(self):
		"""Creates the table SampleFiles, this method should not be used often, it is mainly here to define the schema for the table as well as help with debugging."""
		try:
			self.cursor.execute("CREATE TABLE Disks(Disk_ID INTEGER PRIMARY KEY AUTOINCREMENT, LocalPath varchar(500), Machine varchar(100), Working Boolean);")
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error creating the table: %s" % err)
			return False

	def removeSample(self, hadoopPath):
		"""Removes the row from the Disks table corresponding to machine:path and commits the change to the file."""
		try:
			self.cursor.execute( "DELETE FROM Disks WHERE HadoopPath='%s'" % hadoopPath)
			self.connection.commit()
		except sqlite3.OperationalError as err:
			print("There was an error removing the row: %s" % err)

	def removeDisk(self, path, machine):
		"""Removes the row from the Disks table corresponding to machine:path and commits the change to the file."""
		try:
			self.cursor.execute( "DELETE FROM Disks WHERE LocalPath='%s' AND Machine='%s'" % (path, machine) )
			self.connection.commit()
		except sqlite3.OperationalError as err:
			print("There was an error removing the row: %s" % err)

	def dropSamples(self):
		"""Runs SQL command to drop the SampleFiles table and then commits the change to the DB."""
		try:
			self.cursor.execute("DROP TABLE SampleFiles")
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error dropping the table: %s" % err )
			return None

	def dropDisks(self):
		"""Runs SQL command to drop the Disks table and then commits the change to the DB."""
		try:
			self.cursor.execute("DROP TABLE Disks")
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error dropping the table: %s" % err )
			return None

	def addSampleFile(self, sample, localPath, hadoopPath, machine, IOSlotID):

		query = "INSERT INTO SampleFiles VALUES('%s', '%s', '%s', '%s', '%s')" % (sample, localPath, hadoopPath, IOSlotID, machine)
		
		try:
			self.cursor.execute(query)
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error adding the sample: %s to the database.\n %s" % (hadoopPath, err))
			return None

	def addDisk(self, path, machine, working=1):
		"""Removes the row from the Disks table corresponding to """
		
		#Make sure working is Boolean since SQLite doesn't throw an error if you enter any int into a boolean slot.
		if not working == 1 and not working == 0:
			print("working must be a boolean argument, aborting insert...")
			return None

		try:
			self.cursor.execute( "INSERT INTO Disks(LocalPath, Machine, Working) VALUES('%s', '%s', '%i') " % (path, machine, working) )
			self.connection.commit()
		except sqlite3.OperationalError as err:
			print("There was an error removing the row: %s" % err)

	def listSamples(self, PRINT_OUT=False):
		"""Lists unique sample names and the file count in the SampleFiles table"""
		list_of_samples = self.x("SELECT Sample, Count(LocalPath) FROM SampleFiles Group By Sample")

		if PRINT_OUT:
			for sampleName, count in list_of_samples:
				#count = self.x("Count(*) SELECT * from ")
				print("Sample: %s, Number of Files: %i" % (sampleName, count)) 


		return list_of_samples

	def listDisks(self, PRINT_OUT=False, SELECT_WORKING=True):
		"Returns a python list of the disks available for storing files. If PRINT_OUT is true, print the list of machine directories with the"
		
		command = "SELECT * FROM Disks"
		if SELECT_WORKING:
			command+=" WHERE Working=1"

		list_of_dirs = self.x(command)

		if PRINT_OUT:
			for (path, machine, working) in list_of_dirs:
				print("%s:%s" % (machine, path))

		return list_of_dirs




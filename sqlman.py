import sqlite3
import os, time

class sqlman(object):
	"""SQLite Database Manager: A class which is meant to be a front end to the database for smartsubmit. The manager handles two tables. First, SampleFiles, which stores information on where the sample files are stored. Second, Disks, which stores information about which disks are available for storing the sample files."""

	def __new__(self, connection, databasePath, workingDir='.'):
		"""This method makes sure we're sent a valid connection before we construct the object"""
		if isinstance(connection, sqlite3.Connection):
			return super(sqlman, self).__new__(self)
		else:
			print("You have not provided a valid sqlite3 connection, construction of object aborted.")
			return None

	def __init__(self, connection, databasePath, workingDir='.'):
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

	def __getitem__(self, key):
		try:
			output = self.x("Select * From %s" % key)
			return [list(row) for row in output]
		except sqlite3.OperationalError as err:
			print("%s does not exist as a table" % key)

	def x(self, SQLCommand):
		"""Proxy to self.cursor.execute(SQLCommand), returns the output of self.cursor.fetchall() on success, None on failure. This method runs a sqlf.connection.commit() after executing the command, so don't use it to make changes to the database unless you want them written to disk."""
		try:
			self.cursor.execute(SQLCommand)
			self.connection.commit()
			return self.cursor.fetchall()

		except sqlite3.OperationalError as err:
			print("The command: %s could not be executed. \nCaught error: %s" % (SQLCommand, err))
			raise err

	def makeNewDatabase(self):
		"""This method creates a connection to a new sqlite3 database, named based on the time it was created. The old database file is not effected in any way, but will no longer be used to manage the files."""

		newName = time.strftime("database_generated_at_%m_%d_%Y_%H_%M_%S")
		newName += ".db"

		print("Generating new database at %s%s" %(self.workingDir, newName))

		self.dbPath = self.workingDir+newName
		self.connection=sqlite3.connect(self.dbPath)
		self.cursor = self.connection.cursor()

		self.makeSampleTable()
		self.makeDiskTable()

	def makeSampleTable(self):
		"""Creates the table SampleFiles, this method should not be used often, it is mainly here to define the schema for the table as well as help with debugging."""
		try:
			self.cursor.execute("""CREATE TABLE SampleFiles (
								    Sample_ID      INTEGER       PRIMARY KEY AUTOINCREMENT,
								    Sample         VARCHAR (200) NOT NULL,
								    LocalDirectory VARCHAR (500 NOT NULL,
								    FileName       VARCHAR (100) NOT NULL,
								    HadoopPath     VARCHAR (500) NOT NULL,
								    CondorID       VARCHAR (50) NOT NULL,
								    Machine        VARCHAR (100) NOT NULL,
								    User           VARCHAR (100) NOT NULL,
									    Disk_ID        INTEGER NOT NULL,
								    FOREIGN KEY (
								        Disk_ID
								    )
								    REFERENCES Disks (Disk_ID) 
								);""")

			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error creating the table: %s" % err)
			return False

	def makeDiskTable(self):
		"""Creates the table SampleFiles, this method should not be used often, it is mainly here to define the schema for the table as well as help with debugging."""
		try:
			self.cursor.execute("""CREATE TABLE Disks (
								    Disk_ID   INTEGER       PRIMARY KEY AUTOINCREMENT,
								    LocalDirectory VARCHAR (500) NOT NULL,
								    Machine   VARCHAR (100) NOT NULL,
								    CondorID  VARCHAR (25) NOT NULL,
								    Working   BOOLEAN
								);""")
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error creating the table: %s" % err)
			return False

	def getOwner(self, hadoopDirectory, filename):
		"""Gets the username of the user who inserted the sample file"""

		return self.x("SELECT user FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoopDirectory, filename) )[0][0]

	def getFilesOnDisk(self, dir, machine):
		return self.x("SELECT LocalDirectory, FileName FROM SampleFiles WHERE Disk_ID = (SELECT Disk_ID From Disks WHERE LocalDirectory='%s' AND Machine='%s')" % (dir, machine))

	def removeSample(self, hadoopDirectory, filename):
		"""Removes the row from the Disks table corresponding to machine:path and commits the change to the file."""
		try:
			self.cursor.execute( "DELETE FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoopDirectory, filename))
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

	def addSampleFile(self, sample, filename, localPath, hadoopDirectory, machine, user):
		"""Adds sample file to the SampleFiles table. Sample is the name of the sample set, localPath is the folder containing the file on the IOSlot slave, hadoopDirectory is the location of the file in Hadoop, machine is the domain name of the slave, and condorID is the condor identifier for the slot associated with the disk."""
		
		if not localPath[-1:] == '/': #add trailing / to path if needed
			localPath+='/'
		if not hadoopDirectory[-1:] == '/': #add trailing / to path if needed
			hadoopDirectory+='/'


		query = """INSERT INTO 
				SampleFiles(Sample, 
					LocalDirectory, 
					HadoopPath, 
					Filename, 
					CondorID, 
					Machine, 
					Disk_ID, 
					User) 
				VALUES( '%s', 
						'%s',
						'%s',
						'%s',
						(SELECT CondorID 
							FROM Disks 
							WHERE Machine='%s' 
							AND LocalDirectory='%s'),
						'%s',
						(SELECT Disk_ID 
							FROM Disks 
							WHERE Machine='%s' 
							AND LocalDirectory='%s'),
						'%s')""" % (sample, localPath+sample+'/', hadoopDirectory, filename, machine, localPath, machine, machine, localPath, user)
		
		try:
			return self.x(query)
		except sqlite3.OperationalError as err:
			print("There was an error adding the sample: %s to the database.\n %s" % (hadoopDirectory, err))
			return None

	def addDisk(self, path, machine, condorID, working=1):
		"""Adds disk to the Disks table"""
		
		if not path[-1:] == '/': #add trailing / to path if needed
			path+='/'

		#Make sure working is Boolean since SQLite doesn't throw an error if you enter any int into a boolean slot.
		if not working == 1 and not working == 0:
			print("working must be a boolean argument, aborting insert...")
			return None

		try:
			self.cursor.execute( "INSERT INTO Disks(LocalPath, Machine, CondorID, Working) VALUES('%s', '%s', '%s', '%i') " % (path, machine, condorID, working) )
			self.connection.commit()
		except sqlite3.OperationalError as err:
			print("There was an error adding the row: %s" % err)

	def listSamples(self, PRINT_OUT=False):
		"""Lists unique sample names and the file count in the SampleFiles table"""
		list_of_samples = self.x("SELECT Sample, Count(LocalDirectory) FROM SampleFiles Group By Sample")

		if PRINT_OUT:
			for sampleName, count in list_of_samples:
				#count = self.x("Count(*) SELECT * from ")
				print("Sample: %s, Number of Files: %i" % (sampleName, count)) 


		return list_of_samples

	def listFiles(self, VERBOSE=False):
		"""Returns a human readable list of the SampleFiles directory"""
		pass

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

	def getDict(self):
		return {"Disks":self.__getitem__("Disks"), "SampleFiles":self.__getitem__("SampleFiles")}


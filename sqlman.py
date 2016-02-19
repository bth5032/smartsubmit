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
								    LocalDirectory VARCHAR (500) NOT NULL,
								    Filesize          INTEGER NOT NULL,
								    FileName       VARCHAR (100) NOT NULL,
								    HadoopPath     VARCHAR (500) NOT NULL,
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
			self.cursor.execute("""CREATE TABLE "Disks" (
									`Disk_ID`	INTEGER PRIMARY KEY AUTOINCREMENT,
									`LocalDirectory`	varchar(500) NOT NULL,
									`Machine`	varchar(100) NOT NULL,
									`DiskNum`	INTEGER NOT NULL,
									`FreeSpace` INTEGER,
									`Working`	Boolean);""")
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error creating the table: %s" % err)
			return False

	def getOwner(self, hadoopDirectory, filename):
		"""Gets the username of the user who inserted the sample file"""

		return self.x("SELECT user FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoopDirectory, filename) )[0][0]

	def getFilesOnDisk(self, dir, machine):
		return self.x("SELECT LocalDirectory, FileName, Sample_ID FROM SampleFiles WHERE Disk_ID = (SELECT Disk_ID From Disks WHERE LocalDirectory='%s' AND Machine='%s')" % (dir, machine))

	def removeSample(self, hadoopDirectory, filename):
		"""Removes the row from the SampleFiles table corresponding to the hadoop path"""
		try:
			self.cursor.execute( "DELETE FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoopDirectory, filename))
			self.connection.commit()
			return True
		except sqlite3.OperationalError as err:
			print("There was an error removing the row: %s" % err)
			return False

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

	def diskInfo(self, disk_id=None, machine=None, directory=None):
		"""Returns a dictionary with keys Machine, LocalDirectory, FreeSpace, Working, Disk_ID for disk with given ID or machine directory pair. On error returns an error message"""
		if disk_id:
			try:
				row = self.man.x("SELECT Machine, LocalDirectory, FreeSpace, Working, Disk_ID FROM Disks WHERE Disk_ID = '%s'" % str(disk_id))[0]
				return {"Machine": row[0], "LocalDirectory": row[1], "FreeSpace": row[2], "Working": row[3], "Disk_ID": row[4] }
			except sqlite3.OperationalError as err:
				message= "There was an error looking up the data: %s" % err
				return(message)

		elif (machine and directory):
			try:
				row = self.man.x("SELECT Machine, LocalDirectory, FreeSpace, Working, Disk_ID FROM Disks WHERE Machine = '%s' AND LocalDirectory='%s' " % (machine, directory))[0]
				return {"Machine": row[0], "LocalDirectory": row[1], "FreeSpace": row[2], "Working": row[3], "Disk_ID": row[4] }
			except sqlite3.OperationalError as err:
				message= "There was an error looking up the data: %s" % err
				return(message)
		else:
			return "Can not look up disk info without an id or a machine directory pair. Please specify one of these."

	def updateSampleName(self, newSample, hadoopDirectory, filename):
		"""Allows the user to update the sample name of a file."""
		query = """UPDATE SampleFiles 
				SET 
					sample='%s' 
				WHERE 
					FileName='%s' 
				AND 
					HadoopPath='%s';""" % (newSample, filename, hadoopDirectory)
		try:
			self.x(query)
			return True
		except sqlite3.OperationalError as err:
			message = "There was an error adding the sample: %s to the database.\n %s" % (hadoopDirectory, err)
			print(message)
			return message

	def addSampleFile(self, sample, filename, localPath, hadoopDirectory, machine, user, fsize):
		"""Adds sample file to the SampleFiles table. Sample is the name of the sample set, localPath is the folder containing the file on the IOSlot slave, hadoopDirectory is the location of the file in Hadoop, and machine is the domain name of the slave."""
		
		if not localPath[-1:] == '/': #add trailing / to path if needed
			localPath+='/'
		if not hadoopDirectory[-1:] == '/': #add trailing / to path if needed
			hadoopDirectory+='/'


		query = """INSERT INTO 
				SampleFiles(Sample, 
					LocalDirectory, 
					HadoopPath, 
					Filename,
					Filesize,
					Machine, 
					Disk_ID, 
					User) 
				VALUES( '%s', 
						'%s',
						'%s',
						'%s',
						'%i',
						'%s',
						(SELECT Disk_ID 
							FROM Disks 
							WHERE Machine='%s' 
							AND LocalDirectory='%s'),
						'%s')""" % (sample, localPath+sample+'/', hadoopDirectory, filename, fsize, machine, machine, localPath, user)
		
		try:
			self.x(query)
			return True
		except sqlite3.OperationalError as err:
			message = "There was an error adding the sample: %s to the database.\n %s" % (hadoopDirectory, err)
			print(message)
			return message

	def addDisk(self, path, machine, free_space, working=1):
		"""Adds disk to the Disks table"""
		
		if not path[-1:] == '/': #add trailing / to path if needed
			path+='/'

		#Make sure working is Boolean since SQLite doesn't throw an error if you enter any int into a boolean slot.
		if not working == 1 and not working == 0:
			print("working must be a boolean argument, aborting insert...")
			return None

		try:
			self.cursor.execute( "INSERT INTO Disks(LocalPath, Machine, FreeSpace, Working) VALUES('%s', '%s', %i, '%i') " % (path, machine, free_space, working) )
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

	def getFilesInSample(self, sample_name):
		try:
			rows = self.x("SELECT LocalDirectory, HadoopPath, FileName, Machine FROM SampleFiles WHERE Sample = '%s'" % sample_name)
			return [{"LocalDirectory": y[0], "HadoopPath": y[1], "FileName": y[2], "Machine": y[3] } for y in rows]
		except sqlite3.OperationalError as err:
			print("There was an error getting the files from the table: %s" % err )
			return None

	def getDict(self):
		return {"Disks":self.__getitem__("Disks"), "SampleFiles":self.__getitem__("SampleFiles")}

	def working(self, diskID):
		"""Returns True if the disk with the given ID is tagged as working, False otherwise"""
		if self.x("Select Working From Disks Where Disk_ID='%s'" % str(diskID))[0][0] == 0:
			return False
		else:
			return True

	def updateDiskSpace(self, free_space, machine, disk):
		"""Runs SQL command to update the FreeSpace column in the Disks table for the disk specified by machine/disk."""
		try:
			self.cursor.execute("UPDATE Disks SET FreeSpace=%i WHERE Machine='%s' AND LocalDirectory='%s'" %(free_space, machine, disk))
			self.connection.commit()
			return self.cursor.fetchall()
		except sqlite3.OperationalError as err:
			print("There was an error updating the table: %s" % err )
			return None

	def getNumDisks(self):
		try:
			self.cursor.execute("SELECT Count(*) FROM Disks")
			self.connection.commit()
			return self.cursor.fetchall()[0][0]
		except sqlite3.OperationalError as err:
			print("There was an error reading from the table: %s" % err )
			return 0
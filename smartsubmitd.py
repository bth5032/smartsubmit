import sqlite3
import os

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

	def __init__(self, connection):
		self.connection=connection
		self.cursor = connection.cursor()

	def __repr__(self):
		self.cursor.execute("SELECT * FROM sqLite_master where type='table'")
		tableNames = [ x[2] for x in self.cursor.fetchall()]
		output = "List of Tables: %s" % tableNames
		return output

	def addSampleToDB(self, sample, localPath, hadoopPath, machine, IOSlotID, connection):
		cursor=connection.cursor()
		
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
		
		if cursor.execute(query):
			return True
		else:
			print("There was an error adding the sample: %s to the database." % hadoopPath)
			return None

	def absorbDirectory(self, hadoopPath):
		try:
			fileList = os.listdir(hadoopPath)
		except FileNotFoundError:
			print("Could not open directory %s, check that the path is typed correctly." % hadoopPath)
		toBeAbsorbed=[sampleName for sampleName in fileList if ".root" in sample] #Make a list of files that have ".root" in the name...
		
		print("Here's the list of samples that would go in the database")
		for infile in toBeAbsorbed:
			print(infile)

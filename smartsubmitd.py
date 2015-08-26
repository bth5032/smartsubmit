import sys

class SQLiteDBManager():
"""A class which is meant to be a front end to the database for smartsubmit.
	The manager handles two tables. First, SampleFiles, which stores information
	on where the sample files are stored. Second, Disks, which stores information
	about which disks are available for storing the sample files."""

	def __init__(self, DBPath):
		self.connection=sqlite3.connect(DBPath)
		self.cursor = connection.cursor()

	def __repr__(self):
		self.cursor.execute(".tables")
		output = "List of Tables: %s" self.cursor.fetchall()

	def addSampleToDB(self, sample, localPath, hadoopPath, machine, IOSlotID, connection):
		cursor=connection.cursor()
		return "Expecting a string for the sample name" if !isinstance(sample, basestring)
		return "Expecting a string for the local path" if !isinstance(sample, basestring)
		return "Expecting a string for the Hadoop path" if !isinstance(sample, basestring)
		return "Expecting a string for the machine" if !isinstance(sample, basestring)
		return "Expecting a string for the IOSlot Identifier" if !isinstance(sample, basestring)
		
		query = "INSERT INTO SampleFiles VALUES('%s', '%s', '%s', '%s', '%s')" % (sample, hadoopPath, localPath, IOSlotID, machine)
		
		if cursor.execute(query):
			return True
		else:
			print("There was an error adding the sample: %s to the database." % hadoopPath)
			return None

	def absorbDirectory(self, hadoopPath):
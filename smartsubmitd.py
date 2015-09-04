import sqlman, sqlite3, itertools


class DiskRing(object):
	"""Holds the list of disks available for new sample files. The list of directories is ordered by the method getBestDisk, which attempts to spread the sample files over disks and machines as much as possible.""" 
	def __init__(self, sample, ordered_list):
		self.sample = sample
		self.circle = itertools.cycle(ordered_list)
	
	def getNext(self):
		"""Return the best location for a new sample file"""
		return next(self.circle)

	def setName(self, name):
		"""Changes the sample name, this should only be called by checkIfComputed when it is changing the 'active' sample. If properly used, this should never be called without a setList call as well."""
		self.sample = name

	def setList(self, ordered_list):
		"""This method updates the list of directories. It should only be called when the sample has been changed by checkIfComputed."""
		self.circle = itertools.cycle(ordered_list)

database_file = "test.db"
working_dir = "."

connection = sqlite3.connect(database_file)
man = sqlman.sqlman(connection, database_file, working_dir)

def checkIfComputed(function):
	disks = DiskRing("A", [1,2,3])
	def return_func(sample_name):
		if disks.sample == sample_name:
			return disks.getNext()

		disks.setName(sample_name)
		disks.setList(function(sample_name))
		
		return disks.getNext()

	return return_func

def absorbDirectory(dir_path, sample_name):
	"""	for each root file in dir_path, 
	 	
		1. compute the proper location (machine, LocalPath combination) for the file
		2. move the file to the proper location
		3. absorb each file into the sql database. """
	pass

@checkIfComputed
def getBestDisk(sample_name):
	"""Generates a list of the possible locations for storing a sample file which is ordered by minimizing the following criteria (calling sample_name the "active sample"):	
	
	1. the number of active samples on the disk 
	2. the number of total samples on the disk
	3. the number of active samples on the machine 
	4. the number of total samples on the machine"""

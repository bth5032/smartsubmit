import sqlman, sqlite3, itertools


class DiskRing(object):

	def __init__(self, sample, ordered_list):
		self.sample = sample
		self.circle = itertools.cycle(ordered_list)
	
	def getNext(self):
		return self.circle.next()

	def setName(self, name):
		self.sample = name

	def setList(self, ordered_list):
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
def computeBestLocationForSample(sample_name):
	"""Determines the proper location, which is a localpath/machine combination, for the ntuple specified at ntuple_path. If the most recent file was an ntuple
	The code attempts to minimize the number of samples on the drive, first and foremost."""

	return [4,5,6]

	#Calling sample_name the "active sample" The first time this fucntion is called we generate a list of disks ordered by
	# 1. number of active samples are on the disk 
	# 2. the number of total samples 
	# 3. the number of active samples on the machine 
	# 4. the number of total samples on the machine
	#
	# in subsequent calls we instead return the next item on the list

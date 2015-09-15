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
		"""Checks if the sample_name was the last sample used, if so, it just returns the next drive on the list. Otherwise, construct a new list and then return the first drive"""
		if disks.sample == sample_name:
			return disks.getNext()

		disks.setName(sample_name)
		disks.setList(function(sample_name))
		
		return disks.getNext()

	return return_func

def absorbSampleFile(sample_name, file_path):
	"""Takes in a sample file and the name of the sample, computes the best location for that file using getBestDisk(), issues a command to move the file to the machine, then adds the file to the table."""

def absorbDirectory(dir_path, sample_name):
	"""For each root file in dir_path,
 	
 	1. check if file is already in database
	2. compute the proper location (machine, LocalPath combination) for the file
	3. move the file to the proper location
	4. absorb each file into the sql database."""
	pass

@checkIfComputed
def getBestDisk(sample_name):
	"""Generates a list of the possible locations for storing a sample file which is ordered by minimizing the following criteria (calling sample_name the "active sample"):	
	
	1. the number of active samples on the disk
	2. the number of total samples on the disk"""

	query = """Select 
					O.num_same, 
					Count(SampleFiles.Sample_ID) AS num_total,
					O.CondorID,
					O.Machine,
					O.LocalDirectory
				From 
					(SELECT 
						Count(Distinct S.Sample_ID) AS num_same,
						Disks.CondorID,
						Disks.Machine,
						Disks.LocalDirectory
					FROM 
						Disks
					LEFT JOIN 
						(SELECT 
							* 
						FROM 
							SampleFiles 
						WHERE 
							SampleFiles.Sample="%s") as S
					ON	
						S.Disk_ID=Disks.Disk_ID 
					GROUP BY
						Disks.Disk_ID
					ORDER BY 
						Count(S.Sample_ID) ASC) 
					AS O
				LEFT JOIN 
					SampleFiles 
				ON 
					SampleFiles.CondorID = O.CondorID 
				GROUP BY 
					O.CondorID 
				ORDER BY 
					num_same, 
					num_total ASC;""" % sample_name

	#The query above returns a list of the form 
	# [Number of Active Sample Files on The Disk, Total Number of Sample Files on the Disk, Condor ID for Disk, Machine Address, Directory on Machine Disk is Mounted]]

	output = man.x(query)
	
	#Return list of lists: [Condor ID, SSH Location ("Address:Directory")]

	return [ [x[2], str(x[3])+':'+str(x[4])] for x in output ]
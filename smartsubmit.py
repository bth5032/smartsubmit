import sqlman, sqlite3, itertools, os, subprocess, sys
from custom_errors import RsyncError



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

def checkUniqueList(function):
	def return_func(list_of_samples):
		"""Checks that the list is unique and then calls getMachineIDs. Otherwise returns False"""
		if len(set(list_of_samples)) == len(list_of_samples):
			return function(list_of_samples)
		else:
			return False

	return return_func

def moveRemoteFile(Machine, sample_dir, hadoop_path_to_file):
	"""Moves file at 'hadoop_path_to_file' to 'sample_dir' on remote machine 'Machine'. The current iteration of this method works by creating a pipe and forking an ssh call with an rsync command."""
	
	ssh_syntax = "ssh %s rsync --progress %s %s " % (Machine, hadoop_path_to_file, sample_dir)

	rsync = subprocess.Popen(ssh_syntax, stdout=sys.stdout, stderr=sys.stderr, shell=True)
	
	rsync.wait()

	exit_code = rsync.returncode

	if exit_code == 0:
		return True	
	else: 
		raise RsyncError(exit_code)
		return False

def sampleInTable(hadoop_path_to_file, sample_name):
	"""
	Returns: 
	0: 	if the sample file at the hadoop path is not in the table, 
	
	1: 	if the sample file is already in the table under the sample specified by 'sample_name', 

	<sample_name in table>: if the sample file is in the table, but under a different sample name."""

	hadoop_dir = os.path.dirname(hadoop_path_to_file)
	hadoop_dir += '/' #add trailing / to path
	filename = os.path.basename(hadoop_path_to_file)

	row = man.x("SELECT Sample FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoop_dir, filename))
	print(row)
	if len(row): #row[0] should be the only record if any is returned
		if row[0][0] == sample_name:
			return 1
		else:
			return row[0][0]
	return 0

def absorbSampleFile(sample_name, hadoop_path_to_file, Machine = None, LocalDirectory = None):
	"""
	1. Checks that the sample file is not already in the table

	2. Computes the best location for that file using getBestDisk()
	
	3. issues a command to move the file to the machine and adds the file to the table. 
	
	Mainly for testing purposes, you may specify the location where the file should land by setting Machine and LocalDirectory."""

	#Extract the directory and filename from hadoop_path_to_file
	hadoop_dir = os.path.dirname(hadoop_path_to_file)
	filename = os.path.basename(hadoop_path_to_file)
	hadoop_dir+='/' #add trailing / to path

	#Check if there is whitespace in the sample_name, this will cause errors with the scheme of putting sample files into /BASEDIR/sample_name

	if ' ' in sample_name:
		print("There can not be a space in the sample name")
		return False
 
	#Check if the record is already in the table. 
	#Having the same sample file twice will cause issues when we try to run the analysis on every file in a sample. 

	ret_code = sampleInTable(hadoop_path_to_file, sample_name)
	if not isinstance(ret_code, int):
		print("This sample file is already in the database, but under the sample name %s. The file will not be added until the old sample is removed." % ret_code)
		return False
	elif ret_code == 0:
		print("The sample is already in the table. The file will not be added again until the old sample is deleted")
		return False

	locationData = getBestDisk(sample_name) 
	
	if Machine == None:
		Machine = locationData["Machine"]
	if LocalDirectory == None:	
		LocalDirectory = locationData["LocalDirectory"]
	
	sample_dir = LocalDirectory+sample_name+"/" #Construct sample directory path

	if moveRemoteFile(Machine, sample_dir, hadoop_path_to_file):
		man.addSampleFile(sample_name, filename, LocalDirectory, hadoop_dir, Machine)
 
def absorbDirectory(dir_path, sample_name, NO_OVERWRITE=True):
	"""Calls absorbSampleFile for each root file in directory. If NO_OVERWRITE is true, files already in the table are skipped"""

	if not dir_path[:-1] == '/':
		dir_path += '/'

	for filename in os.listdir(dir_path):
		if filename[-5:] == ".root":
			hadoop_path_to_file=dir_path+filename
			if(not NO_OVERWRITE):
				absorbSampleFile(sample_name, hadoop_path_to_file)
			else:
				if not sampleInTable(hadoop_path_to_file):
					absorbSampleFile(sample_name, hadoop_path_to_file)

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
	
	#Return list of lists: [Condor ID, Machine, Local Directory]

	return [ {"CondorID": x[2], "Machine" : str(x[3]), "LocalDirectory" :str(x[4])} for x in output ]

@checkUniqueList
def getMachineIDs(list_of_samples):
	"""Takes in a list of sample files (specified by their sample ID) and returns a dictionary with the sample files as keys and CondorIDs as values"""

	return_dict = {}

	for sampleID in list_of_samples:
		man["SampleFiles"]

def IDsFromSampleName(sample_name):
	return [x[0] for x in man.x("SELECT Sample_ID FROM SampleFiles WHERE Sample='%s'" % sample_name)]

def makeCondorSubmitFile(path_to_template, sample_name, path_to_executable=None, path_to_proxy=None):
	"""Invokes a sed call to exchange tokens in the condor template file with the proper values."""
	list_of_disks = [ y[0] for y in man.x("SELECT CondorID FROM SampleFiles WHERE Sample='%s' GROUP BY CondorID" % sample_name)]

	if not list_of_disks:
		print("There were no files associated with this sample in the filesystem. Please check the name and try again")
		return False

	for condor_id in list_of_disks:
			list_of_files = [y[0]+y[1] for y in man.x("SELECT LocalDirectory, FileName FROM SampleFiles WHERE CondorID = '%s' AND Sample='%s'" % (condor_id, sample_name))]
			print(list_of_files)
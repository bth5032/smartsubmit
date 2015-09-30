import sqlman, sqlite3, itertools, os, subprocess, sys, zmq
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

def sampleInTable(hadoop_path_to_file, DELETE=False):
	"""Returns True if the sample file at the hadoop path is already in the table, False otherwise. If DELETE is True, it will also delete the record, but not the sample on disk. """

	row = man.x("SELECT * FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoop_dir, filename))

	if len(row): #row[0] should be the only record if any is returned
		if DELETE:
			man.x("DELETE FROM SampleFiles WHERE Sample_ID = '%i';" % row[0][0])
		return True

	return False

def absorbSampleFile(sample_name, hadoop_path_to_file, Machine = None, LocalDirectory = None):
	"""Takes in the hadoop path for a sample file and the sample name, computes the best location for that file using getBestDisk(), then issues a command to move the file to the machine and adds the file to the table. If the file already has a record in SampleFiles, the previous record is deleted, though the file will remain. Mainly for testing purposes, you may specify the location where the file should land by setting Machine and LocalDirectory."""

	#Extract the directory and filename from hadoop_path_to_file
	hadoop_dir = os.path.dirname(hadoop_path_to_file)

	if not hadoop_dir[-1:] == '/': #add trailing / to path if needed
		hadoop_dir+='/'

	filename = os.path.basename(hadoop_path_to_file)

	#Check if there is whitespace in the sample_name, this will cause errors with the scheme of putting sample files into /BASEDIR/sample_name

	if ' ' in sample_name:
		print("There can not be a space in the sample name")
		return False
 
	#Check if the record is already in the table. Having the same sample file twice will cause issues when we try to run the analysis on every file in a sample 

	if sampleInTable(hadoop_path_to_file, True):
		print("This sample file is already in the database, the old record was deleted")

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

### Server Part 

context = zmq.Context()
port="8000"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

message=socket.recv()
socket.send_string(str(getBestDisk("ttbar_powheg_pythia8_25ns")))
import sqlman, sqlite3, itertools, os, subprocess, sys, logging

database_file = "test.db"
working_dir = "."

connection = sqlite3.connect(database_file, check_same_thread=False)
man = sqlman.sqlman(connection, database_file, working_dir)

active_files = [] #holds a list of the files that are in the process of being added 
DISK_OVERHEAD=3000000000 #The extra space to require before we'll store a file on the disk.

def makeRemoteDir(machine, sample_dir):
	"""Makes sample_dir on remote machine if it's not already there"""

	ssh_syntax = "ssh %s ls -d %s" % (machine, sample_dir)
	
	logging.info("Checking for sample directory on %s:%s" %(machine, sample_dir))

	print("Checking for sample directory on %s:%s" %(machine, sample_dir))

	ls = subprocess.Popen(ssh_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	out=ls.stdout.readline().decode('UTF-8').rstrip('\n')

	if str(out) == sample_dir:
		print("Sample directory found.")
		logging.info("Sample directory found.")
		return True
	else:
		print("Sample directory not found, attempting to make it.")
		logging.info("Sample directory not found, attempting to make it.")
		mkdir = subprocess.Popen("ssh %s mkdir %s" % (machine, sample_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
		mkdir.wait()
		exit_code = mkdir.returncode
		if exit_code == 0:
			print("Sample directory succesfully made!")
			logging.info("Sample directory succesfully made!")
			return True
		else:
			print("Sample directory could not be made! Error:")
			logging.error("Sample directory could not be made! Error: %s" % str(exit_code))
			lines_iterator = iter(mkdir.stdout.readline, b"")
			for line in lines_iterator:
				print(line)
				logging.error(line)
			return False

def checkType(hdp_path):
	"""return dir or file depending on whether the object at hdp_path is a dir or file. False if the file does not exist. Used to make sure the user specifies the right path when they want to add a dir or file"""

	hdp_path = hdp_path[7:] if hdp_path[:7] == "/hadoop" else hdp_path #Strip off leading /hadoop

	test_syntax = "hdfs dfs -test -d %s" % hdp_path

	test = subprocess.Popen(test_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	
	out=test.communicate()

	exit_code = test.returncode

	if "No such file or directory" in out[0]:
		return False

	elif exit_code == 0:
		return "dir"

	elif exit_code == 1:
		return "file"

def moveRemoteFile(machine, sample_dir, hadoop_path_to_file, count=0):
	"""Moves file at 'hadoop_path_to_file' to 'sample_dir' on remote machine 'Machine'. The current iteration of this method works by creating a pipe and forking an ssh call with an hdfs dfs command.
	Returns: True if file is moved, a string with the error message if there was an error"""
	
	print("attempting to move remote file %s " % os.path.basename(hadoop_path_to_file))
	print("attempting to make remote directory...")

	if not makeRemoteDir(machine, sample_dir):
		message = "There was an error making the remote directory to house the ntuple '%s'. Please try the move again" % os.path.basename(hadoop_path_to_file)
		print(message)
		return message

	if hadoop_path_to_file[:7] == "/hadoop":
		path_in_hadoop = hadoop_path_to_file[7:]
	else:
		path_in_hadoop = hadoop_path_to_file

	filename = os.path.basename(hadoop_path_to_file)
	print("constructing ssh call...")
	ssh_syntax = "ssh %s \"rm %s%s 2>/dev/null; hdfs dfs -copyToLocal %s %s \"" % (machine, sample_dir, filename, path_in_hadoop, sample_dir)

	print("running move command...")
	move_command = subprocess.Popen(ssh_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	
	# --------Debug move command----------
	output = ""
	lines_iterator = iter(move_command.stdout.readline, b"")
	for line in lines_iterator:
		output += line.decode('UTF-8')+'\n'

	move_command.wait()

	exit_code = move_command.returncode

	if exit_code == 0:
		print("File: %s was moved succesfully! There is a copy at %s:%s" % (os.path.basename(hadoop_path_to_file), machine, sample_dir))
		return True	
	else:
		if count < 3:
			print("There was an error moving the file %s, will try again. Move command had output %s" % (hadoop_path_to_file, output))
			return moveRemoteFile(machine, sample_dir, hadoop_path_to_file, count+1)
		else:
			message = "There was an error moving the file %s. This was the final attempt, please try to add the file again later. Recieved Error:\n------\n%s" % (os.path.basename(hadoop_path_to_file), output)
			print(message)
			return message

def getFreeDiskSpace(machine, disk):
	"""Uses a remote ssh call to check the number of free bytes in the disk specified using df. Returns the number of bytes if the output is what is expected from the remote machine, otherwise returns None."""
	ssh_syntax="ssh %s \"df -B1 %s | tail -n1 | tr -s [:space:] '|' | cut -d '|' -f4\"" % (machine, disk)

	ssh = subprocess.Popen(ssh_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	
	out=ssh.communicate()

	exit_code = ssh.returncode
	try:
		free_space=int(out[0][:-1])
		return free_space
	except StandardError as err:
		print("There was an error checking the disk space for %s:%s\n%s" % (machine, disk, str(err)))
		return None

def checkDiskSpace(fsize, machine, disk):
	"""Determines whether there is enough disk space on machine:disk to store the file. Checks that free_space > DISK_OVERHEAD+fsize."""
	free_space = getFreeDiskSpace(machine, disk)
	
	if free_space:
		man.updateDiskSpace(free_space, machine, disk)
	else:
		return False

	if free_space > DISK_OVERHEAD+fsize:
		return True
	else:
		return False

def updateDiskSpace():
	"""Updates the free space on all the working disks."""

	for disk_info in man.listDisks():
		machine, disk = (disk_info[2], disk_info[1])
		free_space = getFreeDiskSpace(machine, disk)
		if free_space:
			man.updateDiskSpace(free_space, machine, disk)

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
	#print(row)
	if len(row): #row[0] should be the only record if any is returned
		if row[0][0] == sample_name:
			return 1
		else:
			return row[0][0]
	return 0

def absorbSampleFile(sample_name, hadoop_path_to_file, user, Machine = None, LocalDirectory = None):
	"""
	1. Checks that the sample file is not already in the table

	2. Computes the best location for that file using getBestDisk()
	
	3. issues a command to move the file to the machine and adds the file to the table. 
	
	Mainly for testing purposes, you may specify the location where the file should land by setting Machine and LocalDirectory.
	Returns: True if the file was absorbed succesfully, a string with the error message on error."""

	print("attempting to absorb sample file %s" % os.path.basename(hadoop_path_to_file))

	if hadoop_path_to_file in active_files:
		message = "The file %s is in the process of being added in another thread" % os.path.basename(hadoop_path_to_file)
		print(message)
		return message
	else:
		active_files.append(hadoop_path_to_file)

	#Extract the directory and filename from hadoop_path_to_file
	hadoop_dir = os.path.dirname(hadoop_path_to_file)
	filename = os.path.basename(hadoop_path_to_file)
	hadoop_dir+='/' #add trailing / to path

	if (hadoop_path_to_file[:7] == "/hadoop"):
		fsize=os.path.getsize(hadoop_path_to_file)
	else:
		fsize=os.path.getsize("/hadoop"+hadoop_path_to_file)
	#Check if there is whitespace in the sample_name, this will cause errors with the scheme of putting sample files into /BASEDIR/sample_name

	if ' ' in sample_name:
		message = "There can not be a space in the sample name"
		print(message)
		active_files.remove(hadoop_path_to_file)
		return message
	print("Checking that file exists and is not a directory...")
	#Check that the file exists:
	exists = checkType(hadoop_path_to_file)
	if exists == "dir":
		active_files.remove(hadoop_path_to_file)
		message = "The specified path '%s' points to a directory, please use add directory." % (hadoop_path_to_file)
		print(message)
		return message
	elif exists == False:
		active_files.remove(hadoop_path_to_file)
		message = "The specified path '%s' does not exist." % (hadoop_path_to_file)
		print(message)
		return message
 
	#Check if the record is already in the table. 
	#Having the same sample file twice will cause issues when we try to run the analysis on every file in a sample. 
	print("Checking if the file is already in the table...")
	ret_code = sampleInTable(hadoop_path_to_file, sample_name)
	if not isinstance(ret_code, int):
		message = "This sample file %s is already in the database, but under the sample name %s. The file will not be added unless the old file is removed." % (filename, ret_code)
		print(message)
		active_files.remove(hadoop_path_to_file)
		return message
	elif ret_code == 1:
		message = "The file %s is already in the table. The file will not be added again unless the old sample is deleted" % filename
		print(message)
		active_files.remove(hadoop_path_to_file)
		return message


	#Get best disk for the file
	print("Getting best disk to store the file...")

	tries=0	
	locationData = getBestDisk(sample_name, fsize)

	#Check that disk has enough space.
	while (Machine == None and LocalDirectory == None) and tries<len(locationData):
		print("Checking if the disk has enough space...") 
		try:
			space_check = checkDiskSpace(fsize, locationData[tries]["Machine"], locationData[tries]["LocalDirectory"])
		except StandardError as err:
			space_check = False
			message="There was an error when trying to check for free space on %s:%s\n%s" (locationData[tries]["Machine"], locationData[tries]["LocalDirectory"], repr(err))
			print(message)
			logging.error(message)
		
		if space_check:
			Machine = locationData[tries]["Machine"]
			LocalDirectory = locationData[tries]["LocalDirectory"]
			print("Found a good disk %s:%s has enough space." % (Machine, LocalDirectory))
		else:
			print("Could not verify there was enough space on %s:%s." % (locationData[tries]["Machine"], locationData[tries]["LocalDirectory"]))
			tries+=1

	#If machine and local dir were not set then no disk had enough space.
	if Machine==None or LocalDirectory==None:
		message="Not enough space on any drive for %s in sample %s" % (filename, sample_name)
		active_files.remove(hadoop_path_to_file)
		print(message)
		logging.warning(message)
		return message
		
	sample_dir = LocalDirectory+sample_name+"/" #Construct sample directory path
	print("Attempting to move remote file...")
	try:
		status = moveRemoteFile(Machine, sample_dir, hadoop_path_to_file)
	except StandardError as err:
		print("Recieved an error while trying to move the file \n%s" % str(err))
	if status == True:
		status = man.addSampleFile(sample_name, filename, LocalDirectory, hadoop_dir, Machine, user, fsize)
		if status == True:
			print("The Sample file %s has been succesfully absorbed." % filename)
			logging.info("The Sample file %s has been succesfully absorbed." % filename)
			active_files.remove(hadoop_path_to_file)
			return True
		else:
			message = "There was an error adding the sample file to the table\n------\n%s" % status
			print(message)
			logging.error(message)
			active_files.remove(hadoop_path_to_file)
			return message
	else:
		message = "There was an error moving the file %s into the system. \n------\n" % (filename, status)
		print(message)
		logging.error(message)
		active_files.remove(hadoop_path_to_file)
		return message
 
def listdir(path):
	"""Lists the contents of files in a hadoop directory"""

	hdp_path = path[7:] if path[:7] == "/hadoop" else path #Strip off leading /hadoop

	ls_syntax = "hdfs dfs -ls %s | sed 's,.*/\\(.*\\)$,\\1,g'" % hdp_path

	ls = subprocess.Popen(ls_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	
	out=ls.communicate()

	return out[0].split('\n')[1:-1]

def absorbDirectory(sample_name, dir_path, user):
	"""Calls absorbSampleFile for each root file in directory."""
	errors = "" #Flag for whether the directory was added succesfully

	print("Checking that %s is a directory" % dir_path)
	loc_type = checkType(dir_path) #Check if specified path points to a directory

	if loc_type == "dir":
		print("Verified...")

		if not dir_path[:-1] == '/':
			dir_path += '/'

		print("Enumerating files for absorption")
		for filename in listdir(dir_path):
			if filename[-5:] == ".root":
				print("found file %s" % filename)
				hadoop_path_to_file=dir_path+filename
				status = absorbSampleFile(sample_name, hadoop_path_to_file, user)
				if not status == True:
					errors += status+'\n'
				else:
					print("succesfully added file, checking for more")

		if not errors:
			print("Directory succesfully added")
		else:
			message = "There were some errors in adding the directory to the database: \n------\n%s" % errors
			logging.error(message)
			print(message)
	else:
		print("The path specified, '%s', is not a valid directory. Recieved %s from checktype, should get 'dir' for directory." % (dir_path, str(loc_type)))
		logging.info("The user %s tried to add a non valid directory '%s'" % (user, dir_path))

def dropSample(sample_name):
	"""Calls deleteSampleFile for each file in the sample specified."""
	flist = man.getFilesInSample(sample_name)

	for sinfo in flist:
		fname = sinfo["FileName"]
		hdir = sinfo["HadoopPath"]
		fdir = sinfo["LocalDirectory"]
		machine = sinfo["Machine"]
		
		if not hdir[-1] == '/':
			hdir+='/'
		
		ret=deleteSampleFile(hdir+fname)

		if ret==True:
			print("File %s in %s removed from the system" % (fname, sample_name))
		else:
			print("There were some errors removing %s from in %s from the system. Output:\n%s" % (fname, sample_name, ret))

def getBestDisk(sample_name, fsize):
	"""Generates a list of the possible locations for storing a sample file which is ordered by minimizing the following criteria (calling sample_name the "active sample"):	
	
	1. the number of active samples on the disk
	2. the number of total samples on the disk"""

	#This query is really horrible. 
	query = """Select * From
				(Select 
					O.num_same, 
					Count(SampleFiles.Sample_ID) AS num_total,
					O.Disk_ID,
					O.LocalDirectory,
					O.Machine,
					O.DiskNum
				From 
					(SELECT 
						Count(Distinct S.Sample_ID) AS num_same,
						Disks.Disk_ID,
						Disks.LocalDirectory,
						Disks.Machine,
						Disks.DiskNum
					FROM 
						Disks
					LEFT JOIN 
						(SELECT 
							* 
						FROM 
							SampleFiles 
						WHERE 
							SampleFiles.Sample="%s") AS S
					ON	
						S.Disk_ID=Disks.Disk_ID
					WHERE
						Disks.Working='1'
					AND
						Disks.FreeSpace > %i 
					GROUP BY
						Disks.Disk_ID
					ORDER BY 
						Count(S.Sample_ID) ASC) AS O
				LEFT JOIN 
					SampleFiles 
				ON 
					SampleFiles.Disk_ID = O.Disk_ID 
				GROUP BY 
					O.Disk_ID) AS P
					LEFT JOIN
					(SELECT 
						Machine, 
						Count(Machine) as total_on_machine 
					FROM 
						SampleFiles 
					GROUP BY 
						Machine) AS K 
					ON 
						P.Machine = K.Machine
					ORDER BY 
						P.num_same, 
						P.num_total ASC,
						K.total_on_machine ASC,
						P.DiskNum;""" % (sample_name, DISK_OVERHEAD+fsize)

	#The query above returns a list of the form 
	# [Number of Active Sample Files on The Disk, Total Number of Sample Files on the Disk, Condor ID for Disk, Machine Address, Directory on Machine Disk is Mounted]]

	output = man.x(query)
	#Return list of lists: [Condor ID, Machine, Local Directory]

	return [ {"Machine" : str(x[4]), "LocalDirectory" :str(x[3])} for x in output ]

def computeJob(sample_name, user):
	"""Takes in a sample name and returns a list of the (disk, machine) pairs where the samples are stored"""
	logging.info("Computing condor jobs for sample '%s'" % sample_name)
	print("Computing condor jobs for sample '%s'" % sample_name)

	list_of_disks = [ [ y[0], y[1], y[2] ] for y in man.x("SELECT Machine, LocalDirectory, Disk_ID FROM SampleFiles WHERE Sample='%s' GROUP BY Disk_ID" % sample_name)]

	if not list_of_disks:
		print("There were no files associated with the sample '%s' in the filesystem. Please check the name and try again" % sample_name)
		logging.error("There were no files associated with the sample '%s' in the filesystem. Please check the name and try again" % sample_name)
		return False

	list_of_jobs = [] # Should be one for each disk

	for (machine, path, disk_id) in list_of_disks:
			
			#check that disk is working
			if not man.working(disk_id):
				print("Disk with ID %s contains files in the sample, but is tagged as broken" % disk_id)
				continue

			#get list of files on that drive
			list_of_files = [ y[0]+y[1] for y in man.x("SELECT LocalDirectory, FileName FROM SampleFiles WHERE Disk_ID = '%s' AND Sample='%s'" % (disk_id, sample_name))]
			
			#add drive info and list of files on that drive to our job list
			list_of_jobs.append([machine, path, list_of_files])


			print("A condor job has been enumerated for the user '%s'. The disk '%s' holds files\n '%s' \n -----------------\n" % (user, machine+':'+path, str(list_of_files)) )
			logging.info("A condor job has been enumerated for the user '%s'. The disk '%s' holds files\n '%s' \n -----------------\n" % (user, machine+':'+path, str(list_of_files)))
	
	if list_of_jobs:
		return list_of_jobs
	else:
		return False

def deleteSampleFile(hadoop_path_to_file, LAZY=False):
	"""Removes sample file from the SampleFiles table, if LAZY is false, also send a remote command to remove the file from the remote directory. If the file is removed, returns True, otherwise returns an error message."""

	hadoop_dir = os.path.dirname(hadoop_path_to_file)+'/'
	filename = os.path.basename(hadoop_path_to_file)

	message=""
	success=True

	if not LAZY:
		message += "Attempting to delete the file from the remote machine\n"

		(machine, local_dir, disk_id, sample) = man.x("SELECT Machine, LocalDirectory, Disk_ID, Sample FROM SampleFiles WHERE HadoopPath='%s' AND FileName='%s'" % (hadoop_dir, filename))[0]

		if man.working(disk_id):
			ssh_syntax = "ssh %s rm %s " % (machine, local_dir+filename)

			rm = subprocess.Popen(ssh_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
			
			rm.wait()

			exit_code = rm.returncode

			if exit_code == 0:
				logging.info("The file %s in sample '%s' was succesfully deleted from the remote directory" % (filename, sample))
			else:
				message += "There was an error removing %s in sample '%s'. rm failed with exit code %s \n" % (filename, sample, str(exit_code))
				logging.error("There was an error removing %s in sample '%s'. rm failed with exit code %s" % (filename, sample, str(exit_code)))
				success=False
		else:
			disk_info = man.diskInfo(disk_id=disk_id)
			logging.info("Will not attempt to remove file %s in sample %s from %s:%s because it is tagged as not working." % (filename, sample, disk_info["Machine"], disk_info["LocalDirectory"]))
		

	sql_success = man.removeSample(hadoop_dir, filename) 
	if sql_success and success:
		return True
	elif sql_success:
		return message
	else:
		message+="There was an error removing the file from the database."
		return message

def checkDisk(dir, machine):
	"""Attempts to read all files on a disk, returns true if the reads were succesful. If no files on disk, it writes and reads an empty file. Returns output of ssh on failure"""
	files_on_disk = man.getFilesOnDisk(dir, machine)
	log = logging.getLogger("diskCheckHelper")

	if files_on_disk:
		for file_info in files_on_disk:
			path_to_file = file_info[0]+file_info[1]

			ssh_syntax = "ssh %s \"head -c 1024 %s && tail -c 1024 %s\"" % (machine, path_to_file, path_to_file)			
			
			read_bytes = subprocess.Popen(ssh_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
			read_bytes.wait()

			log.debug("Reading file '%s' on '%s'" % (path_to_file, machine))
			
			output = read_bytes.communicate()[0]
			if not read_bytes.returncode == 0:
				log.error("The disk '%s:%s' may have gone down. Output of read command: \n-----\n%s" % (machine,dir,output))

				return output
		return True
	else:
		ssh_syntax = "ssh %s \"touch %stestfile && cat %stestfile\"" %(machine, dir, dir)
		ssh = subprocess.Popen(ssh_syntax, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
		
		ssh.wait()
		
		log.debug("touching file '%stestfile' on '%s'" % (dir, machine))

		output = ssh.communicate()[0]

		if ssh.returncode == 0:
			return True
		else:
			log.error("The disk '%s:%s' may have gone down. Output of read command: \n-----\n%s" % (machine,dir,output))

			return output

def moveToWorkingDisk(machine, local_dir, filename, sample_id):
	"""removes the file specified and then adds it to the table again"""
	(sample, hadoop_dir, user) = man.x("SELECT Sample, HadoopPath, User FROM SampleFiles WHERE Sample_ID='%d'" % sample_id)

	hdp_path = hadoop_dir+filename 
	deleteSampleFile(hdp_path, "smartsubmit", LAZY=True)
	absorbSampleFile(sample, hdp_path, user) 

def badDisk(dirname, machine):
	"""Used to correct the database in the case of a bad disk"""
	man.x("UPDATE Disks SET Working='0' WHERE LocalDirectory='%s' AND Machine='%s'" % (dirname, machine))
	files_on_disk = man.getFilesOnDisk(dirname, machine)
	
	for file_info in files_on_disk:
		moveToWorkingDisk(machine, file_info[0], file_info[1], file_info[2])

def updateSampleName(hdp_path, new_name):
	"""Updates the sample name of a file in the table"""
	filename = os.path.basename(hdp_path)
	dirname = os.path.dirname(hdp_path)+'/'
	output = man.updateSampleName(new_name, dirname, filename)
	if output == True:
		message = "The sample name of the file %s was updates succesfully" % filename 
		logging.info(message)
		print(message)
		return message
	else:
		message = "There was an error updating the sample name\n------\n%s" % output
		logging.error(message)
		print(message)
		return message

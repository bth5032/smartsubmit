#!/usr/bin/python

import zmq, argparse, os, sys, subprocess, pwd
from ss_com import SmartSubmitCommand
from prettytable import PrettyTable

def makeDirs(list_of_samples, parent_dir):
	"""Makes the log directories for the run job command.
	====Args====
	parent_dir: path to the directory that stores the log file tree"""
	
	try:
		if not os.path.isdir(parent_dir):
			os.mkdir(parent_dir)
		for sample in list_of_samples:
			if not os.path.isdir("%s/%s" % (parent_dir, sample)):
				os.mkdir("%s/%s" % (parent_dir, sample))
		return True
	except Exception as err:
		print(err)
		return False

def buildCommand(args):
	comDict = {}

	if args.absorb_sample:
		if args.sample:
			if args.directory:
				comDict["command"] = "add directory"
				comDict["path_to_directory"] = args.directory
			elif args.file:
				comDict["command"] = "add file"
				comDict["path_to_file"] = args.file
			else:
				print("You must specify a file or directory to absorb with -f or -d")
				return ""

			comDict["sample"] = args.sample[0]
			if len(args.sample) > 1:
				print("There should only be one sample name specified")
				return ""
		else:
			print("You must specify at least one sample name")
			return ""	

	elif args.delete_sample:
		comDict["command"] = "delete file"
		if args.file:
			comDict["path_to_file"] = args.file
		else:
			print("You must specify a hadoop path to the file you want to delete.")
			return ""

	elif args.run_job:
		comDict["command"] = "run job"
		#Check for template
		if args.template:
			comDict["path_to_template"] = args.template
		else:
			comDict["path_to_template"] = "./condorFileTemplate"
		#Check for executable
		if args.executable:
			comDict["path_to_executable"] = args.executable
			if args.sample:
				comDict["samples"] = args.sample
			else:
				print("You must specify a sample to run over")
				return ""	
		else:
			print("You must specify an executable to run")
			return ""
		
		#Set log directory, remove trailing slash if present
		comDict["log_dir"] = args.log if not args.log[-1:] == "/" else args.log[:-1] 
	
	elif args.list_samples:
		comDict["command"] = "list sample files"

	elif args.check_job:
		comDict["command"] = "check job"
		try:
			comDict["jid"] = int(args.check_job)
		except:
			print("You must submit an integer greater than 0 as the job id")
			return ""

	elif args.update_file_sample:
		comDict["command"] = "update file sample"
		comDict["new_name"] = args.update_file_sample
		if args.file:
			comDict["file"] = args.file
		else:
			print("You must specify the hadoop path to the file whose sample name you want changed with -f")
			return ""

	#elif args.report_bad_disk:
	#	print("This functionality is stil in development")
	#	return ""
	else:
		return ""

	comDict["user"]=pwd.getpwuid(os.geteuid()).pw_name

	return SmartSubmitCommand(comDict)

def condorSubmit(job_info, sample, log_dir):
	"""Makes a temporary condor submit file using sed to replace tokens in the template file, then calls condor_submit on the processed submit file"""
	
	path_to_executable = command.exe_path
	disk=job_info[1].split('/')[1]
	path_to_template = command.temp_path
	machine = job_info[0]
	list_of_files = job_info[2]
	
	space_seperated_list_of_files = " ".join(list_of_files)

	sed_command = "sed -e 's,\$\$__EXECUTABLE__\$\$,%s,g;s,\$\$__PATH_TO_SAMPLE__\$\$,%s,g;s,\$\$__LOG_DIR__\$\$,%s,g;s,\$\$__DISK__\$\$,%s,g;s,\$\$__MACHINE__\$\$,%s,g;s,\$\$__SAMPLE__\$\$,%s,g' < %s > condor_submit.tmp" %(path_to_executable, space_seperated_list_of_files, log_dir, disk, machine, sample, path_to_template)
	#print("running sed command %s" % sed_command)

	sed = subprocess.Popen(sed_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	
	sed.wait()

	exit_code = sed.returncode

	if not exit_code == 0: #Break if sed error
		print("There was an error creating the submit file from the template. sed quit with error code %s. Will not attempt to submit job for %s" % (str(exit_code), space_seperated_list_of_files))
		return exit_code

	condor_submit_command = "condor_submit condor_submit.tmp"

	condor_submit = subprocess.Popen(condor_submit_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	condor_submit.wait()

	exit_code = condor_submit.returncode

	if not exit_code == 0:
		print(condor_submit.communicate()[1])
	else:
		print("Job Queued")

def sendCommand(command_obj):
	# Set up connection
	# -----------------------------------------------------------------------
	port="7584"
	context = zmq.Context()
	socket = context.socket(zmq.REQ)
	#socket.connect("tcp://127.0.0.1:%s" % port)
	socket.connect("tcp://smartsubmit.t2.ucsd.edu:%s" % port)
	
	# Send command to the server
	# -----------------------------------------------------------------------
	socket.send_pyobj(command_obj, protocol=2)

	return socket.recv_pyobj()

def printSampleFiles(slist, view="Default"):
	"""Takes in the list of the sample files table from the server and pretty prints it to the screen."""

	if view=="Default":
		samples= {}
		stripped_list = [x[1] for x in slist]
		for x in stripped_list:
			if x in samples:
				samples[x] += 1
			else:
				samples[x] = 1

		t=PrettyTable(["Sample Name", "Num Files"])		
		for x in sorted(samples.keys()):
			t.add_row([x, samples[x]])

	elif view=="Less":
		t=PrettyTable(["Sample Name", "File Name", "Owner"])
		stripped_list = [[x[1], x[3], x[8]] for x in slist]
		sorted_list = sorted(stripped_list, key=lambda x: x[0])
		last_sample = sorted_list[0][0]
		for x in sorted_list:
			if not x[0] == last_sample:
				t.add_row(["-----------", "-----------", "-----------"])
				last_sample = x[0]
			t.add_row(x)

	elif view=="More":
		title=["Sample Name", "Local Directory", "Filename", "Machine", "Owner"]
		t=PrettyTable(title)
		stripped_list = [[x[1], x[2], x[3], x[5], x[8]] for x in slist]
		sorted_list = sorted(stripped_list, key=lambda x: x[0])
		last_sample = sorted_list[0][0]
		for x in sorted_list:
			if not x[0] == last_sample:
				t.add_row(["-----------", "-----------", "-----------", "-----------", "-----------"])
				last_sample = x[0]
			t.add_row(x)

	elif view=="Even More":
		title=["Sample Name", "Local Directory", "Filename", "Hadoop Directory", "Machine", "Owner"]
		t=PrettyTable(title)
		stripped_list = [[x[1], x[2], x[3], x[4], x[5], x[8]] for x in slist]
		sorted_list = sorted(stripped_list, key=lambda x: x[0])
		last_sample = sorted_list[0][0]
		for x in sorted_list:
			if not x[0] == last_sample:
				t.add_row(["-----------", "-----------", "-----------", "-----------", "-----------", "-----------"])
				last_sample = x[0]
			t.add_row(x)

	elif view=="All":
		title=["File ID", "Sample Name", "Local Directory", "Filename", "Hadoop Directory", "Condor ID", "Machine", "Disk ID", "Owner"]
		t=PrettyTable(title)
		sorted_list = sorted(slist, key=lambda x: x[1])
		last_sample = sorted_list[0][1]
		for x in sorted_list:
			if not x[1] == last_sample:
				t.add_row(["-----------", "-----------", "-----------", "-----------", "-----------", "-----------", "-----------", "-----------", "-----------"])
				last_sample = x[1]
			t.add_row(x)

	print(t)	

def processSample(list_of_jobs, sample, log_dir):
	for job in list_of_jobs:
		condorSubmit(job, sample, log_dir)
# ------------------------------------------------------
# Start Main 
# ------------------------------------------------------

# Parse Command Line Options
# ------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument( "--absorb_sample", help="absorb a sample file or directory, must be used with either -d or -f and -s", action="store_true")
parser.add_argument("--delete_sample", help="delete a sample file from the filesystem, must be used with -f", action="store_true")
parser.add_argument("--run_job", help="run analysis on sample, must be used with -e and -s, may run over multiple samples by using multiple -s flags", action="store_true")
parser.add_argument("-f", "--file", help="specify file to add or remove", metavar="PATH_TO_FILE_ON_HADOOP")
parser.add_argument("-d", "--directory", help="specify directory to absorb", metavar="PATH_TO_DIR_ON_HADOOP")
parser.add_argument("-s", "--sample", help="specify the sample name", action="append", metavar="SAMPLE_NAME")
parser.add_argument("-e", "--executable", help="specify the path to the executable which will run on the specified samples. Used with --run_jobs")
parser.add_argument("-t", "--template", help="specify the location of the condor submit file, optional argument used with --run_job; default is ./condorFileTemplate", metavar="PATH_TO_TEMPLATE_FILE")
parser.add_argument("--list_samples", help="List the samples in the database with along with their owner.", action="store_true")
parser.add_argument("-v","--view", help="Select how much information to display on each sample file (a number between 0 and 3), used with --list_samples.")
parser.add_argument("-l", "--log", help="Choose the path the directory which stores the log files, used only with --run_jobs. If no directory given the logs will be stored in $PWD/logs/", metavar="PATH_TO_LOG_FILE", default="logs/")
#parser.add_argument("--report_bad_disk", help="Used when a file could not be read by smartsubmit")
parser.add_argument("-c", "--check_job", help="Check on a job with the given job ID. Only used to check the status of file absorbsion.", metavar="JOB_ID")
parser.add_argument("--update_file_sample", help="Choose a new name for the sample file specified by -f [hadoop_path]", metavar="NEW_SAMPLE_NAME")

arguments=parser.parse_args()

# Construct the command to send to the server.
# --------------------------------------------------------------------
command = buildCommand(arguments)


#Handle Command Feedback
if command: 
	reply = sendCommand(command)
	
	if command.command =="list sample files":
		if arguments.view:
			if arguments.view == "0":
				printSampleFiles(reply)
			elif arguments.view == "1":
				printSampleFiles(reply, "Less")
			elif arguments.view == "2":
				printSampleFiles(reply, "More")
			elif arguments.view == "3":
				printSampleFiles(reply, "Even More")
			elif arguments.view == "4":
				printSampleFiles(reply, "All")
			else:
				print("unrecognized view code %s, please select from 0,1,2,3,4. Showing default view:" % arguments.view)
				printSampleFiles(reply)
		else:
			printSampleFiles(reply)

	elif command.command == "run job":
		if makeDirs(command.samples, command.log_dir):
			for sample in command.samples:
				if reply[sample] == False:
					print("There are no files on working disks that are associated with sample '%s'. If you are sure the files are on the disks(use --list_samples), either remove the old files and add them again, or wait for the disk to come back up." % sample)
				else:
					print("---------")
					print("Sample: %s" % sample)
					print("---------")
					processSample(reply[sample], sample, command.log_dir)
					print("\n\n\n")
		else:
			print("Could not make log directories, please check that you have write permissions to the working directory specified: %s" % command.log_dir)

	elif command.command == "delete file":
		print(reply)	

	elif command.command == "add file":
		#reply should be message with the job id
		print("---------------")
		print(reply)
		print("---------------")

	elif command.command == "add directory":
		#reply should be message with the job id
		print("---------------")
		print(reply)
		print("---------------")

	elif command.command == "check job":
		#reply is the output thus far.
		print("---------------")
		print(reply)
		print("---------------")

	elif command.command == "update file sample":
		print("---------------")
		print(reply)
		print("---------------")
else: #the user messed up if empty
	parser.print_help()


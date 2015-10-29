import zmq, argparse, os, sys, subprocess
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
			if not os.path.isdir("logs/%s" % sample):
				os.mkdir("logs/%s" % sample)
		return True
	except Exception as err:
		print(err)
		return False

def buildCommand(args):
	command = ""
	comDict = {}

	if args.absorb_sample:
		command+="absorb sample "
		if args.sample:
			if args.directory:
				command+="directory "+args.directory+" "
				comDict["command"] = "add directory"
				comDict["path_to_directory"] = args.directory
			elif args.file:
				command+="file "+args.file+" "
				comDict["command"] = "add file"
				comDict["path_to_file"] = args.file
			else:
				print("You must specify a file or directory to absorb with -f or -d")
				return ""

			command+=args.sample[0] #There should not be more than one sample for absorbsion
			comDict["sample"] = args.sample[0]
			if len(args.sample) > 1:
				print("There should only be one sample name specified")
				return ""
		else:
			print("You must specify at least one sample name")
			return ""	

	elif args.delete_sample:
		command+="delete sample file "
		comDict["command"] = "delete file"
		if args.file:
			command+=args.file+" "
			comDict["path_to_file"] = args.file
		else:
			print("You must specify a hadoop path to the file you want to delete.")
			return ""

	elif args.run_job:
		command+="run job "
		comDict["command"] = "run job"
		if args.template:
			command+=args.template+" "
			comDict["path_to_template"] = args.template
		else:
			command+="./condorFileTemplate "
			comDict["path_to_template"] = "./condorFileTemplate"

		if args.executable:
			command+=args.executable+" "
			comDict["path_to_executable"] = args.executable
			if args.sample:
				comDict["samples"] = args.sample
				for sample in args.sample:
					command+=sample+" "
			else:
				print("You must specify a sample to run over")
				return ""
		else:
			print("You must specify an executable to run")
			return ""
	
	elif args.list_samples:
		comDict["command"] = "list sample files"

	else:
		return ""
#==========NEED TO FIX THIS!!!============
	if os.getenv("SMARTSUBMIT_SPOOF_USERNAME"): #Here to support adding files to the DB by smartsubmit user when a file was on a bad disk
		comDict["user"]=os.getenv("SMARTSUBMIT_SPOOF_USERNAME")
	else:
		comDict["user"]=os.getenv("LOGNAME")

	return SmartSubmitCommand(comDict)

def condorSubmit(job_info, sample):
	"""Makes a temporary condor submit file using sed to replace tokens in the template file, then calls condor_submit on the processed submit file"""
	
	path_to_executable = command.exe_path
	disk=job_info[1].split('/')[1]
	path_to_template = command.temp_path
	machine = job_info[0]
	list_of_files = job_info[2]
	
	space_seperated_list_of_files = " ".join(list_of_files)

	sed_command = "sed -e 's,\$\$__EXECUTABLE__\$\$,%s,g;s,\$\$__PATH_TO_SAMPLE__\$\$,%s,g;s,\$\$__DISK__\$\$,%s,g;s,\$\$__MACHINE__\$\$,%s,g;s,\$\$__SAMPLE__\$\$,%s,g' < %s > condor_submit.tmp" %(path_to_executable, space_seperated_list_of_files, disk, machine, sample, path_to_template)
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
	socket.send_pyobj(command_obj)

	return socket.recv_pyobj()

def printSampleFiles(slist, view="Default"):
	"""Takes in the list of the sample files table from the server and pretty prints it to the screen."""

	if view=="Default":
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

def processSample(list_of_jobs, sample):
	for job in list_of_jobs:
		condorSubmit(job, sample)
# ------------------------------------------------------
# Start Main 
# ------------------------------------------------------

# Parse Command Line Options
# ------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument( "--absorb_sample", help="absorb a sample file or directory, must be used with either -d or -f and -s", action="store_true")
parser.add_argument("--delete_sample", help="delete a sample file from the filesystem, must be used with -f", action="store_true")
parser.add_argument("--run_job", help="run analysis on sample, must be used with -e and -s", action="store_true")
parser.add_argument("-f", "--file", help="specify file to add or remove")
parser.add_argument("-d", "--directory", help="specify directory to absorb")
parser.add_argument("-s", "--sample", help="specify the sample name", action="append")
parser.add_argument("-e", "--executable", help="specify the path to the executable which will run on the specified samples. Used with --run_jobs")
parser.add_argument("-o", "--output", help="specify the directory for the file which will contain output from smartsubmit, default is the working directory")
parser.add_argument("-t", "--template", help="specify the location of the condor submit file, optional argument used with --run_job; default is ./condorFileTemplate")
parser.add_argument("--list_samples", help="List the samples in the database with along with their owner.", action="store_true")
parser.add_argument("--view", help="Select how much information to display on each sample file (a number between 0 and 3), used with --list_samples.")
parser.add_argument("-l", "--log", help="Choose the path the directory which stores the log files, used only with --run_jobs. If no directory given the logs will be stored in $PWD/logs/")
parser.add_argument("--report_bad_disk". help="Used when a file could not be read by smartsubmit")
arguments=parser.parse_args()

# Construct the command to send to the server.
# --------------------------------------------------------------------
command = buildCommand(arguments)

if arguments.log:
	log_dir = arguments.log
else:
	log_dir = 'logs/'

if command: 
	reply = sendCommand(command)
	if command.command =="list sample files":
		if arguments.view:
			if arguments.view == "0":
				printSampleFiles(reply)
			elif arguments.view == "1":
				printSampleFiles(reply, "More")
			elif arguments.view == "2":
				printSampleFiles(reply, "Even More")
			elif arguments.view == "3":
				printSampleFiles(reply, "All")
			else:
				print("unrecognized view code %s, please select from 0,1,2,3. Showing default view:" % arguments.view)
				printSampleFiles(reply)
		else:
			printSampleFiles(reply)

	elif command.command == "run job":
		if makeDirs(command.samples, log_dir):
			for sample in command.samples:
				print("---------")
				print("Sample: %s" % sample)
				print("---------")
				processSample(reply[sample], sample)
				print("\n\n\n")
		else:
			print("Could not make log directories, please check that you have write permissions to the working directory specified: %s" % log_dir)

else: #the user messed up if empty
	parser.print_help()


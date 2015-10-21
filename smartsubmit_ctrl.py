import zmq, argparse, os, sys
from ss_com import SmartSubmitCommand

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
	
	else:
		return ""

	return SmartSubmitCommand(comDict)

def condorSubmit(command, path_to_template, path_to_executable, condor_id,list_of_files):
	"""Makes a temporary condor submit file using sed to replace tokens in the template file, then calls condor_submit on the processed submit file"""

	(machine, disk) = diskNameFromCondorID(condor_id).split(":")
	space_seperated_list_of_files = " ".join(list_of_files)

	sed_command = "sed -e 's,\$\$__EXECUTABLE__\$\$,%s,g;s,\$\$__PATH_TO_SAMPLE__\$\$,%s,g;s,\$\$__CONDOR_SLOT__\$\$,%s,g;s,\$\$__MACHINE__\$\$,%s,g;s,\$\$__CONDOR_ID__\$\$,%s,g' < %s > condor_submit.tmp" %(path_to_executable, space_seperated_list_of_files, condor_id, machine, condor_id, path_to_template)
	print("running sed command %s" % sed_command)

	sed = subprocess.Popen(sed_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	
	sed.wait()

	exit_code = sed.returncode

	if not exit_code == 0: #Break if sed error
		print("There was an error creating the submit file from the template. sed quit with error code %s. Will not attempt to submit job for %s" % (str(exit_code), str(diskNameFromCondorID(condor_id))) )
		return exit_code

	condor_submit_command = "condor_submit condor_submit.tmp"

	condor_submit = subprocess.Popen(condor_submit_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	condor_submit.wait()

	exit_code = condor_submit.returncode

	if not exit_code == 0:
		print(condor_submit.communicate())

def sendCommand(command_obj):
	# Set up connection
	# -----------------------------------------------------------------------
	port="7584"
	context = zmq.Context()
	socket = context.socket(zmq.REQ)
	socket.connect("tcp://127.0.0.1:%s" % port)

	# Send command to the server
	# -----------------------------------------------------------------------
	socket.send_pyobj(command_obj)
	message = socket.recv_string()
	print("Recieved reply: %s" % message)


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

arguments=parser.parse_args()

# Construct the command to send to the server.
# --------------------------------------------------------------------
command = buildCommand(arguments)

if command: 
	sendCommand(command)
else: #the user messed up if empty
	parser.print_help()



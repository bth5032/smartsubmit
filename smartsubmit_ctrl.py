import zmq, argparse, os, sys


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


args=parser.parse_args()

# Set up connection
port="7584"

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:%s" % port)


# Determine the directory for output files
# -------------------------------------------------------------------------

output_dir = os.getcwd()
output_dir+='/'

if args.output:
	output_dir = args.output

# Send Directory
socket.send_string(output_dir)
message = socket.recv_string()

print(message)


# Construct the command to send to the server.
# --------------------------------------------------------------------
command=""

if args.absorb_sample:
	command+="absorb sample "
	if args.sample:
		if args.directory:
			command+="directory "+args.directory+" "
		elif args.file:
			command+="file "+args.file+" "
		else:
			print("You must specify a file or directory to absorb with -f or -d")
			sys.exit(1)
		command+=args.sample[0] #There should not be more than one sample for absorbsion
		if len(args.sample) > 1:
			print("There should only be one sample name specified")
			sys.exit(1)
	else:
		print("You must specify at least one sample name")
		sys.exit(1)	

elif args.delete_sample:
	command+="delete sample file "
	if args.file:
		command+=args.file+" "
	else:
		print("You must specify a hadoop path to the file you want to delete.")
		sys.exit(1)

elif args.run_job:
	command+="run job "
	
	if args.template:
		command+=args.template+" "
	else:
		command+="./condorFileTemplate "

	if args.executable:
		command+=args.executable+" "
		if args.sample:
			for sample in args.sample:
				command+=sample+" "
		else:
			print("You must specify a sample to run over")
			sys.exit(1)
	else:
		print("You must specify an executable to run")
		sys.exit(1)

if command:
	print(command)
else:
	print(args.help)


# Send command to the server
# -----------------------------------------------------------------------
socket.send_string(command)
message = socket.recv_string()
print("Recieved reply: %s" % message)

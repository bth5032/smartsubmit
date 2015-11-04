import smartsubmit as ss
import thread_printing as tp
import zmq, time, threading, logging
from ss_com import SmartSubmitCommand

context = zmq.Context()
port="7584"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

start_time=time.strftime("%m-%d-%Y_%H:%M:%S")
logging.basicConfig(filename='smartsubmit_%s.log' % start_time, level=logging.DEBUG)
logging.info("smartsubmit started at %s" % start_time)

def addFile(sample, hdp_path, user):
	"""proxy to smartsubmit.absorbSampleFile, makes sure the file descriptor used for thread printing is closed"""
	ss.absorbSampleFile(sample, hdp_path, user)
	tp.closeThreadFile(threading.currentThread().name)

def checkOnJob(jobID):
	"""Reads in and returns the file contents of the output from job with the jobID"""

	reply = ""

	if jobID >= JID or jobID < 0:
		return "There is no job with the specified job ID"

	if job_files[jobID].closed:
		reply+="Job has finished with the output:\n"
		reply+="---------------\n\n"
	else:
		reply+="Job is still running with the output:\n"
		reply+="---------------\n\n"

	output = open(job_files[jobID].name, "r")
	reply+= output.read()
	output.close()

	return reply

JID=0 #Job ID
job_files = {}

while True:
	
	# Get output file
	
	command=socket.recv_pyobj()
	logging.info("recieved command: '%s' from user %s" % (command.command, command.user))

	#threadname=time.strftime("ss_output_for_job_at_%m-%d-%Y_%H:%M:%S")

	#working_dir="/tmp/"
	#outfile_name = working_dir+threadname 
	#outfile=open(outfile_name, "w+")
	#command.outfile = outfile
	#command.outfile_name = outfile_name


	if command.command == "add file":
		
		try:
			threadname=time.strftime("ss_%s" % JID +"_%m-%d-%Y_%H:%M:%S")
			
			working_dir="/tmp/"
			outfile_name = working_dir+threadname 
			outfile=open(outfile_name, "w+")
			job_files[JID] = outfile

			print("absorbing sample file '%s' under sample name '%s' for user'%s'" % (command.hdp_path, command.sample, command.user))
			tp.printer.add_thread(threadname, outfile)
			t=threading.Thread(name=threadname, target=addFile, args=(command.sample, command.hdp_path, command.user))
			tp.printer.add_thread(threadname, outfile)
			
			socket.send_pyobj("The file is being moved and added to the database, you can check the status by running ss_ctrl [-c, --check_job] %s" % str(JID))
			JID+=1
			
			t.start()
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_pyobj("Error parsing command '%s' " % message)

	elif command.command == "delete file":
		try:
			hadoop_path_to_file = command.hdp_path
			print("deleting sample file '%s'" % hadoop_path_to_file)
			message = ss.deleteSampleFile(hadoop_path_to_file, command.user)
			print(message)
			socket.send_pyobj(message)
		except Exception as err:
			print("Error: %s" % err)
			logging.error("There was an error running the delete %s" % err)
			print("There was an error parsing the command")
			socket.send_pyobj("Error parsing command '%s' " % str(command))

	elif command.command == "add directory":
		
		try:
			hadoop_path_to_dir = tokens[3]
			sample_name = tokens[4]
			print("absorbing directory '%s' under sample name '%s'" % (hadoop_path_to_dir, sample_name))
			tp.printer.add_thread(threadname, outfile)
			t=threading.Thread(name=threadname, target=ss.absorbDirectory, args=(hadoop_path_to_dir, sample_name))
			tp.printer.add_thread(threadname, outfile)
			t.start()
			socket.send_string("Creating sample '%s' from directory '%s'." % (sample_name,hadoop_path_to_dir))
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)
	
	elif command.command == "run job":
		ret = {}
		for sample_name in command.samples:
			ret[sample_name] = ss.computeJob(sample_name, command.user)
		
		socket.send_pyobj(ret)
	
	elif command.command == "list sample files":
		socket.send_pyobj(ss.man["SampleFiles"])
		
	elif command.command == "check job":
		output = checkOnJob(command.jid)
		socket.send_pyobj(output)
		
	else:
		logging.error("No action defined for '%s'" % command.command)
		print("""Error, no action defined for message '%s', allowable actions are:
	1. add file
	2. add directory
	3. delete file
	4. run job
	5. list sample files
	6. report bad disk
	7. check job""" % command.command)

	print(command)



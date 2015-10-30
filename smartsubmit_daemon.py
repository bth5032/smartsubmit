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

#threading.Thread(target="ss.main")

def processCommand(command):
	if command.command == "run job":
		runJob(command)
	elif command.command == "add file":
		addFile(command)
	elif command.command == "delete file":
		deleteFile(command)
	elif command.command == "add directory":
		addDir(command)
	elif command.command == "delete sample":
		deleteSample(command)

def checkOnJob(jobID):
	pass

while True:
	
	# Get output file
	
	command=socket.recv_pyobj()
	logging.info("recieved command: '%s' from user %s" % (command.command, command.user))

	threadname=time.strftime("ss_output_for_job_at_%m-%d-%Y_%H:%M:%S")

	working_dir="/tmp/"
	outfile_name = working_dir+threadname 
	outfile=open(outfile_name, "w+")
	command.outfile = outfile
	command.outfile_name = outfile_name


	if command.command == "add file":
		
		try:
			hadoop_path_to_file = tokens[3]
			sample_name = tokens[4]
			print("absorbing sample file '%s' under sample name '%s'" % (hadoop_path_to_file, sample_name))
			tp.printer.add_thread(threadname, outfile)
			t=threading.Thread(name=threadname, target=ss.absorbSampleFile, args=(sample_name, hadoop_path_to_file))
			tp.add_thread(threadname, outfile)
			socket.send_string("Absorbing Sample File '%s' into sample '%s'" %(hadoop_path_to_file, sample_name) )
			t.start()
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)

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
	
	elif command.command == "delete file":
		try:
			hadoop_path_to_file = tokens[3]
			print("deleting sample file '%s'" % hadoop_path_to_file)
			tp.printer.add_thread(threadname, outfile)
			t=threading.Thread(name=threadname, target=ss.deleteSampleFile, args=(hadoop_path_to_file,))
			tp.add_thread(threadname, outfile)
			t.start()
			socket.send_string("Deleting Sample File '%s'" % hadoop_path_to_file)
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
		

	else:
		logging.error("No action defined for '%s'" % command.command)
		print("""Error, no action defined for message '%s', allowable actions are:
	1. add file
	2. add directory
	3. delete file
	4. run job
	5. list sample files
	6. report bad disk""" % command.command)

	print(command)



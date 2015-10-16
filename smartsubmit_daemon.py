import smartsubmit as ss
import thread_printing as tp
import zmq, time, threading
from ss_com import SmartSubmitCommand

context = zmq.Context()
port="7584"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

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


while True:
	
	# Get output file
	
	working_dir=socket.recv_string()
	threadname=time.strftime("ss_output_for_job_at_%m-%d-%Y_%H:%M:%S")
	try: #Open the file they specified.
		outfile=open(working_dir+threadname, "w+")
		socket.send_string("File created succesfully, output from this command will be stored at %s" % working_dir+threadname)
	except:
		working_dir="/tmp/"
		outfile=open(working_dir+threadname, "w+")
		socket.send_string("Could not open file at the specified location, output from this command will be stored at %s" % "/tmp/"+threadname)

	command=socket.recv_pyobj()
	"""tokens=message.split(" ")

	# Get Command

	if message[:18] == "absorb sample file":
		
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

	elif message[:23] == "absorb sample directory":
		
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
	
	elif message[:18] == "delete sample file":
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
	
	elif message[:7] == "run job":
		try:
			path_to_template = tokens[2]
			path_to_exe = tokens[3]
			sample_name = tokens[4]
			tokens=list(filter(None, tokens)) #make sure all tokens are not empty
			
			print("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
			t=threading.Thread(name=threadname, target=ss.runJob, args=(path_to_exe, sample_name, path_to_template))
			tp.printer.add_thread(threadname, outfile)
			t.start()
			if len(tokens) > 5: #Running over multiple samples
				for x in range(5, len(tokens)):
					threadname=time.strftime("ss_output_for_job_at_%m-%d-%Y_%H:%M:%S")
					outfile = open(working_dir+threadname, "w+")
					sample_name=tokens[x]
					print("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
					t=threading.Thread(name=threadname, target=ss.runJob, args=(path_to_exe, sample_name, path_to_template))
					tp.printer.add_thread(threadname, outfile)
					t.start()
				socket.send_string("running executable '%s' on samples '%s'. \nNOTE: There were multiple samples provided, each will have it's own output file." % (path_to_exe,str(tokens[4:])))
			else:
				socket.send_string("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)
	
	else:
		socket.send_string(
Error, no action defined for message '%s', allowable actions are:
	1. absorb sample file <hadoop path to file> <sample name>
	2. absorb sample directory <hadoop path to directory> <sample name>
	3. delete sample file <hadoop path to file>
	4. run job <path to executable on network drive> <sample>
	 % message)"""

	print(command)
	socket.send_string("fuck!")



import smartsubmit as ss
import thread_printing as tp
import zmq, time, threading


context = zmq.Context()
port="7584"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

while True:
	
	# Get output file
	
	message=socket.recv_string()
	filename=time.strftime("ss_output_for_job_at_%m-%d-%Y_%H:%M:%S")
	try:
		outfile=open(message+filename, "w+")
		socket.send_string("File created succesfully, output from this command will be stored at %s" % message+filename)
	except:
		outfile=open("/tmp/"+filename, "w+")
		socket.send_string("Could not open file at the specified location, output from this command will be stored at %s" % "/tmp/"+filename)

	message=socket.recv_string()
	tokens=message.split(" ")

	# Get Command

	if message[:18] == "absorb sample file":
		
		try:
			hadoop_path_to_file = tokens[3]
			sample_name = tokens[4]
			print("absorbing sample file '%s' under sample name '%s'" % (hadoop_path_to_file, sample_name))
			#threading.Thread=ss.absorbSampleFile(sample_name, hadoop_path_to_file)
			socket.send_string("Absorbing Sample File '%s' into sample '%s'" %(hadoop_path_to_file, sample_name) )
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)

	elif message[:23] == "absorb sample directory":
		
		try:
			hadoop_path_to_dir = tokens[3]
			sample_name = tokens[4]
			print("absorbing directory '%s' under sample name '%s'" % (hadoop_path_to_dir, sample_name))
			socket.send_string("Creating sample '%s' from directory '%s' " % (sample_name,hadoop_path_to_dir))
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)
	
	elif message[:18] == "delete sample file":
		try:
			hadoop_path_to_file = tokens[3]
			print("deleting sample file '%s'" % hadoop_path_to_file)
			socket.send_string("Deleting Sample File '%s'" % hadoop_path_to_file)
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)
	
	elif message[:7] == "run job":
		try:
			path_to_exe = tokens[2]
			sample_name = tokens[3]
			tokens=list(filter(None, tokens))
			print("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
			if len(tokens) > 4:
				for x in range(4, len(tokens)):
					sample_name=tokens[x]
					if sample_name:
						print("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
				socket.send_string("running executable '%s' on samples '%s'" % (path_to_exe,str(tokens[3:])))
			else:
				socket.send_string("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
		except IndexError:
			print("error parsing command '%s'" % message)
			socket.send_string("Error parsing command '%s' " % message)
	
	else:
		socket.send_string("""
Error, no action defined for message '%s', allowable actions are:
	1. absorb sample file <hadoop path to file> <sample name>
	2. absorb sample directory <hadoop path to directory> <sample name>
	3. delete sample file <hadoop path to file>
	4. run job <path to executable on network drive> <sample>
""" % message)



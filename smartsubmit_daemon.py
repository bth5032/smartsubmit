import smartsubmit as ss
import zmq

context = zmq.Context()
port="7584"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

while True:
	message=socket.recv_string()
	tokens=message.split(" ")
	print(message[:18])

	if message[:18] == "absorb sample file":
		
		try:
			hadoop_path_to_file = tokens[3]
			sample_name = tokens[4]
			print("absorbing sample file '%s' under sample name '%s'" % (hadoop_path_to_file, sample_name))
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
			path_to_exe = tokens[3]
			sample_name = tokens[4]
			print("running executable '%s' on sample '%s'" % (path_to_exe, sample_name))
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


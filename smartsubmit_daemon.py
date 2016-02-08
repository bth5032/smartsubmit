import smartsubmit as ss
import thread_printing as tp
import zmq, time, threading, logging, os
from ss_com import SmartSubmitCommand
import getpass

## Email Stuff
import smtplib 
import email.mime.text as mt
import email.utils as eutils


#Set up global job tracking
JID=0 #Job ID
job_files = {}

start_time=time.strftime("%m-%d-%Y_%H:%M:%S")
logging.basicConfig(filename='smartsubmit_%s.log' % start_time, level=logging.DEBUG, format='%(asctime)s | %(message)s', datefmt='%m-%d-%Y %H:%M:%S')
logging.info("smartsubmit started at %s" % start_time)

log_disk_helper = logging.getLogger("diskCheckHelper")
log_disk_helper.setLevel(logging.DEBUG)
log_disk_helper.addHandler(logging.FileHandler("diskCheckHelper_%s.log" % start_time))
log_disk_helper.propagate=0


def emailAdmins(message_body):
	logging.info("Sending message to admins: \n------\n%s" % message_body)
	server = smtplib.SMTP('smtp.ucsd.edu', 587)
	server.starttls()
	server.login(username, password)
	msg = mt.MIMEText(message_body)
	
	if len(admins) > 1:
		msg["To"] = ", ".join([eutils.formataddr(x) for x in admins])
	else:
		msg["To"] = eutils.formataddr(admins[0])

	msg["From"] = eutils.formataddr(admins[0])
	server.sendmail(admins[0][1], [x[1] for x in admins], msg.as_string())
	server.quit()
	
def diskCheckHelper():
	"""Checks the disks every 6 hours, if a disk has gone bad, it will email everyone in the admins list defined above."""
	while True:
		message = ""
		#Run check disk for every disk in the table
		for (disk_id, directory, machine, disk_num, working) in ss.man["Disks"]:
			if working:
				result=ss.checkDisk(directory, machine)
				if not result == True:
					message += "The disk mounted at '%s' on '%s'  may have gone down.\n%s\n" % (directory, machine, result)
		if message:
			emailAdmins(message)
		time.sleep(60*60*6)

def addFile(sample, hdp_path, user):
	"""proxy to smartsubmit.absorbSampleFile, makes sure the file descriptor used for thread printing is closed"""
	try:
		ss.absorbSampleFile(sample, hdp_path, user)
	except Exception as err:
		print("There was an error adding the file '%s' into sample '%s'" % (os.path.basename(hdp_path), sample))
		print(err)
		logging.error("There was an error adding the file '%s' into sample '%s'" % (os.path.basename(hdp_path), sample))
		logging.exception(err)
	tp.closeThreadFile(threading.currentThread().name)

def addDirectory(sample, hdp_dir, user):
	"""proxy to smartsubmit.absorbDirectory, makes sure the file descriptor used for thread printing is closed"""
	try:
		ss.absorbDirectory(sample, hdp_dir, user)
	except Exception as err:
		print("There was an error adding the directory '%s' to the sample '%s'" % (hdp_dir, sample))
		print(err)
		logging.error("There was an error adding the directory '%s'" % hdp_dir)
		logging.exception(err)
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

def run_server():
	"""Runs the zeromq server and handles all the jobs"""
	#Connect to server
	
	global JID, job_files
	context = zmq.Context()
	port="7584"
	socket=context.socket(zmq.REP)
	socket.bind("tcp://*:%s" % port)

	while True:

		# Get command
		
		command=socket.recv_pyobj()
		logging.info("recieved command: %s from user %s" % (command.command, command.user))

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

				if (not command.user == "bhashemi"):
					#Only bhashemi can add files for now.
					logging.error("User %s tried to add a file, but only 'bhashemi' can add files." % command.user)
					socket.send_pyobj("Only user 'bhashemi' can add files.")
					continue

				print("absorbing sample file '%s' under sample name '%s' for user'%s'" % (command.hdp_path, command.sample, command.user))

				if ' ' in command.sample:
					message = "There can not be a space in the sample name '%s'" % command.sample
					print(message)
					logging.error(message)
					socket.send_pyobj(message)
				else:
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

				if (not command.user == "bhashemi"):
					#Only bhashemi can add files for now.
					logging.error("User %s tried to delete a file, but only 'bhashemi' can delete files." % command.user)
					socket.send_pyobj("Only user 'bhashemi' can delete files.")
					continue

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

				if (not command.user == "bhashemi"):
					#Only bhashemi can add files for now.
					logging.error("User %s tried to add a directory, but only 'bhashemi' can add directories." % command.user)
					socket.send_pyobj("Only user 'bhashemi' can add directories.")
					continue

				threadname=time.strftime("ss_%s" % JID +"_%m-%d-%Y_%H:%M:%S")
					
				working_dir="/tmp/"
				outfile_name = working_dir+threadname 
				outfile=open(outfile_name, "w+")
				job_files[JID] = outfile

				print("absorbing directory '%s' under sample name '%s' for user'%s'" % (command.dir, command.sample, command.user))
				logging.info("absorbing directory '%s' under sample name '%s' for user'%s'" % (command.dir, command.sample, command.user))

				if ' ' in command.sample:
					message = "There can not be a space in the sample name %s" % command.sample
					print(message)
					logging.error(message)
					socket.send_pyobj(message)
				else:
					tp.printer.add_thread(threadname, outfile)
					t=threading.Thread(name=threadname, target=addDirectory, args=(command.sample, command.dir, command.user))
					tp.printer.add_thread(threadname, outfile)
					
					socket.send_pyobj("The directory is being absorbed into the system and added to the database, you can check the status by running ss_ctrl [-c, --check_job] %s" % str(JID))
					JID+=1
					
					t.start()
			except Exception as err:
				print("error parsing command: \nadd directory: %s\nsample name: %s\nuser: %s" % (command.dir, command.sample, command.user))
				socket.send_pyobj("error parsing command: \nadd directory: %s\nsample name: %s\nuser: %s" % (command.dir, command.sample, command.user) )
		
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

		elif command.command == "update file sample":
			try:
				if (not command.user == "bhashemi"):
					#Only bhashemi can add files for now.
					logging.error("User %s tried to change sample names, but only 'bhashemi' can manage samples." % command.user)
					socket.send_pyobj("Only user 'bhashemi' can manage samples.")
					continue

				output = ss.updateSampleName(command.hdp_path, command.new_name)
				socket.send_pyobj(output)
			except Exception as err:
				socket.send_pyobj("There was an error\n------\n%s\n running the command %s" (err,str(command)))
			
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

def checkPass():
	server = smtplib.SMTP('smtp.ucsd.edu', 587)
	server.starttls()
	try:
		server.login(username, password)
		server.quit()
		print("Connected to mail server successfully with provided login info. Please press Ctrl+z to suspend this process to the background, then run bg to start the process again.")
		return True
	except Exception as err:
		server.quit()
		print("Could not connect to mail server, shutting down")
		return False


try:
	run_server()
except Exception as err:
	logging.exception(err)
	exit(1)
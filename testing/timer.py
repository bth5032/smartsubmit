#!/usr/bin/python

import subprocess as sp
import time, datetime, sys


def getClusterIds():
	"""Reads the standard input which should be filled with all the cluster ids for the jobs just submitted."""
	ids[]
	for line in sys.stdin:
		ids.append(line[:-1])
	return ids

def getStdoutFilename(jid):
	"""Reads the condor logs and returns a tuple of (stdout, stderr) the locations of the std output and std error files for the job with the specified job id"""

	stdout, stderr, outfile = ""

	cq = sp.Popen(["condor_history","-l",jid], stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')

	for l in lines:
		if l[:6] == "Out = ":
			stdout = l[6:]
		elif l[:6] == "Err = ":
			stderr=l[6:]
		elif l[:20] == "TransferOutputRemaps":
			outfile=l.split('"')[1].strip().split('=')[1]

	return (stdout, stderr, outfile)

def stdoutInfo(stdout, stderr):
	time_start, time_end, root_start, root_end, root_real, root_user, root_sys = ""

	with open(stdout) as f:
		for line in f:
			if not line:
				continue
		
			a = line.strip('\n').split(None, 1)
		
			if a[0] == "START_TIME:":
				time_start = a[1]
			elif a[0] == "END_TIME:":
				time_end = a[1]
			elif a[0] == "ROOT_START:":
				root_start = a[1]
			elif a[0] == "ROOT_END:":
				root_end = a[1]	

	with open(stderr) as f:
		for line in f:
			if not line:
				pass

			a=line.split()

			elif a[0] == "real":
				root_real = a[1]
			elif a[0] == "user":
				root_user = a[1]
			elif a[0] == "sys":
				root_sys = a[1]


	return (time_start, root_start, root_end, time_end, root_real, root_user, root_sys)

def running():
	"""Returns whether any condor jobs exist for bhashemi"""
	cq = sp.Popen("condor_q", stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with bhashemi, same as a grep call
	lines = filter(lambda x: "bhashemi" in x, lines)
	return bool(lines)

def wait():
	"""keeps running until all jobs are finished"""
	while running()
		time.sleep(300)

def main(start_time, procs):
	"""Main function: 
1. Wait for jobs to end
2. get stdout filenames from condor_history
3. get time_start, root_start, root_end, time_end from stdout files
4. get return file times (block until all files written to disk).
5. compute last time_end and last file_write times.
6. print out all that information.

start_time is the time the script started. procs is an empty dictionary with condor cluster_ids for keys."""

	wait() # (1)

	for jid in procs:
		procs[jid]= dict(zip(("stdout", "stderr", "outfile"), getStdoutFilename(key))) # (2)
		procs[jid]["time_start"], procs[jid]["root_start"], procs[jid]["root_end"], procs[jid]["time_end"], procs[jid]["root_real"], procs[jid]["root_user"], procs[jid]["root_sys"]  = stdoutInfo(procs[jid]["stdout"]) # (3)
		procs[jid]["return_file"]

	########################################
	# Print it all out #####################

	net_time = datetime.timedelta(seconds=(end-start))
	print("""Start:\t%s
End:\t%s
Net Time:\t%s
Last Job:\t%s
Last File:\t%s
================
JID\tStart\tRoot Start\tRoot End\tJob End\tFile Write
================""" % (start, end, net_time, last_job, last_file)

	for jid in procs:
		print("%s\t%s\t%s\t%s\t%s\t%s" % (jid, procs[jid]["start"], procs[jid]["root_start"], procs[jid]["root_end"], procs[jid]["end"], procs[jid]["file"]))

	########################################
	########################################

if (__name__ == "__main__"):
	main(time.time(), dict.fromkeys(getClusterIds()))



def getJobInfoFromHistory(jid):
	"""Gets the time of completion, the start time, and the time submitted from condor_history"""
	cq = sp.Popen(["condor_history", jid], stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with bhashemi, same as a grep call
	lines = filter(lambda x: "bhashemi" in x, lines)
	if len(lines) == 1:
		info=lines.split()
		return({"runtime": info[4], "start": info[2]+' '+info[3], "end": info[6]+' '+info[7], "status": info[5]})
	else:
		print("There was an error parsing condor_history for Job ID: %s" % jid)
		return None



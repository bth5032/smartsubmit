#!/usr/bin/python

import subprocess as sp
import time, datetime, sys, os


def checkMachines(procs):
	"""Checks the filenames of stdout against the machine the job landed on from uname in the jobs stdout. Returns a dictionary with (clusterID, stdoutfile) (key, value) pairs."""
	
	wrong_machines = {}
	for jid in procs:
		target=procs[jid]["stdout"].split("condorLog_")[1].split("_")[0]
		if not target == procs[jid]["machine"]:
			wrong_machines[jid] = procs[jid]["stdout"]
	
	return wrong_machines

def hrt(unix_time):
	"""return human readable time"""
	try:
		return datetime.datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
	except:
		print("time could not be parsed")
		return "N/A"

def getClusterIds():
	"""Reads the standard input which should be filled with all the cluster ids for the jobs just submitted."""
	ids = []
	for line in sys.stdin:
		if line.strip():
			print(line[:-1])
			ids.append(line[:-1])
	return ids

def getFilenames(jid):
	"""Parses condor_history -l and returns a tuple of (stdout, stderr, outfile) the locations of the std output, std error, and returned files for the job with the specified job id"""
	
	out = err = outfile = ""

	cq = sp.Popen(["condor_history","-l",jid], stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')

	for l in lines:
		if l[:6] == "Out = ":
			out = l.split('"')[1]
		elif l[:6] == "Err = ":
			err = l.split('"')[1]
		elif l[:20] == "TransferOutputRemaps":
			outfile=l.split('"')[1].split('=')[1]

	if not out:
		print("issue with stdout: %s" % jid)
	if not err:
		print("issue with stderr: %s" % jid)
	if not outfile:
		print("issue with outfile: %s" % jid)
	return (out, err, outfile)	

def getFileTime(path):
	"""Takes a path to the file and blocks until the file is completely written, then returns the last modified time from stat"""
	if not path:
		return "N/A"
	else:
		while True:
			if os.path.exists(path):
				stats = os.stat(path)
				time.sleep(5)
				if stats.st_mtime == os.stat(path).st_mtime:
					return stats.st_mtime

def stdoutInfo(stdout, stderr):
	time_start = time_end = root_start = root_end = root_real = root_user = root_sys = machine = ""

	print(stdout)

	with open(stdout) as f:
		for line in f:
			if not line.strip():
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
			elif a[0] == "MACHINE:":
				machine=a[1]

	print(stderr)

	with open(stderr) as f:
		for line in f:
			a=line.split()
			if len(a) < 1:
				continue
			elif a[0] == "real":
				root_real = a[1]
			elif a[0] == "user":
				root_user = a[1]
			elif a[0] == "sys":
				root_sys = a[1]


	return (time_start, root_start, root_end, time_end, root_real, root_user, root_sys, machine)

def running(JIDs):
	"""Returns whether any condor jobs exist for bhashemi"""
	cq = sp.Popen("condor_q", stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with bhashemi, same as a grep call
	lines = filter(lambda x: any(jid in x for jid in JIDs), lines)
	return bool(lines)

def wait(JIDs):
	"""keeps running until all jobs are finished"""
	while running(JIDs):
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

	wait(procs.keys()) # (1)
	end_time = time.time()

	for jid in procs:
		procs[jid]= dict(zip(("stdout", "stderr", "outfile"), getFilenames(jid))) # (2)
		procs[jid]["time_start"], procs[jid]["root_start"], procs[jid]["root_end"], procs[jid]["time_end"], procs[jid]["root_real"], procs[jid]["root_user"], procs[jid]["root_sys"], procs[jid]["machine"]  = stdoutInfo(procs[jid]["stdout"], procs[jid]["stderr"]) # (3)
		procs[jid]["file_time"] = getFileTime(procs[jid]["outfile"])
		procs[jid]["file_time_hr"] = hrt(procs[jid]["file_time"])

	########################################
	# Print it all out #####################

	print(procs)

	net_time = datetime.timedelta(seconds=(end_time-start_time))

	last_job = last_file = 0

	for jid in procs:
		if (procs[jid]["file_time"]) and (procs[jid]["file_time"] > last_file):
			last_file = procs[jid]["file_time"]
		if procs[jid]["time_end"] > last_job:
			last_job = procs[jid]["time_end"]

	print("""Start:\t%s
End:\t%s
Net Time:\t%s
Last Job:\t%s
Last File:\t%s
================
ID\tStart\tRoot Start\tRoot End\tRoot Real Time\tRoot Sys Time\tRoot User Time\tJob End\tFile Write
================""" % (start_time, end_time, net_time, last_job, last_file))

	for jid in procs:
		print("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (jid, procs[jid]["time_start"], procs[jid]["root_start"], procs[jid]["root_end"], procs[jid]["root_real"], procs[jid]["root_sys"], procs[jid]["root_user"], procs[jid]["time_end"], procs[jid]["file_time_hr"]))

	wrong_machines = checkMachines(procs)


	if wrong_machines:
		print("\n===========================================\nThere were some jobs that did not land on the proper machines")
		for jid in wrong_machines:
			print("JID: %s\tstdout: %s" % (jid, wrong_machines[jid]))

	else:
		print("\n===========================================\nEvery job landed on the proper machine.")

	########################################
	########################################

if (__name__ == "__main__"):
	main(time.time(), dict.fromkeys(getClusterIds()))
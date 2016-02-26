#!/usr/bin/python

import subprocess as sp
import time, datetime, sys


def getLines():
	cq = sp.Popen("condor_q", stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with bhashemi, same as a grep call
	lines = filter(lambda x: "bhashemi" in x, lines)
	return lines

def getClusterIds():
	"""Reads the standard input which should be filled with all the cluster ids for the jobs just submitted."""
	ids[]
	for line in sys.stdin:
		ids.append(line[:-1])
	return ids

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

def getStdoutFilename(jid):
	"""Reads the condor logs and returns the location of the std output for the job with the specified job id"""
	cq = sp.Popen(["condor_history","-l",jid], stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with "Out = ", same as a grep call
	lines = filter(lambda x: "Out = " in x, lines)
	return lines[7:-1]

def getInfoFromStdout(filename):
	"""Reads in data from the standard output of the jobs and parses out relevant information"""
	f = open(filename)
	

def running():
	"""Returns whether any condor jobs exist for bhashemi"""
	cq = sp.Popen("condor_q", stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with bhashemi, same as a grep call
	lines = filter(lambda x: "bhashemi" in x, lines)
	return bool(lines)

def fillJobData(procs):
	"""Fills in the dictionary procs with values from"""	
	for jid in procs:
		procs[jid]=getJobInfoFromHistory(jid)
		procs[jid]["filename"]=getStdoutFilename(jid)
		procs[jid].update(getInfoFromStdout(procs[jid]["filename"]))

def wait():
	"""keeps running until all jobs are finished"""
	while running()
		time.sleep(300)

def main():
	"""Main function. Gets start time and waits for all the jobs in the condor_q to be completed. Then it reads timing information for each job from both the condor logs and the standard output of the condor executable script."""
	start=time.time()

	#### Set up procs dictionary
	ids = getClusterIds()
	procs = dict.fromkeys(ids)

	wait() # wait for jobs to end
	fillJobData(procs) #extract data into the procs dict from condor_history, the stdout file, 

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
	main()
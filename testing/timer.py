#!/usr/bin/python

import subprocess as sp
import time, datetime


def getLines():
	cq = sp.Popen("condor_q", stdout=sp.PIPE)
	output = cq.communicate()[0]
	lines = output.split('\n')
	#get only lines with bhashemi, same as a grep call
	lines = filter(lambda x: "bhashemi" in x, lines)
	return lines

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


start=time.time()
first_started = None
## Set up dictionaries and lists with all the processes.
lines = getLines()
procs = {}

for line in lines:
	line=line.split()
	procs[line[0]] = {"run_time":None,"start_time":None,"end_time":None,"elapsed_time":None}


while lines:
	this_procs_active = []
	for line in lines:
		line=line.split()
		ID=line[0]
		run_time = line[4]

		this_procs_active.append(ID)
		procs[ID]["run_time"] = run_time

		#Set start time if there is no start time and the proc is running
		if procs[ID]["start_time"] == 0 and line[5] == "R":
			#print("started %s" % str(ID)) 
			now = time.time()
			if first_started == 0:
				first_started = now
				print("First process started at: %s with ID: %s" % (time.strftime("%T"), ID))
				print("The latency between submission and starting was: %s" % str(datetime.timedelta(seconds=(first_started - start))))
				print("\n ID\t| Start time\t| End Time\t| elapsed time\t| 'run_time'(from condor):")
			procs[ID]["start_time"] = now
		
		#Set end time if we get status "C" for completed
		if procs[ID]["end_time"] == 0 and line[5] == "C":
			#print("found C")
			procs[ID]["end_time"] = time.time()
			procs[ID]["elapsed_time"] = datetime.timedelta(seconds=(procs[ID]["end_time"] - procs[ID]["start_time"]))
			print("%s\t| %s\t| %s\t| %s\t| %s" % (str(ID), str(procs[ID]["start_time"]), str(procs[ID]["end_time"]), str(procs[ID]["elapsed_time"]), str(procs[ID]["run_time"])))
			this_procs_active.pop()

	#Set end time for any processes that werent in the que anymore
	for condor_id in (set(last_procs_active) - set(this_procs_active)):
		#print("lost the line for %s" % str(ID))
		if procs[condor_id]["end_time"] == 0: #If we get a line with Status "C" for complete this will be set higher up
			procs[condor_id]["end_time"] = time.time()
			print("%s\t| %s\t| %s\t| %s\t| %s" % (str(ID), str(procs[ID]["start_time"]), str(procs[ID]["end_time"]), str(procs[ID]["elapsed_time"]), str(procs[ID]["run_time"])))

	
	lines=getLines()
	last_procs_active = this_procs_active

### End While

end = time.time()

for ID in this_procs_active:
	#print("Check for %s after loop broke" % str(ID))
	if procs[ID]["end_time"] == 0: #If we get a line with Status "C" for complete this will be set higher up
		procs[ID]["end_time"] = time.time()
		procs[ID]["elapsed_time"] = datetime.timedelta(seconds=(procs[ID]["end_time"] - procs[ID]["start_time"]))
		print("%s\t| %s\t| %s\t| %s\t| %s" % (str(ID), str(procs[ID]["start_time"]), str(procs[ID]["end_time"]),str(procs[ID]["elapsed_time"]), str(procs[ID]["run_time"])))

print("total elapsed time:")
print(datetime.timedelta(seconds=(end-start)))
print("Total time from first started job:")
print(datetime.timedelta(seconds=(end-first_started)))
	


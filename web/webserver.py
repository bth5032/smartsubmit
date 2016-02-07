from flask import *
from json import dumps as json_dump

import sys, os, subprocess
sys.path.append("../")
import sqlman, sqlite3


database_file="../test.db"
working_dir="../"
connection = sqlite3.connect(database_file, check_same_thread=False)
man = sqlman.sqlman(connection, database_file, working_dir)

app=Flask(__name__)

def checkAlive():
	ps = subprocess.Popen("ps aux", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	out= ps.stdout.readline().decode('UTF-8').rstrip('\n')
	exit_code = ps.returncode

	if "smartsubmit_daemon.py" in str(out):
		return True
	else:
		return False

@app.route("/")
def showIndex():
	
	#Get Sample Info
	sample_dict = frontpageSamples()
	sample_list = []
	for k in sample_dict:
		sample_list.append(sample_dict[k])

	#Get Log Info
	log_list=renderedLogs(3)
	up_list=getUptime()

	return render_template("index.html", sample_list=sample_list, log_list=log_list, up_list=up_list)

def frontpageSamples():
	"""Takes in the list of the sample files table from the server and pretty prints it to the screen."""
	slist=man["SampleFiles"]
	samples={}
	stripped_list = [[x[1],x[8]] for x in slist]
	for x in stripped_list:
		if x[0] in samples:
			samples[x[0]]["count"] += 1
		else:
			samples[x[0]] = {"name": x[0], "count": 1, "owner": x[1]}

	return samples

def rawLogs(count=3):
	"""Reads latest log files and pulls 'count' commands in"""
	path="../"
	fls = os.listdir(path) #file list
	lls = [] #log list
	for f in fls:
		if ("smartsubmit_" in f) and (".log" in f):
			lls.append(f)

	lls.sort(key=lambda f: os.path.getmtime(os.path.join(path, f)), reverse=True)
	output=[]

	for fname in lls:
		f=open("../"+fname)
		for line in f:
			if "recieved command:" in line:
				output.append(line)
				count-=1
				if count<1:
					return output

	return output

def renderedLogs(count=20):
	log_list=rawLogs(count)
	return [ [log_list[i][0:19], log_list[i][40:]] for i in range(0,len(log_list)) ]

def getUptime(count=3):
	#f=open("/root/ss_testing/weblog")
	lines=[]
	with open("/root/ss_testing/weblog") as f:
		for l in f:
			lines.append([ l[:l.rfind(":")] , l[l.rfind(":")+1:-1]])
			count-=1
			if count == 0:
				return lines

	return lines

@app.route("/files")
def renderFiles():
	sample_dict = frontpageSamples()
	sample_list = []
	for k in sample_dict:
		sample_list.append(sample_dict[k])

	return render_template("files.html", sample_list=sample_list)

@app.route("/get_sample_files/<sname>.json")
def returnSampleFiles(sname):
	rows = man.x("Select FileName, LocalDirectory, HadoopPath, Machine, User From SampleFiles Where Sample=='%s'" % sname)
	return Response(json_dump(rows), mimetype='application/json')

@app.route("/howto")
def renderHowTo():
	return render_template("howto.html")

@app.route("/history")
def renderHistory():
	log_list=renderedLogs(20)
	alive=checkAlive()
	return render_template("history.html", log_list=log_list, alive=alive)

@app.route("/uptime")
def renderUptime():
	up_list=getUptime(20)

	return render_template("uptime.html", up_list=up_list)

if __name__ == "__main__":
    app.run(debug=True)
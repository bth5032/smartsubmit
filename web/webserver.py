from flask import *
from json import dumps as json_dump

import sys, os
sys.path.append("../")
import sqlman, sqlite3


database_file="../test.db"
working_dir="../"
connection = sqlite3.connect(database_file, check_same_thread=False)
man = sqlman.sqlman(connection, database_file, working_dir)

app=Flask(__name__)


@app.route("/")
def showIndex():
	
	#Get Sample Info
	sample_dict = frontpageSamples()
	sample_list = []
	for k in sample_dict:
		sample_list.append(sample_dict[k])

	#Get Log Info
	log_list=frontpageLog()

	return render_template("index.html", sample_list=sample_list, log_list=log_list)

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

def frontpageLog(count=3):
	"""Reads latest log files and pulls 10 commands in"""
	fls = os.listdir("../") #file list
	lls = [] #log list
	for f in fls:
		if ("smartsubmit_" in f) and (".log" in f):
			lls.append(f)

	lls.sort(reverse=True)

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


def frontpageHistory():
	pass

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
	return render_template("howto.html")

@app.route("/uptime")
def renderUptime():
	return render_template("howto.html")

if __name__ == "__main__":
    app.run(debug=True)
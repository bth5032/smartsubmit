import os, time

class SmartSubmitCommand(object):
	"""Command object sent to the smartsubmit server. 
		The supported commands are 
		1. run job
		2. add file
		3. delete file
		4. add directory
		5. delete sample
		6. list sample files"""

	def __init__(self, kwargs):
		self.user = os.getenv("LOGNAME")
		self.time = time.time()
		self.strftime = time.strftime("%m-%d-%Y_%H:%M:%S")

		self.command = kwargs["command"]
		if self.command == "run job":
			"""Run a job. Expects 
					1. path_to_executable, a string that gives the location of the executable on the network drive.
					2. samples, a list of strings that are the sample names in the smartsubmit database
					3. path_to_template, a string that gives the location of the condor template file."""
			self.exe_path = kwargs["path_to_executable"]
			self.samples = kwargs["samples"]
			self.temp_path = kwargs["path_to_template"]
		elif self.command == "add file":
			"""Add a file. Expects 
					1. path_to_file, a string that gives the location of the .root file on the network drive.
					2. sample, the name of the sample with which to tag the file given"""
			self.sample = kwargs["sample"]
			self.filename = os.path.basename(kwargs["path_to_file"])
			self.dir = os.path.dirname(kwargs["path_to_file"])+'/'
		elif self.command == "delete file":
			"""Delete a file. Expects 
					1. path_to_file, a string that gives the location of the .root file on the network drive."""
			self.filename = os.path.basename(kwargs["path_to_file"])
			self.dir = os.path.dirname(kwargs["path_to_directory"])+'/'
		elif self.command == "add directory":
			"""Add a directory. Expects 
					1. path_to_directory, a string that gives the location of the folder which holds the .root on hadoop.
					2. sample, the name of the sample with which to tag the given files"""
			self.sample = kwargs["sample"]
			self.dir = kwargs["path_to_directory"]
			if not self.dir[-1:] == '/':
				self.dir+='/'
		elif self.command == "delete sample":
			"""Delete all files in a sample: Expects
				1. sample_name: The sample name which is to be deleted """
			self.sample = kwarg["sample_name"]
			self.dir = os.path.dirname(kwargs["path_to_directory"])
		elif self.command == "list sample files":
			"""Just list sample files and exit"""
			pass

	def __repr__(self):
		return self.__dict__

	def __str__(self):
		return str(self.__dict__)
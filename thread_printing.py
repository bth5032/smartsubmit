"""Module which allows each thread to print to its own file specified in the printer object's thread_files dictionary"""

import threading, time, sys, _io

class ThreadPrinter(_io.TextIOWrapper):
	def __init__(self):
		self.thread_files = {}

	def add_thread(self, name, outfile):
		self.thread_files[name]=outfile

	def write(self, value):
		try:
			outfile = self.thread_files[threading.currentThread().name]
			outfile.write(value)
			outfile.flush()
		except:
			sys.__stdout__.write(value)



printer = ThreadPrinter()
main_name = threading.currentThread().name
printer.add_thread(main_name, sys.__stdout__)

sys.stdout=printer
#sys.stderr=printer
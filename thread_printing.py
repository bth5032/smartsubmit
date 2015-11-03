"""Module which allows each thread to print to its own file specified in the printer object's thread_files dictionary"""

import threading, sys, io

class ThreadPrinter(io.TextIOWrapper):
  
	def __init__(self):
		self.thread_files = {}

	def add_thread(self, name, outfile):
		self.thread_files[name]=outfile

	def write(self, value):
		try:
			outfile = self.outfile() #If you forget to add the thread this will fail
			outfile.write(value)
			outfile.flush()
		except:
			sys.__stdout__.write(value) #write to stdout if you can't write to the file
			sys.__stdout__.flush()
	
	def __del__(self):
		sys.stdout = sys.__stdout__
	
	def __repr__(self):
		return "ThreadPrinter" #So that errors can be traced back to this class

	def outfile(self):
		return self.thread_files[threading.currentThread().name]

	def flush(self):
		if not threading.enumerate(): #When the program closes one final flush is called, so we'll get errors unless we handle that by checking whether the flush is being called after all the threads are closed
			return
		else:
			self.outfile().flush()

def closeThreadFile(threadname):
	if not printer.thread_files[threadname].closed:
		print("closing thread %s" % threadname)
		printer.thread_files[threadname].close()

printer = ThreadPrinter()
main_name = threading.currentThread().name
printer.add_thread(main_name, sys.__stdout__) #keep the main thread printing to the standard output

sys.stdout=printer #print() is now a proxy to printer.write()
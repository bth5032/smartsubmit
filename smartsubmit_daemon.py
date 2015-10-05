import smartsubmit as ss
import zmq

context = zmq.Context()
port="8000"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

while True:
	message=socket.recv()
	if message[:18] == "absorb sample file":
		socket.send_string("Absorbing Sample File")
		print(message)
	elif message[:23] == "absorb sample directory":
		socket.send_string("Absorbing Sample Directory")
		print(message)
	elif message[:18] == "delete sample file":
		socket.send_string("Deleting Sample File")
		print(message)
	elif message[:7] == "run job":
		socket.send_string("Running Job On Sample")
		print(message)

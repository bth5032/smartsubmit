import smartsubmit as ss
import zmq

context = zmq.Context()
port="8000"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

message=socket.recv()
socket.send_pyobj(ss)
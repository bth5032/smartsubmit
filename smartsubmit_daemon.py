import smartsubmit as ss

context = zmq.Context()
port="8000"
socket=context.socket(zmq.REP)
socket.bind("tcp://*:%s" % port)

message=socket.recv()
socket.send_string(str(getBestDisk("ttbar_powheg_pythia8_25ns")))
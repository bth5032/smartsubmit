import zmq, argparse

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--message", default="Hi There", help="choose the message to send!", action="store")

args=parser.parse_args()

port="8000"

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:%s" % port)
socket.send_string("%s" % "Hi")
message = socket.recv()
print("Recieved reply: %s" % ss.getBestDisk("ttbar_powheg_pythia8_25ns"))

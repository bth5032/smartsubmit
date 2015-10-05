import zmq, argparse

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--message", default="Hi There", help="choose the message to send!", action="store")

args=parser.parse_args()

port="7584"

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:%s" % port)
socket.send_string("%s" % "absorb sample file /hadoop/path/myfile.root")
message = socket.recv_string()
print("Recieved reply: %s" % message)

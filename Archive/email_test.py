import smartsubmit as ss
import zmq, time, threading, logging
from ss_com import SmartSubmitCommand

## Email Stuff
import smtplib 
import email.mime.text as mt
import email.utils as eutils

admins = [["name1", "addr1"]]
username = ""
password = ""

def emailAdmins(message_body):
	server = smtplib.SMTP('smtp.ucsd.edu', 587)
	server.starttls()
	server.login(username, password)
	msg = mt.MIMEText(message_body)
	
	if len(admins) > 1:
		msg["To"] = ", ".join([eutils.formataddr(x) for x in admins])
	else:
		msg["To"] = eutils.formataddr(admins[0])

	msg["From"] = eutils.formataddr(admins[0])
	server.sendmail(admins[0][1], [x[1] for x in admins], msg.as_string())
	server.quit()
	
def diskCheckHelper():
	"""Checks the disks every 6 hours, if a disk has gone bad, it will email everyone in the admins list defined above."""

	while True:
		message = ""
		#Run check disk for every disk in the table
		for (directory, machine, working) in [["/data3/xrootd/smartsubmitSamples/","cabinet-7-7-7.t2.ucsd.edu", 1], ["/data3/xrootd/smartsubmitSamples/","cabinet-7-7-0.t2.ucsd.edu", 0], ["/data3/xrootd/smartsubmitSamples/","cabinet-7-7-0.t2.ucsd.edu", 1], ["/data4/xrootd/smartsubmitSamples/","cabinet-7-7-0.t2.ucsd.edu", 1], ["/data3/xrootd/smartsubmitSamples/","cabinet-7-7-17.t2.ucsd.edu", 1]]:
			if working:
				result=ss.checkDisk(directory, machine)
				if not result == True:
					message += "The disk mounted at '%s' on '%s'  may have gone down.\n%s\n" % (directory, machine, result)
		if message:
			emailAdmins(message)
		time.sleep(60)


disk_check=threading.Thread(name="disk_check", target=diskCheckHelper, daemon=True)
disk_check.start()

while True:
	time.sleep(500)
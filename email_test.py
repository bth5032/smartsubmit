import smartsubmit as ss
import thread_printing as tp
import zmq, time, threading, logging
from ss_com import SmartSubmitCommand

## Email Stuff
import smtplib 
import email.mime.text as mt
import email.utils as eutils

admins = ["bthashemi@ucsd.edu"]
username = ""
password = ""

def emailAdmins(message_body):
	server = smtplib.SMTP('smtp.ucsd.edu', 587)
	server.starttls()
	server.login(username, password)
	msg = mt.MIMEText(message_body)
	msg["To"] = ", ".join([eutils.formataddr(x) for x in admins])
	msg["From"] = eutils.formataddr(["Bobak Hashemi", "bthashemi@ucsd.edu"])
	server.sendmail("bthashemi@ucsd.edu", admins, msg.as_string())
	server.quit()
	
def diskCheckHelper():
	"""Checks the disks every 6 hours, if a disk has gone bad, it will email everyone in the admins list defined above."""
	while True:
		#Run check disk for every disk in the table
		for (disk_id, directory, machine, disk_num, working) in ss.man["Disks"]:
			if working:
				if not False:
					message = "The disk mounted at '%s' on '%s'  may have gone down." % (directory, machine)
					emailAdmins(directory, machine, message)
		time.sleep(60)



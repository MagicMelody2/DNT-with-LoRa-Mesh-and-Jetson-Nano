import serial
import sqlite3
import requests
from datetime import datetime

# Add Firebase URL to connect to the realtime database
FIREBASE_URL = "https://battleship-a7d29-default-rtdb.firebaseio.com" 


# Create database for packets.db
db = sqlite3.connect("packets.db")

db.execute("""
	CREATE TABLE IF NOT EXISTS packets(
	id INTEGER PRIMARY KEY, 
	packet_id TEXT, 
	node TEXT,
	data TEXT, 
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	status INT
	)
""")


# Define Functions
def sync_pending_packets():

	# Remember which row only has status=0
	unsynced_packets = db.execute(
		"SELECT * FROM packets WHERE status = 0"
	).fetchall()

	for i in unsynced_packets:
		print(i)

			try:
		r = requests.post(
		FIREBASE_URL + "/jetson_packets.json",
		json = payload
		)

		print("Firebase: ", r.status_code)

		while  r.status_code == 200:

			db.execute(
				"UPDATE packets SET status = 1 WHERE id = ?",
				(packet_db_id,)
			)
			db.commit()

			print("Packet synced successfully")

	except Exception as e:
		print("Firebase Error:", e)
		print("Packet remains queued")

	return


sync_pending_packets()

ser = serial.Serial('/dev/ttyUSB0', 115200)

while True:

	line = ser.readline().decode(errors='ignore').strip()

	if line:
		print("RECIEVED: ", line)

		timestamp = datetime.now().isoformat()

	# Save locally
	db.execute(
		"INSERT INTO packets (data, timestamp, status) VALUES (?, ?, ?)",
		(line, timestamp, 0)
	)
	db.commit()

	# Remember which row was just inserted
	packet_db_id = db.execute(
		"SELECT last_insert_rowid()"
	).fetchone()[0]

	# Push to firebase
	payload = {
		"message": line,
		"timestamp": timestamp,
	}

	try:
		r = requests.post(
		FIREBASE_URL + "/jetson_packets.json",
		json = payload
		)

		print("Firebase: ", r.status_code)

		if r.status_code == 200:

			db.execute(
				"UPDATE packets SET status = 1 WHERE id = ?",
				(packet_db_id,)
			)
			db.commit()

			print("Packet synced successfully")

		else:
			print("Upload failed, packet remains queued")


	except Exception as e:
		print("Firebase Error:", e)
		print("Packet remains queued")


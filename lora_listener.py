
# Import libraries
import serial
import sqlite3
import requests
import time
from datetime import datetime

# Add Firebase URL to connect to the realtime database
FIREBASE_URL = "https://battleship-a7d29-default-rtdb.firebaseio.com" 

# How often to attempt backlog sync
RETRY_INTERVAL_SECONDS = 60

# Create database for packets.db
db = sqlite3.connect("packets.db")

db.execute("""
	CREATE TABLE IF NOT EXISTS packets(
	id INTEGER PRIMARY KEY,
	packet_id TEXT,
	node TEXT,
	data TEXT,
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	status INT,
	firebase_key TEXT
	)
""")

# Take packets and either uploads them to database or saves it to queue if there is no connection
def upload_packet(packet_db_id, payload):

	"""Try to upload one packet to Firebase. Returns True on success."""

	# Tests connection with a timeout of 5 seconds, if connection is lost
	try:
		r = requests.post(
			FIREBASE_URL + "/jetson_packets.json",
			json = payload,
			timeout = 5
		)

		# If device is connected to wifi, upload the packets and set the status to 1
		# to show package was synced
		if r.status_code == 200:

			firebase_key = r.json()["name"]

			db.execute("UPDATE packets SET status = 1, firebase_key = ? WHERE id = ? ", (firebase_key, packet_db_id))
			db.commit()
			print(f"Packet {packet_db_id} synced successfully")
			return True
		else:
			print(f"Upload failed ({r.status_code}), packet {packet_db_id} remains queued")
			return False

	# Packet stays in queue if there is there is an exception
	except Exception as e:
		print("Firebase Error:", e)
		print(f"Packet {packet_db_id} remains queued")
		return False


# Sync packets
def sync_pending_packets():

	"""Try to upload one packet to Firebase.Returns True on success."""

	# Fetch all packets if status = 0 (not syncyed)
	unsynced_packets = db.execute(
		"SELECT id, data, timestamp FROM packets WHERE status = 0"
	).fetchall()

	# If there is no unsynced packets, 
	if not unsynced_packets:
		return

	# Update user
	print(f"Found {len(unsynced_packets)} unsynched packet(s), retrying...")

	# For every packet in unsynced array, add the data and timestamp from table to a JSON payload
	# Then upload the packet with the ID and the JSON
	for packet_db_id, data, timestamp in unsynced_packets:
		payload = {"message": data, "timestamp": timestamp}
		upload_packet(packet_db_id, payload)

# Automatically delete specific packets from Firebase
def delete_packets(packet_id):

	row = db.execute("SELECT firebase_key FROM packets WHERE id=?",(packet_id,)).fetchone()

	if not row:
		return

	firebase_key = row[0]

	requests.delete(f"{FIREBASE_URL}/jetson_packets/{firebase_key}.json")

	db.execute("DELETE FROM packets WHERE id=?", (packet_id,))

	db.commit()

# Delete all packets
def delete_all_packets():

	requests.delete(FIREBASE_URL + "/jetson_packets.json")

	db.execute("DELETE FROM packets WHERE status = 1")
	db.commit()
	print("All synced packets delete")


# Call functions
delete_all_packets()
sync_pending_packets()

# Get connection status from USB webcam
ser = serial.Serial('/dev/ttyUSB0', 115200)

# Update retry time to current time
last_retry_time = time.time()
print("Time", last_retry_time)

while True:

	# Checks if there is any serial data from the LoRa packets
	if ser.in_waiting:
		line = ser.readline().decode(errors='ignore').strip()

		# If yes - decode the data and forward it to Firebase
		if line:
			print("RECIEVED:", line)
			timestamp = datetime.now().isoformat()


		# Execute SQL comamnd to insert data into columns using packets
		# Then commit the command
		db.execute(
			"INSERT INTO packets (data, timestamp, status) VALUES (?, ?, ?)",
			(line, timestamp, 0)
		)
		db.commit()

		# Fetch the last packet in the table and save the packet id
		packet_db_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

		# Create paylod and upload packet with id and payload information
		payload = {"message": line, "timestamp": timestamp}
		upload_packet(packet_db_id, payload)

	# If the current time - the last retry time was longer than a minute
	# Try to sync packets again to see if connection was found
	# Update last_retry time to current time
	if time.time() - last_retry_time >= RETRY_INTERVAL_SECONDS:
		sync_pending_packets()
		last_retry_time = time.time()
		print("Timer restarted", last_retry_time)
	# Small pause to not max out a CPU core
	time.sleep(0.05)




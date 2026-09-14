
# Import libraries
import serial
import sqlite3
import requests
import time
import json
from datetime import datetime
nodes = {}

# =====================================================================
# CONFIG
# ====================================================================

# Add Firebase URL to connect to the realtime database
FIREBASE_URL = "https://battleship-a7d29-default-rtdb.firebaseio.com" 

# How often to attempt backlog sync
RETRY_INTERVAL_SECONDS = 60


# =====================================================================
# DATABASE
# ====================================================================

# Create database for packets.db
db = sqlite3.connect("packets.db")

db.execute("""
	CREATE TABLE IF NOT EXISTS packets(
	id INTEGER PRIMARY KEY,

	seq TEXT UNIQUE,
	rover_id TEXT,

	latitude REAL,
	longitude REAL,

	rover_time TEXT,
	recieved_time TEXT,

	raw_packet TEXT,

	status INTEGER DEFAULT 0,
	firebase_key TEXT

	)
""")

db.commit()

# =====================================================================
# FIREBASE UPLOAD
# ====================================================================

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
			print(f"[SYNCED] Packet {packet_db_id}")
			return True
		else:
			print(f"[FAILED] HTTP ({r.status_code}), packet {packet_db_id} remains queued")
			return False

	# Packet stays in queue if there is there is an exception
	except Exception as e:
		print("[FAILED] Firebase Error:", e)
		print(f"Packet {packet_db_id} remains queued")
		return False



# =====================================================================
# RETRY UNSYNCED PACKETS
# ====================================================================
def sync_pending_packets():

	"""Try to upload one packet to Firebase.Returns True on success."""

	# Fetch all packets if status = 0 (not syncyed)
	unsynced_packets = db.execute(
		"""
		SELECT
			id,
			seq,
			rover_id,
			latitude,
			longitude,
			rover_time,
			recieved_time
		FROM packets
		WHERE status = 0
		"""
	).fetchall()

	# If there is no unsynced packets,
	if not unsynced_packets:
		print("No packets waiting for sync")
		return

	# Update user
	print(f"Found {len(unsynced_packets)} unsynched packet(s), retrying...")

	# For every packet in unsynced array, add the data and timestamp from table to a JSON payload
	# Then upload the packet with the ID and the JSON
	for row in unsynced_packets:

		packet_db_id = row[0]

		payload = {
			"seq": row[1],
			"rover_id": row[2],
			"latitude": row[3],
			"longitude": row[4],
			"rover_time": row[5],
			"recieved_time": row[6]
		}

		upload_packet(packet_db_id, payload)


# =====================================================================
# DELETE SINGLE PACKET
# ====================================================================

# Automatically delete specific packets from Firebase
def delete_packets(packet_id):

	row = db.execute("SELECT firebase_key FROM packets WHERE id=?",(packet_id,)).fetchone()

	if not row:
		return

	firebase_key = row[0]

	requests.delete(f"{FIREBASE_URL}/jetson_packets/{firebase_key}.json")

	db.execute("DELETE FROM packets WHERE id=?", (packet_id,))

	db.commit()

	print(f"Deleted packet {packet_id}")


# =====================================================================
# DELETE ALL SYNCED PACKETS
# ====================================================================

# Delete all packets

def delete_all_packets():

	requests.delete(FIREBASE_URL + "/jetson_packets.json")

	db.execute("DELETE FROM packets WHERE status = 1")
	db.commit()
	print("Deleted all synced packets")



# =====================================================================
# UPDATE ALL NODES
# ====================================================================


def update_node(packet):

	rover_id = packet.get("id")

	if not rover_id:
		return

	nodes[rover_id] = {
		"lat": packet.get("lat"),
		"lng": packet.get("lng"),
		"battery": packet.get("battery"),
		"hops": packet.get("hops"),
		"last_seen": time.time()
	}


# =====================================================================
# DISPLAY NODES
# ====================================================================


def display_nodes():

	print("\n === ACTIVE NODES ===")

	for rover_id, info in nodes.items():
		age = time.time() - info["last_seen"]

		print(
			f"{rover_id} | "
			f"LAT: {info['lat']} "
			f"LNG: {info['lng']} "
			f"HOPS: {info['hops']} "
			f"AGE:{age:.1f}s"
		)

	print("==========================")



# =====================================================================
# OFFLINE DETECTION
# ====================================================================


def check_timeouts():

	for rover_id, info in nodes.items():
		age = time.time() - info["last_seen"]

		if age > 30:
			print(f"[OFFLINE] {rover_id} last seen {age:.0f}s ago")


# =====================================================================
# STARTUP
# ====================================================================

# Call functions
delete_all_packets()
sync_pending_packets()

# Get connection status from USB webcam
ser = serial.Serial('/dev/ttyUSB0', 115200)

# Update retry time to current time
last_retry_time = time.time()
print("Listening for LoRa packet...", last_retry_time)



# =====================================================================
# MAIN LOOP
# ====================================================================
while True:

	try:

		# Checks if there is any serial data from the LoRa packets
		if ser.in_waiting:
			line = ser.readline().decode(errors='ignore').strip()

			# If yes - decode the data and forward it to Firebase
			if not line:
				continue

			# print("RAW:", line)
			print("SERIAL: ", repr(line))

			# BaseNode prints:
			# [RX] {"id: Rover_1"...}
			if "[RX]" not in line:
				print("INFO:", line)
				continue

			json_start = line.find("{")

			if json_start == -1:
				continue

			json_text = line[json_start:]

			try:
				packet = json.loads(json_text)

				# Debug statements
				print("\n ----- PACKET -----")
				print("RID: ", packet.get("id"))
				print("LAT: ", packet.get("lat"))
				print("LNG: ", packet.get("lng"))
				print("TME: ", packet.get("time"))
				print("HOP: ", packet.get("hops"))
				print("SEQ: ", packet.get("seq"))
				print("-----------------------")


				update_node(packet)
				display_nodes()
				check_timeouts()


			except Exception as e:
				print("JSON Error:", e)
				print("Raw JSON:", json_text)
				continue

			# Execute SQL comamnd to insert data into columns using packets
			# Then commit the command
			rover_id = packet.get("id")
			seq = packet.get("seq")
			lat = packet.get("lat")
			lng = packet.get("lng")
			rover_time = packet.get("time")
			recieved_time = datetime.now().isoformat()

			# Duplicate protection
			if seq:
				existing = db.execute(
					"""
			
		SELECT id
					FROM packets
					WHERE seq = ?
					""",
					(seq,)
				).fetchone()

				if existing:
					print(f"[DUPLICATE] {seq} ignored")
					continue

			db.execute(
				"""
				INSERT INTO packets(
					seq,
					rover_id,
					latitude,
					longitude,
					rover_time,
					recieved_time,
					raw_packet,
					status
				)
				VALUES (?, ?, ?, ?, ?, ?, ?, 0)
				""",
				(
					seq,
					rover_id,
					lat,
					lng,
					rover_time,
					recieved_time,
					json.dumps(packet)
				)
			)

			db.commit()
			# Fetch the last packet in the table and save the packet id
			packet_db_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

			payload = {
				"seq": seq,
				"rover_id": rover_id,
				"latitude": lat,
				"longitude": lng,
				"rover_time": rover_time,
				"recieved_time": recieved_time
			}

			upload_packet(
				packet_db_id,
				payload
			)

			# If the current time - the last retry time was longer than a minute
			# Try to sync packets again to see if connection was found
			# Update last_retry time to current time
		if(
			 time.time() - last_retry_time >= RETRY_INTERVAL_SECONDS
		):
			sync_pending_packets()
			last_retry_time = time.time()
			print("Timer restarted:", last_retry_time)

		# Small pause to not max out a CPU core
		time.sleep(0.05)

	except KeyboardInterrupt:
		print("Stopping...")
		break

	except Exception as e:
		print("Runtime Error:", e)
		time.sleep(1)
db.close()

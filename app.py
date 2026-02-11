import time
import threading
from flask import Flask, jsonify
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

app = Flask(__name__)

# 🔐 Firebase Init
cred = credentials.Certificate("/etc/secrets/serviceAccount.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# 🔥 YOUR CLEANUP LOGIC (fixed for subcollection)
def cleanup():
    print("Running cleanup...")

    now = datetime.now(timezone.utc)

    docs = (
        db.collection_group('chat_history')   # IMPORTANT
        .where("expiry", "<=", now)
        .stream()
    )

    batch = db.batch()
    count = 0
    deleted = 0

    for doc in docs:
        print("Deleting:", doc.reference.path)

        batch.delete(doc.reference)
        count += 1
        deleted += 1

        # Firestore batch safety limit
        if count == 450:
            batch.commit()
            batch = db.batch()
            count = 0

    if count > 0:
        batch.commit()

    print(f"Cleanup completed. Deleted {deleted} messages")

# 🔁 Background loop (runs every 5 minutes)
def background_worker():
    while True:
        try:
            cleanup()
        except Exception as e:
            print("Cleanup error:", e)

        time.sleep(300)  # 300 sec = 5 minutes


# 🚀 Start background thread when server starts
def start_worker():
    thread = threading.Thread(target=background_worker)
    thread.daemon = True
    thread.start()

start_worker()

# 🌐 Required route (Render needs an HTTP endpoint)
@app.route("/")
def home():
    return jsonify({"status": "Flask cleanup worker running"})
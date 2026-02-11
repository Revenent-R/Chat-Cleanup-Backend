import time
import threading
from flask import Flask, jsonify
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)

cred = credentials.Certificate("serviceAccount.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

worker_started = False
worker_lock = threading.Lock()

def cleanup():
    print("Running cleanup...")

    try:
        now = firestore.Timestamp.now()

        docs = (
            db.collection_group("chat_history")
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

            if count == 450:
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        print(f"Cleanup completed. Deleted {deleted} messages")

    except Exception as e:
        print("Cleanup error:", e)


def background_worker():
    print("Background worker started")

    while True:
        cleanup()
        time.sleep(300)


def start_worker_once():
    global worker_started

    with worker_lock:
        if not worker_started:
            print("Starting background worker...")
            thread = threading.Thread(target=background_worker)
            thread.daemon = True
            thread.start()
            worker_started = True


@app.before_request
def init_worker():
    start_worker_once()


@app.route("/")
def home():
    return jsonify({"status": "cleanup worker running"})
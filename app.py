import time
import threading
from flask import Flask, jsonify
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime,timezone

app = Flask(__name__)

cred = credentials.Certificate("serviceAccount.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

worker_started = False
worker_lock = threading.Lock()

def update_last_message_for_chat(safeKey):
    try:
        chat_ref = db.collection("chats").document(safeKey)

        # ✅ Python SDK uses string direction
        latest = (
            chat_ref.collection("chat_history")
            .order_by("time", direction="DESCENDING")
            .limit(1)
            .stream()
        )

        new_message = ""
        new_time = ""

        latest_list = list(latest)
        if len(latest_list) > 0:
            data = latest_list[0].to_dict()
            new_message = data.get("message", "")
            new_time = data.get("time", "")

        details = chat_ref.collection("chat_detail").stream()

        batch = db.batch()
        for d in details:
            batch.update(d.reference, {
                "message": new_message,
                "time": new_time
            })

        batch.commit()
        print(f"Updated last message for chat {safeKey}")

    except Exception as e:
        print("update_last_message_for_chat error:", e)

def cleanup():
    print("Running cleanup...")

    try:
        now = datetime.now(timezone.utc)

        docs = (
            db.collection_group("chat_history")
            .where("expiry", "<=", now)
            .stream()
        )

        batch = db.batch()
        count = 0
        deleted = 0

        affected_chats = set()

        for doc in docs:
            print("Deleting:", doc.reference.path)

            # chats/{safeKey}/chat_history/{msgId}
            history_ref = doc.reference.parent
            chat_doc_ref = history_ref.parent
            safeKey = chat_doc_ref.id if chat_doc_ref else None

            if safeKey:
                affected_chats.add(safeKey)
            affected_chats.add(safeKey)

            batch.delete(doc.reference)
            count += 1
            deleted += 1

            if count == 450:
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        # 🔥 AFTER DELETIONS → update last messages
        for safeKey in affected_chats:
            update_last_message_for_chat(safeKey)

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
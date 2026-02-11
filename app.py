import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

cred = credentials.Certificate("serviceAccount.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

def cleanup():
    now = datetime.now(timezone.utc)

    docs = (
        db.collection("messages")
        .where("expiresAt", "<=", now)
        .stream()
    )

    batch = db.batch()
    count = 0

    for doc in docs:
        batch.delete(doc.reference)
        count += 1

        if count == 450:
            batch.commit()
            batch = db.batch()
            count = 0

    if count > 0:
        batch.commit()

    print("Cleanup completed")

if __name__ == "__main__":
    cleanup()
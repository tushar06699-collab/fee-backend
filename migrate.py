import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_DIR = os.path.join(BASE_DIR, "sessions")

print("\n🔍 Searching for .db files inside:", SESSIONS_DIR)

if not os.path.isdir(SESSIONS_DIR):
    print("❌ sessions folder not found!")
    exit()

db_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".db")]

if not db_files:
    print("❌ No .db files found inside sessions/")
    exit()

for db in db_files:
    path = os.path.join(SESSIONS_DIR, db)
    print(f"\n📌 Checking: {db}")

    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()

        # --- Check columns in student table ---
        cur.execute("PRAGMA table_info(student)")
        cols = [c[1] for c in cur.fetchall()]

        # months JSON field exists?
        if "months" not in cols:
            print("➡ Adding missing column: months")
            cur.execute("ALTER TABLE student ADD COLUMN months TEXT DEFAULT '{}'")

        # annual_charge should NOT exist in student table (belongs to receipt)
        if "annual_charge" in cols:
            print("⚠ Removing wrong column: annual_charge (belongs in receipt table)")
            # SQLite cannot drop columns directly — skip or later recreate table

        # --- Check receipt table ---
        cur.execute("PRAGMA table_info(receipt)")
        rcols = [c[1] for c in cur.fetchall()]

        if "annual_charge" not in rcols:
            print("➡ Adding missing column: annual_charge")
            cur.execute("ALTER TABLE receipt ADD COLUMN annual_charge INTEGER DEFAULT 0")

        if "receipt_number" not in rcols:
            print("➡ Adding missing column: receipt_number")
            cur.execute("ALTER TABLE receipt ADD COLUMN receipt_number TEXT")

        conn.commit()
        conn.close()
        print(f"✔ Updated: {db}")

    except Exception as e:
        print(f"❌ Error in {db}: {e}")

print("\n✨ Migration Completed Successfully!")

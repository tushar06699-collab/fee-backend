# app.py
import os
import json
import traceback
import sqlite3
from typing import Dict, Any

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from extensions import db                # your SQLAlchemy() from extensions.py
from models import Student, Receipt, FeeStructure
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ------------------------
# Config / Sessions setup
# ------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_DIR = os.path.join(BASE_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)

DEFAULT_SESSION = "2024_25"
CURRENT_SESSION = DEFAULT_SESSION

# cache engines and sessionmakers for non-default sessions
ENGINES = {}
SESSIONMAKERS = {}

def session_db_filename(session_name: str) -> str:
    safe = "".join(ch for ch in session_name if ch.isalnum() or ch in ("_", "-"))
    return f"school_{safe}.db"

def session_db_path(session_name: str) -> str:
    return os.path.join(SESSIONS_DIR, session_db_filename(session_name))

def get_db_uri_for_session(session_name: str) -> str:
    return "sqlite:///" + session_db_path(session_name)

def get_previous_session_name(session_name: str):
    try:
        parts = session_name.split("_")
        if len(parts) != 2:
            return None
        start = int(parts[0])
        prev_start = start - 1
        prev_end = prev_start + 1
        prev_end_str = str(prev_end)[2:]  # last two digits
        return f"{prev_start}_{prev_end_str}"
    except Exception:
        return None

# ------------------------
# Monthly + Annual default structure
# ------------------------
def default_month_structure():
    return {
        "Jan": {"status": "Due", "paid": 0, "due": 0},
        "Feb": {"status": "Due", "paid": 0, "due": 0},
        "Mar": {"status": "Due", "paid": 0, "due": 0},
        "Apr": {"status": "Due", "paid": 0, "due": 0},
        "May": {"status": "Due", "paid": 0, "due": 0},
        "Jun": {"status": "Due", "paid": 0, "due": 0},
        "Jul": {"status": "Due", "paid": 0, "due": 0},
        "Aug": {"status": "Due", "paid": 0, "due": 0},
        "Sep": {"status": "Due", "paid": 0, "due": 0},
        "Oct": {"status": "Due", "paid": 0, "due": 0},
        "Nov": {"status": "Due", "paid": 0, "due": 0},
        "Dec": {"status": "Due", "paid": 0, "due": 0},
        "Annual": {"status": "Due", "paid": 0, "due": 0},
    }

def ensure_months_has_annual(months):
    if not isinstance(months, dict):
        months = {}
    if "Annual" not in months:
        months["Annual"] = {"status": "Due", "paid": 0, "due": 0}
    return months

def ensure_months_normalized(months):
    m = months or {}
    for k, v in default_month_structure().items():
        if k not in m:
            m[k] = v.copy()
        else:
            # normalize inner dict
            if not isinstance(m[k], dict):
                m[k] = v.copy()
            else:
                m[k]["status"] = m[k].get("status", "Due")
                m[k]["paid"] = int(m[k].get("paid", 0) or 0)
                m[k]["due"] = int(m[k].get("due", 0) or 0)
    return m

def ensure_session_db_exists(session_name: str):
    """
    Create DB file if missing, ensure tables exist and minimal columns present.
    If created == True, copy students from previous session once.
    """
    db_file = session_db_path(session_name)
    created = False

    if not os.path.exists(db_file):
        open(db_file, "a").close()
        created = True

    if session_name != DEFAULT_SESSION:
        if session_name not in ENGINES:
            uri = get_db_uri_for_session(session_name)
            engine = create_engine(uri, connect_args={"check_same_thread": False})
            ENGINES[session_name] = engine
            SESSIONMAKERS[session_name] = sessionmaker(bind=engine)

            # create tables if missing
            db.metadata.create_all(engine)

            # safe ALTERs for missing columns
            with engine.connect() as conn:
                try:
                    conn.execute(text("ALTER TABLE student ADD COLUMN annual_charge INTEGER DEFAULT 0"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE student ADD COLUMN months TEXT"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE receipt ADD COLUMN receipt_number TEXT"))
                except Exception:
                    pass

    # copy previous session only when db was newly created
    if created:
        try:
            copy_students_from_previous(session_name)
        except Exception:
            print("Carry forward failed for", session_name)
            print(traceback.format_exc())

    return {"created": created, "db_file": db_file}

def get_engine_for_session(session_name: str):
    ensure_session_db_exists(session_name)
    if session_name == DEFAULT_SESSION:
        return db.engine
    return ENGINES.get(session_name)

def get_sessionmaker_for_session(session_name: str):
    ensure_session_db_exists(session_name)
    if session_name == DEFAULT_SESSION:
        return None
    return SESSIONMAKERS.get(session_name)

# ------------------------
# Flask init
# ------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_DATABASE_URI"] = get_db_uri_for_session(DEFAULT_SESSION)
db.init_app(app)

with app.app_context():
    db.create_all()

# ------------------------
# Helpers
# ------------------------
def row_to_student_dict(mapping: Dict[str, Any]):
    months_val = mapping.get("months")
    months = {}
    if months_val:
        if isinstance(months_val, (str, bytes)):
            try:
                months = json.loads(months_val)
            except Exception:
                months = {}
        elif isinstance(months_val, dict):
            months = months_val
        else:
            months = {}
    annual = mapping.get("annual_charge")
    if annual is None:
        try:
            annual_struct = months.get("Annual") if isinstance(months, dict) else None
            annual = int(annual_struct.get("paid", 0)) if annual_struct else 0
        except Exception:
            annual = 0

    return {
        "id": mapping.get("id"),
        "name": mapping.get("name"),
        "father": mapping.get("father"),
        "class_name": mapping.get("class_name"),
        "roll": mapping.get("roll"),
        "previous_due": int(mapping.get("previous_due") or 0),
        "advance": int(mapping.get("advance") or 0),
        "months": months,
        "annual_charge": int(annual or 0)
    }

def normalize_months_structure(months):
    out = {}
    if not months:
        return out
    if isinstance(months, dict):
        for k, v in months.items():
            if v is None:
                out[k] = {"status": "Due", "paid": 0, "due": 0}
                continue
            if isinstance(v, dict):
                status = v.get("status", "Due")
                paid = int(v.get("paid", 0) or 0)
                due = int(v.get("due", 0) or 0)
                out[k] = {"status": status, "paid": paid, "due": due}
            else:
                out[k] = {"status": "Due", "paid": 0, "due": 0}
    return out

def calc_carry_forward_amount(student_row):
    """
    NEW LOGIC:
    Only carry unpaid monthly dues to next session.
    Ignore previous_due originally entered when student was created.
    """
    months = student_row.get("months") or {}
    months_norm = normalize_months_structure(months)

    unpaid_sum = 0
    for mrec in months_norm.values():
        # only use monthly 'due' values
        due_amt = int(mrec.get("due", 0) or 0)
        unpaid_sum += due_amt

    return unpaid_sum   # ❗ONLY unpaid months carried forward

def copy_students_from_previous(new_session_name: str):
    prev = get_previous_session_name(new_session_name)
    if not prev:
        print("No previous session computed for", new_session_name)
        return

    prev_path = session_db_path(prev)
    if not os.path.exists(prev_path):
        print("Previous session DB not found:", prev_path)
        return

    print(f"Carry-forward: copying students from {prev} -> {new_session_name}")

    prev_engine = get_engine_for_session(prev)
    prev_students = []

    if prev == DEFAULT_SESSION:
        with app.app_context():
            s_objs = Student.query.all()
            for s in s_objs:
                prev_students.append({
                    "name": s.name,
                    "father": s.father,
                    "class_name": s.class_name,
                    "roll": s.roll,
                    "previous_due": int(s.previous_due or 0),
                    "advance": int(s.advance or 0),
                    "months": s.months or {},
                    "annual_charge": 0
                })
    else:
        with prev_engine.connect() as conn:
            rows = conn.execute(text("SELECT id, name, father, class_name, roll, previous_due, advance, months, annual_charge FROM student")).fetchall()
            for r in rows:
                m = r._mapping
                months_val = m.get("months")
                months = {}
                if months_val:
                    try:
                        months = json.loads(months_val) if isinstance(months_val, (str, bytes)) else months_val
                    except Exception:
                        months = {}
                prev_students.append({
                    "name": m.get("name"),
                    "father": m.get("father"),
                    "class_name": m.get("class_name"),
                    "roll": m.get("roll"),
                    "previous_due": int(m.get("previous_due") or 0),
                    "advance": int(m.get("advance") or 0),
                    "months": months,
                    "annual_charge": int(m.get("annual_charge") or 0)
                })

    new_engine = get_engine_for_session(new_session_name)
    if new_session_name == DEFAULT_SESSION:
        with app.app_context():
            for s in prev_students:
                exists = Student.query.filter_by(class_name=s["class_name"], roll=str(s["roll"])).first()
                if exists:
                    continue
                carry = calc_carry_forward_amount(s)
                new_student = Student(
                    name=s["name"],
                    father=s["father"],
                    class_name=s["class_name"],
                    roll=str(s["roll"]),
                    previous_due=carry,
                    advance=0,
                    months=ensure_months_normalized(default_month_structure())
                )
                db.session.add(new_student)
            db.session.commit()
    else:
        with new_engine.begin() as conn:
            for s in prev_students:
                existing = conn.execute(text("SELECT id FROM student WHERE class_name=:cls AND roll=:roll"), {"cls": s["class_name"], "roll": str(s["roll"])}).fetchone()
                if existing:
                    continue
                carry = calc_carry_forward_amount(s)
                months_json = json.dumps(ensure_months_normalized(default_month_structure()))
                conn.execute(text(
                    "INSERT INTO student (name, father, class_name, roll, previous_due, advance, months, annual_charge) "
                    "VALUES (:name, :father, :class_name, :roll, :previous_due, :advance, :months, :annual_charge)"
                ), {
                    "name": s["name"],
                    "father": s["father"],
                    "class_name": s["class_name"],
                    "roll": str(s["roll"]),
                    "previous_due": carry,
                    "advance": 0,
                    "months": months_json,
                    "annual_charge": 0
                })

# ------------------------
# before_request: allow preflight and ensure session exists
# ------------------------
@app.before_request
def before_request_switch_db():
    if request.method == "OPTIONS":
        resp = make_response(jsonify({"ok": True, "reason": "preflight"}), 200)
        resp.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type,X-Session"
        return resp

    try:
        sname = get_session_from_request()
        ensure_session_db_exists(sname)
    except Exception as e:
        tb = traceback.format_exc()
        print("Session ensure error:\n", tb)
        resp = make_response(jsonify({"success": False, "error": str(e)}), 500)
        resp.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*")
        return resp

def get_session_from_request():
    s = request.headers.get("X-Session")
    if not s:
        s = request.args.get("session")
    return s or CURRENT_SESSION

# ------------------------
# Create new session (auto)
# ------------------------
@app.route("/session/create_auto", methods=["POST"])
def create_auto_session():
    data = request.get_json() or {}
    from_session = data.get("from_session")
    extra_fee = int(data.get("extra_fee", 0))

    if not from_session:
        return jsonify({"success": False, "message": "Missing from_session"}), 400

    try:
        s, e = from_session.split("_")
        s = int(s) + 1
        e = int(e) + 1
        new_session = f"{s}_{str(e)[-2:]}"
    except Exception:
        return jsonify({"success": False, "message": "Invalid session format"}), 400

    # Ensure DB exists -> ensure_session_db_exists will copy students once if the DB is newly created
    ensure_session_db_exists(new_session)

    # If user wants to add an extra fee, add to previous_due for all students
    if extra_fee != 0:
        new_engine = get_engine_for_session(new_session)
        with new_engine.begin() as conn:
            conn.execute(text("UPDATE student SET previous_due = previous_due + :fee"), {"fee": extra_fee})

    return jsonify({"success": True, "new_session": new_session})

# ------------------------
# Basic routes
# ------------------------
@app.route("/")
def home():
    s = get_session_from_request()
    return jsonify({"success": True, "message": "Backend running", "session": s})

@app.route("/health")
def health():
    s = get_session_from_request()
    return jsonify({"status": "ok", "session": s})

# ------------------------
# STUDENT endpoints
# ------------------------
@app.route("/student/add", methods=["POST"])
def add_student():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No JSON provided"}), 400

    name = data.get("name")
    cls = data.get("class_name")
    roll = str(data.get("roll")) if data.get("roll") is not None else None

    if not name or not cls or roll is None:
        return jsonify({"success": False, "message": "Missing name/class/roll"}), 400

    sname = get_session_from_request()

    if sname == DEFAULT_SESSION:
        with app.app_context():
            if Student.query.filter_by(class_name=cls, roll=roll).first():
                return jsonify({"success": False, "message": "Student exists"}), 200
            months = ensure_months_normalized(ensure_months_has_annual(data.get("months", {})))
            student = Student(
                name=name,
                father=data.get("father"),
                class_name=cls,
                roll=roll,
                previous_due=int(data.get("previous_due", 0) or 0),
                advance=int(data.get("advance", 0) or 0),
                months=months
            )
            db.session.add(student)
            db.session.commit()
            return jsonify({"success": True, "student": student.to_dict()})
    else:
        engine = get_engine_for_session(sname)
        if engine is None:
            return jsonify({"success": False, "message": "Session engine missing"}), 500

        months_struct = ensure_months_normalized(ensure_months_has_annual(data.get("months", {})))
        months_json = json.dumps(months_struct)
        with engine.begin() as conn:
            existing = conn.execute(text("SELECT id FROM student WHERE class_name=:cls AND roll=:roll"), {"cls": cls, "roll": roll}).fetchone()
            if existing:
                return jsonify({"success": False, "message": "Student exists"}), 200
            res = conn.execute(text(
                "INSERT INTO student (name, father, class_name, roll, previous_due, advance, months, annual_charge) "
                "VALUES (:name, :father, :class_name, :roll, :previous_due, :advance, :months, :annual_charge)"
            ), {
                "name": name,
                "father": data.get("father"),
                "class_name": cls,
                "roll": roll,
                "previous_due": int(data.get("previous_due", 0) or 0),
                "advance": int(data.get("advance", 0) or 0),
                "months": months_json,
                "annual_charge": int((data.get("months") or {}).get("Annual", {}).get("paid", 0) or 0)
            })
            lastid = None
            try:
                lastid = res.lastrowid
            except Exception:
                pass
            return jsonify({"success": True, "student": {"id": lastid, "name": name, "class_name": cls, "roll": roll}})

@app.route("/students")
def get_students():
    sname = get_session_from_request()
    if sname == DEFAULT_SESSION:
        with app.app_context():
            students = Student.query.all()
            return jsonify({"success": True, "students": [s.to_dict() for s in students]})
    else:
        engine = get_engine_for_session(sname)
        if engine is None:
            return jsonify({"success": False, "message": "Session engine missing"}), 500

        with engine.connect() as conn:
            res = conn.execute(text("SELECT id, name, father, class_name, roll, previous_due, advance, months, annual_charge FROM student"))
            rows = [row_to_student_dict(r._mapping) for r in res.fetchall()]
            return jsonify({"success": True, "students": rows})

@app.route("/student/<class_name>/<roll>")
def get_single_student(class_name, roll):
    sname = get_session_from_request()
    if sname == DEFAULT_SESSION:
        with app.app_context():
            student = Student.query.filter_by(class_name=class_name, roll=roll).first()
            if not student:
                return jsonify({"success": False, "message": "Not found"}), 404
            return jsonify({"success": True, "student": student.to_dict()})
    else:
        engine = get_engine_for_session(sname)
        with engine.connect() as conn:
            res = conn.execute(text(
                "SELECT id, name, father, class_name, roll, previous_due, advance, months, annual_charge FROM student "
                "WHERE class_name=:cls AND roll=:roll"
            ), {"cls": class_name, "roll": roll})
            row = res.fetchone()
            if not row:
                return jsonify({"success": False, "message": "Not found"}), 404
            return jsonify({"success": True, "student": row_to_student_dict(row._mapping)})

@app.route("/update_student", methods=["POST", "OPTIONS"])
def update_student():
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"})

    data = request.json or {}
    class_name = data.get("class")
    roll = data.get("roll")
    if not class_name or roll is None:
        return jsonify({"success": False, "message": "Missing class or roll"}), 400

    sname = get_session_from_request()
    sdata = data.get("student", {})

    if sname == DEFAULT_SESSION:
        with app.app_context():
            student = Student.query.filter_by(class_name=class_name, roll=roll).first()
            if not student:
                return jsonify({"success": False, "message": "Student not found"}), 404
            student.name = sdata.get("name", student.name)
            student.father = sdata.get("father", student.father)
            student.previous_due = int(sdata.get("previous_due", student.previous_due) or 0)
            student.advance = int(sdata.get("advance", student.advance) or 0)
            months = ensure_months_normalized(ensure_months_has_annual(sdata.get("months", student.months)))
            student.months = months
            try:
                student.annual_charge = int(months.get("Annual", {}).get("paid", 0) or 0)
            except Exception:
                pass
            student.class_name = sdata.get("class_name", student.class_name)
            student.roll = sdata.get("roll", student.roll)
            db.session.commit()
            return jsonify({"success": True, "message": "Updated"})
    else:
        engine = get_engine_for_session(sname)
        months_struct = ensure_months_normalized(ensure_months_has_annual(sdata.get("months", {})))
        months_json = json.dumps(months_struct)
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE student SET name=:name, father=:father, previous_due=:prev, advance=:adv, months=:months, annual_charge=:annual_charge, class_name=:cls, roll=:roll "
                "WHERE class_name=:wcls AND roll=:wroll"
            ), {
                "name": sdata.get("name", sdata.get("name")),
                "father": sdata.get("father", sdata.get("father")),
                "prev": int(sdata.get("previous_due", 0) or 0),
                "adv": int(sdata.get("advance", 0) or 0),
                "months": months_json,
                "annual_charge": int(months_struct.get("Annual", {}).get("paid", 0) or 0),
                "cls": sdata.get("class_name", class_name),
                "roll": sdata.get("roll", roll),
                "wcls": class_name,
                "wroll": roll
            })
            return jsonify({"success": True, "message": "Updated"})

@app.route("/student/delete", methods=["POST"])
def delete_student():
    data = request.json or {}
    cls = data.get("class")
    roll = data.get("roll")
    if not cls or roll is None:
        return jsonify({"success": False, "message": "Missing class or roll"}), 400

    sname = get_session_from_request()
    if sname == DEFAULT_SESSION:
        with app.app_context():
            student = Student.query.filter_by(class_name=cls, roll=roll).first()
            if not student:
                return jsonify({"success": False, "message": "Not found"}), 404
            Receipt.query.filter_by(student_id=student.id).delete()
            db.session.delete(student)
            db.session.commit()
            return jsonify({"success": True, "message": "Deleted"})
    else:
        engine = get_engine_for_session(sname)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM receipt WHERE class_name=:cls AND roll=:roll"), {"cls": cls, "roll": roll})
            conn.execute(text("DELETE FROM student WHERE class_name=:cls AND roll=:roll"), {"cls": cls, "roll": roll})
            return jsonify({"success": True, "message": "Deleted"})

# ------------------------
# RECEIPTS endpoints (with payment application logic)
# ------------------------
def apply_payment_to_student_months_and_prev(months: Dict[str, Any], prev_due: int, payment: int):
    """
    Option A: apply payment to previous_due first, then to months (Jan..Dec then Annual).
    Returns (new_months_dict, new_prev_due, unpaid_left)
    """
    months = months or {}
    months = ensure_months_normalized(months)
    remaining = int(payment or 0)

    # 1) pay previous due
    pay_prev = min(remaining, int(prev_due or 0))
    prev_due = int(prev_due or 0) - pay_prev
    remaining -= pay_prev

    # 2) apply to months in calendar order (Jan..Dec then Annual)
    order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Annual"]
    for m in order:
        if remaining <= 0:
            break
        rec = months.get(m) or {"paid":0,"due":0,"status":"Due"}
        due_amt = int(rec.get("due", 0) or 0)
        if due_amt <= 0:
            continue
        to_pay = min(remaining, due_amt)
        rec["paid"] = int(rec.get("paid", 0) or 0) + to_pay
        rec["due"] = due_amt - to_pay
        if rec["due"] <= 0:
            rec["status"] = "Paid"
            rec["due"] = 0
        else:
            rec["status"] = "Partial"
        months[m] = rec
        remaining -= to_pay

    return months, prev_due, remaining

@app.route("/receipt/add", methods=["POST"])
def add_receipt():
    try:
        data = request.json or {}

        # Required fields
        required = ["name", "father", "class", "roll", "date",
                    "totalPaid", "totalDue", "advance", "months", "receiptKey"]
        for key in required:
            if key not in data:
                return jsonify({"success": False, "message": f"Missing {key}"}), 400

        # Current session name
        sname = get_session_from_request()

        # -------------------------------
        # Normalize months
        # -------------------------------
        months_raw = data.get("months", {})
        months = {}

        if isinstance(months_raw, list):
            for item in months_raw:
                key = item.get("month") or item.get("name")
                if not key:
                    continue
                months[key] = {
                    "paid": int(item.get("paid", 0)),
                    "due": int(item.get("due", 0)),
                    "status": item.get("status", ""),
                    "purpose": item.get("purpose", ""),
                    "extra": int(item.get("extra", 0)),
                    "date": item.get("date", "")
                }

        elif isinstance(months_raw, dict):
            for k, v in months_raw.items():
                if not isinstance(v, dict):
                    months[k] = {"paid": 0, "due": 0, "status": "", "purpose": "",
                                 "extra": 0, "date": ""}
                else:
                    months[k] = {
                        "paid": int(v.get("paid", 0)),
                        "due": int(v.get("due", 0)),
                        "status": v.get("status", ""),
                        "purpose": v.get("purpose", ""),
                        "extra": int(v.get("extra", 0)),
                        "date": v.get("date", "")
                    }

        else:
            months = {}

        months_json = json.dumps(months)
        annual_paid = int(months.get("Annual", {}).get("paid", 0))

        # =========================================================
        # 🟢 DEFAULT SESSION — ORM
        # =========================================================
        if sname == DEFAULT_SESSION:
            with app.app_context():

                # Duplicate check
                existing = Receipt.query.filter_by(receipt_key=data["receiptKey"]).first()
                if existing:
                    return jsonify({
                        "success": True,
                        "message": "Duplicate ignored",
                        "receipt_number": existing.receipt_number
                    })

                # Generate new receipt number
                last = Receipt.query.order_by(Receipt.id.desc()).first()
                seq = (last.id + 1) if last else 1

                receipt_number = f"{sname}-{data['class']}-{data['roll']}-{seq:06d}"

                # Save receipt
                r = Receipt(
                    student_id=None,
                    name=data["name"],
                    father=data["father"],
                    class_name=data["class"],
                    roll=str(data["roll"]),
                    date=data["date"],
                    total_paid=int(data["totalPaid"]),
                    total_due=int(data["totalDue"]),
                    advance=int(data["advance"]),
                    months_json=months_json,
                    receipt_key=data["receiptKey"],
                    annual_charge=annual_paid,
                    receipt_number=receipt_number
                )
                db.session.add(r)
                db.session.commit()

                # Update student monthly dues
                student = Student.query.filter_by(
                    class_name=data["class"],
                    roll=str(data["roll"])
                ).first()

                if student:
                    current_months = ensure_months_normalized(student.months or {})
                    prev_due = int(student.previous_due or 0)
                    payment = int(data["totalPaid"])

                    new_months, new_prev_due, _ = apply_payment_to_student_months_and_prev(
                        current_months, prev_due, payment
                    )

                    student.months = new_months
                    student.previous_due = new_prev_due
                    student.annual_charge = int(new_months.get("Annual", {}).get("paid", 0))
                    db.session.commit()

                return jsonify({
                    "success": True,
                    "message": "Receipt saved",
                    "receipt_number": receipt_number
                })

        # =========================================================
        # 🔵 NON-DEFAULT SESSION — RAW SQL
        # =========================================================
        else:
            engine = get_engine_for_session(sname)
            if not engine:
                return jsonify({"success": False, "message": "Session engine missing"}), 500

            with engine.begin() as conn:

                # Duplicate check
                chk = conn.execute(text(
                    "SELECT receipt_number FROM receipt WHERE receipt_key=:rk"
                ), {"rk": data["receiptKey"]}).fetchone()

                if chk:
                    return jsonify({
                        "success": True,
                        "message": "Duplicate ignored",
                        "receipt_number": chk[0]
                    })

                # Generate next receipt number
                last = conn.execute(text(
                    "SELECT id FROM receipt ORDER BY id DESC LIMIT 1"
                )).fetchone()

                seq = (last[0] + 1) if last else 1
                receipt_number = f"{sname}-{data['class']}-{data['roll']}-{seq:06d}"

                # Insert receipt
                conn.execute(text("""
                    INSERT INTO receipt
                    (student_id, name, father, class_name, roll, date,
                     total_paid, total_due, advance, months_json,
                     receipt_key, annual_charge, receipt_number)
                    VALUES
                    (:sid, :name, :father, :cls, :roll, :date,
                     :paid, :due, :adv, :months,
                     :rk, :annual, :rno)
                """), {
                    "sid": None,
                    "name": data["name"],
                    "father": data["father"],
                    "cls": data["class"],
                    "roll": str(data["roll"]),
                    "date": data["date"],
                    "paid": int(data["totalPaid"]),
                    "due": int(data["totalDue"]),
                    "adv": int(data["advance"]),
                    "months": months_json,
                    "rk": data["receiptKey"],
                    "annual": annual_paid,
                    "rno": receipt_number
                })

                # Update student
                st_row = conn.execute(text("""
                    SELECT id, previous_due, months 
                    FROM student 
                    WHERE class_name=:cls AND roll=:roll
                """), {
                    "cls": data["class"],
                    "roll": str(data["roll"])
                }).fetchone()

                if st_row:
                    m = st_row._mapping
                    prev = int(m["previous_due"] or 0)

                    try:
                        cur_months = json.loads(m["months"])
                    except:
                        cur_months = {}

                    paid = int(data["totalPaid"])

                    new_months, new_prev, _ = apply_payment_to_student_months_and_prev(
                        cur_months, prev, paid
                    )

                    conn.execute(text("""
                        UPDATE student 
                        SET previous_due=:pd, months=:m, annual_charge=:ac 
                        WHERE id=:id
                    """), {
                        "pd": new_prev,
                        "m": json.dumps(new_months),
                        "ac": int(new_months.get("Annual", {}).get("paid", 0)),
                        "id": m["id"]
                    })

            # Return for non-default session
            return jsonify({
                "success": True,
                "message": "Receipt saved",
                "receipt_number": receipt_number
            })

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/receipt/history")
def receipt_history():
    sname = get_session_from_request()

    # ============================================
    # DEFAULT SESSION → ORM
    # ============================================
    if sname == DEFAULT_SESSION:
        with app.app_context():
            records = Receipt.query.order_by(Receipt.id.desc()).all()
            return jsonify({
                "success": True,
                "history": [
                    {
                        "id": r.id,
                        "name": r.name,
                        "father": r.father,
                        "class_name": r.class_name,
                        "roll": r.roll,
                        "date": r.date,
                        "total_paid": r.total_paid,
                        "total_due": r.total_due,
                        "advance": r.advance,
                        "annual_charge": r.annual_charge,
                        "receipt_number": r.receipt_number,
                        "months": json.loads(r.months_json) if r.months_json else {},
                    }
                    for r in records
                ]
            })

    # ============================================
    # NON-DEFAULT SESSION → RAW SQL
    # ============================================
    else:
        engine = get_engine_for_session(sname)
        if not engine:
            return jsonify({"success": False, "message": "Session engine missing"}), 500

        with engine.connect() as conn:
            res = conn.execute(text("""
                SELECT id, name, father, class_name, roll, date,
                       total_paid, total_due, advance,
                       months_json, annual_charge, receipt_number
                FROM receipt
                ORDER BY id DESC
            """))

            result = res.fetchall()

            # 🔥 FIX: use _mapping for safe dict conversion
            rows = [dict(r._mapping) for r in result]

            return jsonify({
                "success": True,
                "history": [
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "father": row["father"],
                        "class_name": row["class_name"],
                        "roll": row["roll"],
                        "date": row["date"],
                        "total_paid": row["total_paid"],
                        "total_due": row["total_due"],
                        "advance": row["advance"],
                        "annual_charge": row.get("annual_charge", 0),
                        "receipt_number": row.get("receipt_number"),
                        "months": json.loads(row["months_json"]) if row["months_json"] else {},
                    }
                    for row in rows
                ]
            })
# ------------------------
@app.route("/fees/get")
def fees_get():
    session = request.headers.get("X-Session", DEFAULT_SESSION)
    db_path = os.path.join(SESSIONS_DIR, f"school_{session}.db")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fee_structure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_name TEXT UNIQUE,
            monthly_fee INTEGER DEFAULT 0,
            annual_charge INTEGER DEFAULT 0
        )
    """)

    try:
        cur.execute("ALTER TABLE fee_structure ADD COLUMN annual_charge INTEGER DEFAULT 0")
    except:
        pass

    default_classes = [
        "Nursery", "LKG", "UKG",
        "1st", "2nd", "3rd", "4th", "5th",
        "6th", "7th", "8th",
        "9th", "10th",
        "11th Arts", "12th Arts"
    ]

    cur.execute("SELECT class_name FROM fee_structure")
    existing = {row["class_name"] for row in cur.fetchall()}

    for cls in default_classes:
        if cls not in existing:
            cur.execute(
                "INSERT INTO fee_structure (class_name, monthly_fee, annual_charge) VALUES (?, ?, ?)",
                (cls, 0, 0)
            )

    conn.commit()
    cur.execute("SELECT * FROM fee_structure ORDER BY id")
    rows = cur.fetchall()
    conn.close()

    return jsonify({"success": True, "fees": [dict(r) for r in rows]})

@app.route("/fees/update", methods=["POST"])
def update_fee():
    data = request.json or {}
    cls = data.get("class_name")
    monthly = int(data.get("monthly_fee", 0))
    annual = int(data.get("annual_charge", 0))

    if not cls:
        return jsonify({"success": False, "message": "Missing class_name"}), 400

    sname = get_session_from_request()
    db_path = os.path.join(SESSIONS_DIR, f"school_{sname}.db")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        cur.execute("ALTER TABLE fee_structure ADD COLUMN annual_charge INTEGER DEFAULT 0")
    except:
        pass

    cur.execute(
        "UPDATE fee_structure SET monthly_fee=?, annual_charge=? WHERE class_name=?",
        (monthly, annual, cls)
    )

    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO fee_structure (class_name, monthly_fee, annual_charge) VALUES (?, ?, ?)",
            (cls, monthly, annual)
        )

    conn.commit()
    conn.close()

    return jsonify({"success": True, "message": "Updated"})

@app.route("/fees/setup_defaults")
def setup_fees():
    defaults = {
        "Nursery": 1200, "LKG": 1300, "UKG": 1300,
        "1st": 1300, "2nd": 1300, "3rd": 1300, "4th": 1400, "5th": 1400,
        "6th": 1500, "7th": 1500, "8th": 1700, "9th": 1900, "10th": 1900,
        "11th_Medical": 2200, "11th_Commerce": 2100, "11th_Art": 2100,
        "12th_Medical": 2200, "12th_Commerce": 2100, "12th_Art": 2100
    }
    sname = get_session_from_request()
    if sname == DEFAULT_SESSION:
        with app.app_context():
            for c, fee in defaults.items():
                if not FeeStructure.query.filter_by(class_name=c).first():
                    db.session.add(FeeStructure(class_name=c, monthly_fee=fee))
            db.session.commit()
            return jsonify({"success": True, "message": "Inserted"})
    else:
        engine = get_engine_for_session(sname)
        with engine.begin() as conn:
            for c, fee in defaults.items():
                res = conn.execute(text("UPDATE fee_structure SET monthly_fee=:m WHERE class_name=:cls"), {"m": fee, "cls": c})
                if res.rowcount == 0:
                    conn.execute(text("INSERT INTO fee_structure (class_name, monthly_fee) VALUES (:cls, :m)"), {"cls": c, "m": fee})
            return jsonify({"success": True, "message": "Inserted"})

# ------------------------
# SESSION LIST
# ------------------------
@app.route("/session/list")
def session_list():
    files = os.listdir(SESSIONS_DIR)
    sessions = []

    for f in files:
        if f.startswith("school_") and f.endswith(".db"):
            name = f.replace("school_", "").replace(".db", "")
            sessions.append(name)

    if DEFAULT_SESSION not in sessions:
        sessions.append(DEFAULT_SESSION)

    try:
        sessions.sort(key=lambda x: int(x.split("_")[0]))
    except:
        pass

    return jsonify({"success": True, "sessions": sessions})

@app.route("/receipt/delete/<int:id>", methods=["DELETE"])
def delete_receipt(id):
    try:
        sname = get_session_from_request()
        if sname == DEFAULT_SESSION:
            with app.app_context():
                rec = Receipt.query.get(id)
                if not rec:
                    return jsonify({"success": False, "message": "Receipt not found"}), 404
                db.session.delete(rec)
                db.session.commit()
                return jsonify({"success": True, "message": "Receipt deleted"})
        engine = get_engine_for_session(sname)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM receipt WHERE id = :rid"), {"rid": id})
        return jsonify({"success": True, "message": "Receipt deleted"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/receipt/delete_all", methods=["DELETE"])
def delete_all_receipts():
    try:
        sname = get_session_from_request()
        if sname == DEFAULT_SESSION:
            with app.app_context():
                Receipt.query.delete()
                db.session.commit()
                return jsonify({"success": True, "message": "All deleted"})
        engine = get_engine_for_session(sname)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM receipt"))
        return jsonify({"success": True, "message": "All deleted"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

from openpyxl import Workbook
from flask import send_file
import io, json
from sqlalchemy import text

MONTHS_ORDER = [
    "Annual", "Apr", "May", "Jun", "Jul", "Aug",
    "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar",
    "previousDue"
]

@app.route("/export/excel")
def export_excel():
    sname = get_session_from_request()
    engine = get_engine_for_session(sname)

    # ----------------------------------------------------
    # FETCH STUDENTS
    # ----------------------------------------------------
    students = []
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM student")).fetchall()
        for row in rows:
            students.append(row._mapping)

    # ----------------------------------------------------
    # FETCH RECEIPTS
    # ----------------------------------------------------
    receipts = []
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM receipt")).fetchall()
        for row in rows:
            receipts.append(row._mapping)

    # ----------------------------------------------------
    # CREATE WORKBOOK
    # ----------------------------------------------------
    wb = Workbook()

    # ====================================================
    #  SHEET 1: STUDENTS FEE REPORT
    # ====================================================
    ws = wb.active
    ws.title = "Students Fee Report"

    header = ["ID", "Name", "Father", "Class", "Roll", "Previous Due", "Advance"]

    for m in MONTHS_ORDER:
        header.append(f"{m} Paid")
        header.append(f"{m} Status")

    header += ["Total Paid", "Total Due"]

    ws.append(header)

    total_school_paid = 0
    total_school_due = 0
    class_summary = {}  # for summary sheet

    for st in students:
        months = json.loads(st["months"]) if isinstance(st["months"], str) else st["months"]

        row = [
            st["id"], st["name"], st["father"],
            st["class_name"], st["roll"],
            st.get("previous_due", 0),
            st.get("advance", 0)
        ]

        total_paid = 0
        total_due = 0

        for m in MONTHS_ORDER:
            rec = months.get(m, {"paid": 0, "due": 0, "status": "Due"})

            paid = rec.get("paid", 0)
            due = rec.get("due", 0)
            status = rec.get("status", "Due")

            row.append(paid)
            row.append(status)

            total_paid += paid
            total_due += due

        # Add totals
        row.append(total_paid)
        row.append(total_due)

        ws.append(row)

        # For summary totals
        total_school_paid += total_paid
        total_school_due += total_due

        # For class wise summary
        cls = st["class_name"]
        if cls not in class_summary:
            class_summary[cls] = {
                "students": 0,
                "paid": 0,
                "due": 0
            }
        class_summary[cls]["students"] += 1
        class_summary[cls]["paid"] += total_paid
        class_summary[cls]["due"] += total_due

    # ====================================================
    #  SHEET 2: SUMMARY REPORT
    # ====================================================
    ws2 = wb.create_sheet("Summary")

    ws2.append(["SUMMARY REPORT"])
    ws2.append([""])  # blank line

    ws2.append(["Total Students", len(students)])
    ws2.append(["Total Collection (Paid)", total_school_paid])
    ws2.append(["Total Due", total_school_due])
    ws2.append([""])

    ws2.append(["Class", "Students", "Total Paid", "Total Due"])
    for cls, val in class_summary.items():
        ws2.append([cls, val["students"], val["paid"], val["due"]])

    # ====================================================
    #  SHEET 3: RECEIPTS
    # ====================================================
    ws3 = wb.create_sheet("Receipts")

    ws3.append([
        "ID","Name","Father","Class","Roll",
        "Date","Total Paid","Total Due","Advance",
        "Annual Charge","Receipt Number","Months (JSON)"
    ])

    for r in receipts:
        ws3.append([
            r["id"], r["name"], r["father"], r["class_name"], r["roll"],
            r["date"], r["total_paid"], r["total_due"],
            r["advance"], r["annual_charge"], r["receipt_number"],
            r["months_json"]
        ])

    # ----------------------------------------------------
    # EXPORT FILE
    # ----------------------------------------------------
    file_stream = io.BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)

    filename = f"School_Report_{sname}.xlsx"

    return send_file(
        file_stream,
        download_name=filename,
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ------------------------
# Run
# ------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

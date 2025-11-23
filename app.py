from flask import Flask, request, jsonify
from flask_cors import CORS
from models import db, Student, Receipt, FeeStructure
import json

app = Flask(__name__)

# ============================
# CORS CONFIG
# ============================
CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    supports_credentials=True
)

# ============================
# DATABASE
# ============================
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///school.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

with app.app_context():
    db.create_all()


# ============================
# BASIC ROUTES
# ============================
@app.route("/")
def home():
    return jsonify({"success": True, "message": "Backend running"})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ============================
# ADD STUDENT
# ============================
@app.route("/student/add", methods=["POST"])
def add_student():
    data = request.get_json()

    new_student = Student(
        name=data["name"],
        father=data["father"],
        class_name=data["class_name"],
        roll=data["roll"],
        previous_due=data.get("previous_due", 0),
        advance=data.get("advance", 0),
        months=data.get("months", {})
    )

    db.session.add(new_student)
    db.session.commit()

    return jsonify({"success": True, "student": new_student.to_dict()})


# ============================
# GET ALL STUDENTS   ✔ FIXED
# ============================
@app.route("/students")
def get_students():
    students = Student.query.all()

    return jsonify({
        "success": True,
        "students": [s.to_dict() for s in students]
    })


# ============================
# GET ONE STUDENT
# ============================
@app.route("/student/<class_name>/<roll>")
def get_single_student(class_name, roll):
    student = Student.query.filter_by(class_name=class_name, roll=roll).first()

    if not student:
        return jsonify({"success": False, "message": "Not found"}), 404

    return jsonify({"success": True, "student": student.to_dict()})


# ============================
# UPDATE STUDENT
# ============================
@app.route("/update_student", methods=["POST", "OPTIONS"])
def update_student():
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"})

    data = request.json
    class_name = data["class"]
    roll = data["roll"]

    student = Student.query.filter_by(class_name=class_name, roll=roll).first()
    if not student:
        return jsonify({"success": False, "message": "Student not found"}), 404

    sdata = data["student"]

    student.name = sdata.get("name", student.name)
    student.father = sdata.get("father", student.father)
    student.previous_due = sdata.get("previous_due", student.previous_due)
    student.advance = sdata.get("advance", student.advance)
    student.months = sdata.get("months", student.months)

    db.session.commit()

    return jsonify({"success": True, "message": "Updated"})


# ============================
# ADD RECEIPT
# ============================
@app.route("/receipt/add", methods=["POST"])
def add_receipt():
    try:
        data = request.json

        required = ["name", "father", "class", "roll", "date",
                    "totalPaid", "totalDue", "advance", "months", "receiptKey"]

        for key in required:
            if key not in data:
                return jsonify({"success": False, "message": f"Missing {key}"})

        # Prevent duplicate
        existing = Receipt.query.filter_by(receipt_key=data["receiptKey"]).first()
        if existing:
            return jsonify({"success": False, "message": "Duplicate ignored"})

        r = Receipt(
            student_id=None,
            name=data["name"],
            father=data["father"],
            class_name=data["class"],
            roll=data["roll"],
            date=data["date"],
            total_paid=data["totalPaid"],
            total_due=data["totalDue"],
            advance=data["advance"],
            months_json=json.dumps(data["months"]),
            receipt_key=data["receiptKey"]
        )

        db.session.add(r)
        db.session.commit()

        return jsonify({"success": True, "message": "Receipt saved"})

    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# ============================
# RECEIPT HISTORY
# ============================
@app.route("/receipt/history")
def receipt_history():
    records = Receipt.query.order_by(Receipt.id.desc()).all()

    return jsonify({
        "success": True,
        "history": [
            {
                "id": r.id,
                "name": r.name,
                "father": r.father,
                "class": r.class_name,
                "roll": r.roll,
                "date": r.date,
                "totalPaid": r.total_paid,
                "totalDue": r.total_due,
                "advance": r.advance,
                "months": json.loads(r.months_json),
            }
            for r in records
        ]
    })


# ============================
# DELETE STUDENT
# ============================
@app.route("/student/delete", methods=["POST"])
def delete_student():
    data = request.json
    cls = data["class"]
    roll = data["roll"]

    student = Student.query.filter_by(class_name=cls, roll=roll).first()
    if not student:
        return jsonify({"success": False, "message": "Not found"})

    Receipt.query.filter_by(student_id=student.id).delete()
    db.session.delete(student)
    db.session.commit()

    return jsonify({"success": True, "message": "Deleted"})


# ============================
# FEE SYSTEM
# ============================
@app.route("/fees/get")
def get_fees():
    fees = FeeStructure.query.all()
    return jsonify({"success": True, "fees": [f.to_dict() for f in fees]})


@app.route("/fees/update", methods=["POST"])
def update_fee():
    data = request.json
    cls = data["class_name"]

    fee = FeeStructure.query.filter_by(class_name=cls).first()
    if not fee:
        fee = FeeStructure(class_name=cls, monthly_fee=data["monthly_fee"])
        db.session.add(fee)
    else:
        fee.monthly_fee = data["monthly_fee"]

    db.session.commit()
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

    for c, fee in defaults.items():
        if not FeeStructure.query.filter_by(class_name=c).first():
            db.session.add(FeeStructure(class_name=c, monthly_fee=fee))

    db.session.commit()
    return jsonify({"success": True, "message": "Inserted"})


# ============================
# RUN SERVER
# ============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

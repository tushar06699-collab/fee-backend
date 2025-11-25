from extensions import db
import json


# ==========================================================
# STUDENT MODEL
# ==========================================================
class Student(db.Model):
    __tablename__ = "student"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(100))
    father = db.Column(db.String(100))
    class_name = db.Column(db.String(50))
    roll = db.Column(db.String(20))

    previous_due = db.Column(db.Integer, default=0)
    advance = db.Column(db.Integer, default=0)

    # months + annual_charge included here
    months = db.Column(db.JSON)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "father": self.father,
            "class_name": self.class_name,
            "roll": self.roll,
            "previous_due": self.previous_due,
            "advance": self.advance,
            "months": self.months or {},
        }


# ==========================================================
# RECEIPT MODEL
# ==========================================================
class Receipt(db.Model):
    __tablename__ = "receipt"

    id = db.Column(db.Integer, primary_key=True)

    student_id = db.Column(
        db.Integer,
        db.ForeignKey("student.id"),
        nullable=True
    )

    name = db.Column(db.String(100))
    father = db.Column(db.String(100))
    class_name = db.Column(db.String(20))
    roll = db.Column(db.String(20))

    date = db.Column(db.String(50))

    total_paid = db.Column(db.Integer, default=0)
    total_due = db.Column(db.Integer, default=0)
    advance = db.Column(db.Integer, default=0)

    # Annual charge collected in this receipt
    annual_charge = db.Column(db.Integer, default=0)

    # Full months breakdown
    months_json = db.Column(db.Text)

    # Prevent duplicate receipts
    receipt_key = db.Column(db.String(200), unique=True)

    # 🔥 THE MISSING FIELD (THIS FIXES EVERYTHING)
    receipt_number = db.Column(db.String(200))

    def to_dict(self):
        return {
            "id": self.id,
            "student_id": self.student_id,
            "name": self.name,
            "father": self.father,
            "class_name": self.class_name,
            "roll": self.roll,
            "date": self.date,
            "total_paid": self.total_paid,
            "total_due": self.total_due,
            "advance": self.advance,
            "annual_charge": self.annual_charge,
            "months": json.loads(self.months_json) if self.months_json else {},
            "receipt_number": self.receipt_number
        }

# ==========================================================
# FEE STRUCTURE MODEL
# ==========================================================
class FeeStructure(db.Model):
    __tablename__ = "fee_structure"

    id = db.Column(db.Integer, primary_key=True)
    class_name = db.Column(db.String(50), unique=True)
    monthly_fee = db.Column(db.Integer, default=0)
    annual_charge = db.Column(db.Integer, default=0)  # ✅ REQUIRED FIELD

    def to_dict(self):
        return {
            "class_name": self.class_name,
            "monthly_fee": self.monthly_fee,
            "annual_charge": self.annual_charge  # ✅ MUST RETURN
        }

# validate.py
from datetime import datetime

def coerce_claim(rec: dict) -> dict | None:
    try:
        id_ = str(rec["id"])
        ndc = str(rec["ndc"])
        npi = str(rec["npi"])
        qty = float(rec["quantity"])
        price = float(rec["price"])
        ts = datetime.fromisoformat(str(rec["timestamp"]))
        if qty <= 0 or price < 0:
            return None
        rec2 = {"id": id_, "ndc": ndc, "npi": npi, "quantity": qty, "price": price, "timestamp": ts.isoformat()}
        rec2["unit_price"] = price / qty
        return rec2
    except Exception:
        return None

def coerce_revert(rec: dict) -> dict | None:
    try:
        return {
            "id": str(rec["id"]),
            "claim_id": str(rec["claim_id"]),
            "timestamp": str(rec["timestamp"]),
        }
    except Exception:
        return None

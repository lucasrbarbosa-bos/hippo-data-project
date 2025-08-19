from datetime import datetime
from typing import Optional, Dict, Any

def coerce_claim(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return a normalized dict or None if invalid.
    Expected fields: id, ndc, npi, quantity, price, timestamp (ISO 8601).
    """
    try:
        id_ = str(rec["id"])
        ndc = str(rec["ndc"])
        npi = str(rec["npi"])
        qty = float(rec["quantity"])
        price = float(rec["price"])
        # Validate timestamp is parseable; store ISO string
        ts = datetime.fromisoformat(str(rec["timestamp"])).isoformat()
        if qty <= 0 or price < 0:
            return None
        unit_price = price / qty
        return {
            "claim_id": id_,
            "ndc": ndc,
            "npi": npi,
            "quantity": qty,
            "price": price,
            "unit_price": unit_price,
            "timestamp": ts,
        }
    except Exception:
        return None

def coerce_revert(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return a normalized dict or None if invalid.
    Expected fields: id, claim_id, timestamp.
    """
    try:
        id_ = str(rec["id"])
        claim_id = str(rec["claim_id"])
        ts = datetime.fromisoformat(str(rec["timestamp"])).isoformat()
        return {"id": id_, "claim_id": claim_id, "timestamp": ts}
    except Exception:
        return None

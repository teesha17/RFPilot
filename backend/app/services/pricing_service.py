from app.db.connection import get_db

def approve_pricing(pricing_id, user):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    UPDATE pricing_calculations
    SET pricing_status='approved',
        approved_by=%s,
        approved_at=NOW()
    WHERE pricing_id=%s AND company_id=%s
    """, (user["user_id"], pricing_id, user["company_id"]))

    if cur.rowcount == 0:
        return False

    conn.commit()
    return True
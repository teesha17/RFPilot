from app.db.connection import get_db

def approve_match(match_id, user):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    UPDATE product_matches
    SET match_status='human_approved',
        is_selected=TRUE,
        reviewed_by=%s,
        reviewed_at=NOW()
    WHERE match_id=%s AND company_id=%s
    """, (user["user_id"], match_id, user["company_id"]))

    if cur.rowcount == 0:
        return False

    conn.commit()
    return True
from app.db.connection import get_db

def get_hitl_requests(company_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT request_id, rfp_id, gap_analysis, status, created_at
    FROM custom_product_requests
    WHERE company_id=%s AND status IN ('pending','in_progress')
    """, (company_id,))

    return cur.fetchall()

def resolve_hitl(request_id, user, body):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    UPDATE custom_product_requests
    SET status='approved',
        resolution_outcome=%s,
        feasibility_notes=%s,
        estimated_cost=%s,
        resolved_at=NOW()
    WHERE request_id=%s AND company_id=%s
    """, (
        body["resolution_outcome"],
        body.get("feasibility_notes"),
        body.get("estimated_cost"),
        request_id,
        user["company_id"]
    ))

    if cur.rowcount == 0:
        return False

    conn.commit()
    return True
from app.db.connection import get_db

def fetch_rfps(company_id, status=None):
    conn = get_db()
    cur = conn.cursor()

    query = """
    SELECT r.rfp_id, r.project_name, r.tender_ref,
           r.deadline, r.status, r.relevance_label,
           c.client_name
    FROM rfp_documents r
    LEFT JOIN client_profile c ON r.issuer_client_id = c.client_id
    WHERE r.company_id = %s
    """

    params = [company_id]

    if status:
        query += " AND r.status = %s"
        params.append(status)

    query += " ORDER BY r.created_at DESC"

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    return [
        {
            "rfp_id": r[0],
            "project_name": r[1],
            "tender_ref": r[2],
            "deadline": r[3],
            "status": r[4],
            "relevance_label": r[5],
            "client_name": r[6]
        } for r in rows
    ]

def get_rfp_items(rfp_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT i.item_id, i.item_no,
           pm.match_id, pm.match_status,
           pc.sku_code, pc.product_name
    FROM rfp_items i
    LEFT JOIN product_matches pm ON i.item_id = pm.rfp_item_id
    LEFT JOIN product_catalog pc ON pm.sku_id = pc.sku_id
    WHERE i.rfp_id=%s
    """, (rfp_id,))

    rows = cur.fetchall()

    items = {}
    for r in rows:
        if r[0] not in items:
            items[r[0]] = {
                "item_id": r[0],
                "item_no": r[1],
                "matches": []
            }

        if r[2]:
            items[r[0]]["matches"].append({
                "match_id": r[2],
                "match_status": r[3],
                "sku_code": r[4],
                "product_name": r[5]
            })

    return list(items.values())


def get_rfp_pricing(rfp_id, company_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT pricing_id, total_bid_price, pricing_status
    FROM pricing_calculations
    WHERE rfp_id=%s AND company_id=%s
    """, (rfp_id, company_id))

    rows = cur.fetchall()

    return [
        {
            "pricing_id": r[0],
            "total_bid_price": r[1],
            "pricing_status": r[2]
        } for r in rows
    ]


def submit_proposal(rfp_id, user):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    UPDATE rfp_documents
    SET status='submitted'
    WHERE rfp_id=%s AND company_id=%s
    """, (rfp_id, user["company_id"]))

    conn.commit()


def get_rfp_detail(rfp_id, company_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT r.rfp_id,
           r.project_name,
           r.tender_ref,
           r.deadline,
           r.status,
           r.relevance_label,
           c.client_id,
           c.client_name,
           c.client_type
    FROM rfp_documents r
    LEFT JOIN client_profile c
        ON r.issuer_client_id = c.client_id
    WHERE r.rfp_id = %s
      AND r.company_id = %s
    """, (rfp_id, company_id))

    r = cur.fetchone()

    if not r:
        return None

    return {
        "rfp_id": r[0],
        "project_name": r[1],
        "tender_ref": r[2],
        "deadline": r[3],
        "status": r[4],
        "relevance_label": r[5],
        "client": {
            "client_id": r[6],
            "client_name": r[7],
            "client_type": r[8]
        }
    }
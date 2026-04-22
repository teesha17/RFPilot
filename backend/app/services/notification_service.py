from app.db.connection import get_db

def get_notifications(user):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT notification_id, type, message, rfp_id, is_read, created_at
    FROM notifications
    WHERE user_id=%s AND company_id=%s AND is_read=FALSE
    """, (user["user_id"], user["company_id"]))

    return cur.fetchall()

def mark_read(notification_id, user):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    UPDATE notifications
    SET is_read=TRUE, read_at=NOW()
    WHERE notification_id=%s AND user_id=%s
    """, (notification_id, user["user_id"]))

    conn.commit()

def mark_notification_read(notification_id, user):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    UPDATE notifications
    SET is_read=TRUE
    WHERE notification_id=%s AND user_id=%s
    """, (notification_id, user["user_id"]))

    conn.commit()
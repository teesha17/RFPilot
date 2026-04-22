from app.db.connection import get_db
from app.core.security import verify_password, create_access_token, hash_password
import uuid

def login_user(email, password):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    SELECT user_id, company_id, role, password_hash
    FROM users
    WHERE email=%s AND is_active=TRUE
    """, (email,))

    user = cur.fetchone()

    if not user:
        return None, "User not found"

    if not verify_password(password, user[3]):
        return None, "Wrong password"

    token = create_access_token({
        "user_id": str(user[0]),
        "company_id": str(user[1]),
        "role": user[2]
    })

    return {
        "token": token,
        "user_id": user[0],
        "role": user[2]
    }, None


def register_user(body):
    conn = get_db()
    cur = conn.cursor()

    # check if user exists
    cur.execute("SELECT 1 FROM users WHERE email=%s", (body["email"],))
    if cur.fetchone():
        return None, "User already exists"

    user_id = str(uuid.uuid4())
    password_hash = hash_password(body["password"])

    # if company_id not provided → create new company
    company_id = body.get("company_id") or str(uuid.uuid4())

    cur.execute("""
    INSERT INTO users (user_id, email, password_hash, role, company_id, is_active)
    VALUES (%s, %s, %s, %s, %s, TRUE)
    """, (
        user_id,
        body["email"],
        password_hash,
        body.get("role", "sales_executive"),
        company_id
    ))

    conn.commit()

    return {
        "user_id": user_id,
        "company_id": company_id,
        "email": body["email"]
    }, None
from fastapi import APIRouter, HTTPException
from app.services.auth_service import login_user, register_user

router = APIRouter()

@router.post("/login")
def login(body: dict):
    data, error = login_user(body["email"], body["password"])

    if error:
        raise HTTPException(status_code=401, detail=error)

    return {"success": True, "data": data}

@router.post("/register")
def register(body: dict):
    data, error = register_user(body)

    if error:
        raise HTTPException(status_code=400, detail=error)

    return {
        "success": True,
        "data": data
    }
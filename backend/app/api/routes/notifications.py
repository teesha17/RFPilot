from fastapi import APIRouter, Depends
from app.dependencies.auth import get_current_user
from app.services.notification_service import get_notifications, mark_read
from app.services.notification_service import mark_notification_read

router = APIRouter()

@router.get("/")
def notifications(user=Depends(get_current_user)):
    return {"success": True, "data": get_notifications(user)}

@router.post("/{notification_id}/read")
def read(notification_id: str, user=Depends(get_current_user)):
    mark_read(notification_id, user)
    return {"success": True}

@router.post("/{notification_id}/read")
def read(notification_id: str, user=Depends(get_current_user)):
    mark_notification_read(notification_id, user)
    return {"success": True}
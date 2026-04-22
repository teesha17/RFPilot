from fastapi import APIRouter, Depends
from app.dependencies.roles import require_role
from app.services.hitl_service import get_hitl_requests, resolve_hitl

router = APIRouter()

@router.get("/pending")
def pending(user=Depends(require_role("technical_manager","admin"))):
    return {"success": True, "data": get_hitl_requests(user["company_id"])}

@router.post("/{request_id}/resolve")
def resolve(request_id: str, body: dict, user=Depends(require_role("technical_manager","admin"))):
    resolve_hitl(request_id, user, body)
    return {"success": True}
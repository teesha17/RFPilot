from fastapi import APIRouter, Depends, HTTPException
from app.dependencies.roles import require_role
from app.services.match_service import approve_match

router = APIRouter()

@router.post("/{match_id}/approve")
def approve(match_id: str, user=Depends(require_role("technical_manager","admin"))):
    if not approve_match(match_id, user):
        raise HTTPException(status_code=404)
    return {"success": True}
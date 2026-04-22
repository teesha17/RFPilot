from fastapi import APIRouter, Depends, HTTPException
from app.dependencies.roles import require_role
from app.services.pricing_service import approve_pricing

router = APIRouter()

@router.post("/{pricing_id}/approve")
def approve(pricing_id: str, user=Depends(require_role("pricing_manager","admin"))):
    if not approve_pricing(pricing_id, user):
        raise HTTPException(status_code=404)
    return {"success": True}
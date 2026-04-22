from fastapi import APIRouter, Depends
from app.dependencies.auth import get_current_user
from app.services.rfp_service import (
    fetch_rfps,
    get_rfp_items,
    get_rfp_pricing,
    submit_proposal,
    get_rfp_detail
)
from app.dependencies.roles import require_role
from fastapi import HTTPException

router = APIRouter()

@router.get("/")
def get_rfps(status: str = None, user=Depends(get_current_user)):
    return {"success": True, "data": fetch_rfps(user["company_id"], status)}

@router.get("/{rfp_id}/items")
def items(rfp_id: str, user=Depends(get_current_user)):
    return {"success": True, "data": get_rfp_items(rfp_id)}

@router.get("/{rfp_id}/pricing")
def pricing(rfp_id: str, user=Depends(get_current_user)):
    return {
        "success": True,
        "data": get_rfp_pricing(rfp_id, user["company_id"])
    }

@router.post("/{rfp_id}/proposal/submit")
def submit(rfp_id: str,
           user=Depends(require_role("sales_executive","sales_manager","admin"))):
    
    submit_proposal(rfp_id, user)
    return {"success": True, "message": "Proposal submitted"}

@router.get("/{rfp_id}")
def get_rfp(rfp_id: str, user=Depends(get_current_user)):
    data = get_rfp_detail(rfp_id, user["company_id"])

    if not data:
        raise HTTPException(status_code=404, detail="RFP not found")

    return {
        "success": True,
        "data": data
    }
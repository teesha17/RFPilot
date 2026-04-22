from fastapi import APIRouter, Depends
from app.dependencies.auth import get_current_user
from app.services.catalog_service import get_catalog

router = APIRouter()

@router.get("/")
def catalog(user=Depends(get_current_user)):
    return {"success": True, "data": get_catalog(user["company_id"])}
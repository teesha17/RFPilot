from fastapi import Depends, HTTPException
from app.dependencies.auth import get_current_user

def require_role(*roles):
    def checker(user=Depends(get_current_user)):
        if user["role"] not in roles:
            raise HTTPException(status_code=403, detail="Not allowed")
        return user
    return checker
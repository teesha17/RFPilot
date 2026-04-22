from app.api.routes import auth, catalog, hitl, matches, pricing, rfp
from fastapi import FastAPI
from app.api.routes import notifications

app = FastAPI()

app.include_router(auth.router, prefix="/auth")
app.include_router(rfp.router, prefix="/rfps")
app.include_router(matches.router, prefix="/matches")
app.include_router(pricing.router, prefix="/pricing")
app.include_router(hitl.router, prefix="/hitl")
app.include_router(catalog.router, prefix="/catalog")
app.include_router(notifications.router, prefix="/notifications")
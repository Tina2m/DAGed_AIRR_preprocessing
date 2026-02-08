"""
Refactored main.py - demonstrates the new structure.

This file shows how main.py should look after full refactoring.
The actual main.py will be updated once all units are extracted.
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.config import validate_presto_tools, BASE_DIR
from app.api.routes import router

# Validate pRESTO tools on startup
validate_presto_tools()

# Create FastAPI app
app = FastAPI(title="pRESTO Click-to-Run Backend")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Mount UI static files
app.mount("/ui", StaticFiles(directory="ui", html=True), name="ui")

# Include API routes
app.include_router(router)


#!/usr/bin/env python3
"""
Startup script for AI Batch Processor backend
"""
import uvicorn

if __name__ == "__main__":
    print(" Starting AI Batch Processor Backend...")
    print(" Frontend will be available at: http://localhost:8000")
    print(" API will be available at: http://localhost:8000/docs")
    print(" Auto-reload completely disabled for stability")
    
    uvicorn.run(
        "server.main:socket_app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        reload_dirs=None,
        reload_includes=None,
        reload_excludes=None,
        log_level="info"
    )

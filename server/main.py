from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import socketio
import json
import os
import asyncio
import pandas as pd
from typing import Dict, List, Optional, Any
import aiofiles
from datetime import datetime
import uuid
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for request validation
class AIConfig(BaseModel):
    service: str
    model: str
    api_key: str
    params: Dict[str, Any] = {}
    rate_limit: int = 10

class MappingConfig(BaseModel):
    group_by: str
    main_content: str

class PromptTemplate(BaseModel):
    system: str = ""
    main: str

class OutputConfig(BaseModel):
    format: str
    directory: str
    include_prompt: bool = False
    timestamp_files: bool = True

class ProcessingConfig(BaseModel):
    data_file: str
    ai_config: AIConfig
    mapping: MappingConfig
    prompt_template: PromptTemplate
    output: OutputConfig

app = FastAPI(title="AI Batch Processor API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Socket.IO setup
sio = socketio.AsyncServer(
    async_mode='asgi', 
    cors_allowed_origins='*',
    logger=True,
    engineio_logger=True
)
socket_app = socketio.ASGIApp(sio, app)

# Global state
processing_status = {
    "is_processing": False,
    "is_paused": False,
    "current": 0,
    "total": 0,
    "current_group": "",
    "eta": None,
    "rate": None
}

uploaded_files = {}
processing_results = []

# Create necessary directories
os.makedirs("uploads", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# Mount static files
client_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "client")
app.mount("/static", StaticFiles(directory=client_dir, html=True), name="static")

@app.get("/")
async def serve_index():
    """Serve the main HTML file"""
    return FileResponse(os.path.join(client_dir, "index.html"))

# Socket.IO event handlers
@sio.event
async def connect(sid, environ):
    logger.info(f"Client connected: {sid}")
    # Send initial status to newly connected client
    await sio.emit('connect', {'status': 'connected'}, room=sid)

@sio.event
async def disconnect(sid):
    logger.info(f"Client disconnected: {sid}")

@sio.event
async def join(sid, data):
    """Handle client joining a room"""
    logger.info(f"Client {sid} joining room: {data}")
    await sio.emit('joined', {'room': data}, room=sid)

@sio.event
async def get_status(sid):
    """Send current status to requesting client"""
    logger.info(f"Client {sid} requesting status")
    await sio.emit('status_update', processing_status, room=sid)

@sio.event
async def ping(sid):
    """Handle ping from client"""
    await sio.emit('pong', {'timestamp': datetime.now().isoformat()}, room=sid)

@sio.event
async def error(sid, data):
    """Handle client-side errors"""
    logger.error(f"Client {sid} error: {data}")
    await sio.emit('error_received', {'message': 'Error logged on server'}, room=sid)

async def broadcast_progress(data: Dict[str, Any]):
    """Broadcast progress updates to all connected clients"""
    logger.info(f"Broadcasting progress: {data}")
    await sio.emit('progress_update', data)

async def broadcast_item_completed(index: int, group: str):
    """Broadcast item completion"""
    logger.info(f"Item completed: {index} in group {group}")
    await sio.emit('item_completed', {
        "index": index,
        "group": group
    })

async def broadcast_item_error(index: int, error: str, group: str):
    """Broadcast item error"""
    logger.info(f"Item error: {index} in group {group} - {error}")
    await sio.emit('item_error', {
        "index": index,
        "error": error,
        "group": group
    })

async def broadcast_batch_completed(total_processed: int, total_errors: int):
    """Broadcast batch completion"""
    logger.info(f"Batch completed: {total_processed} processed, {total_errors} errors")
    await sio.emit('batch_completed', {
        "total_processed": total_processed,
        "total_errors": total_errors
    })

async def broadcast_rate_limit_wait(wait_time: float):
    """Broadcast rate limit wait"""
    logger.info(f"Rate limit wait: {wait_time} seconds")
    await sio.emit('rate_limit_wait', {
        "wait_time": wait_time
    })

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload and parse data file"""
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")
        
        # Validate file extension
        allowed_extensions = {'.csv', '.json', '.txt'}
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in allowed_extensions:
            raise HTTPException(status_code=400, detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}")
        
        # Save file
        file_id = str(uuid.uuid4())
        file_path = f"uploads/{file_id}_{file.filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)
        
        # Parse file based on extension
        data = await parse_file(file_path, file_ext)
        
        # Validate that we have data
        if not data or len(data) == 0:
            raise HTTPException(status_code=400, detail="File appears to be empty or contains no valid data")
        
        # Get columns safely
        columns = list(data[0].keys()) if data and len(data) > 0 else []
        
        # Store file info
        uploaded_files[file_id] = {
            "filename": file.filename,
            "file_path": file_path,
            "file_id": file_id,
            "rows": len(data),
            "columns": columns,
            "data": data
        }
        
        return {
            "filename": file.filename,
            "rows": len(data),
            "columns": columns,
            "file_id": file_id
        }
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def parse_file(file_path: str, file_ext: str) -> List[Dict[str, Any]]:
    """Parse uploaded file based on extension"""
    try:
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
        elif file_ext == '.json':
            df = pd.read_json(file_path)
        elif file_ext == '.txt':
            # For txt files, assume one item per line
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            df = pd.DataFrame({'content': [line.strip() for line in lines if line.strip()]})
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        # Check if DataFrame is empty
        if df.empty:
            raise ValueError("File contains no data or all rows are empty")
        
        # Check if DataFrame has any columns
        if len(df.columns) == 0:
            raise ValueError("File has no valid columns")
        
        # Convert to records and filter out completely empty rows
        records = df.to_dict('records')
        filtered_records = []
        
        for record in records:
            # Check if record has any non-empty values
            if any(value is not None and str(value).strip() != '' for value in record.values()):
                filtered_records.append(record)
        
        if not filtered_records:
            raise ValueError("File contains no valid data rows")
        
        return filtered_records
        
    except Exception as e:
        logger.error(f"File parsing error: {str(e)}")
        raise e

@app.post("/start_processing")
async def start_processing(config: ProcessingConfig, background_tasks: BackgroundTasks):
    """Start batch processing"""
    try:
        if processing_status["is_processing"]:
            raise HTTPException(status_code=400, detail="Processing already in progress")
        
        # Validate config
        if not config.data_file:
            raise HTTPException(status_code=400, detail="Data file not specified")
        
        if not config.ai_config.api_key:
            raise HTTPException(status_code=400, detail="API key required")
        
        if not config.prompt_template.main:
            raise HTTPException(status_code=400, detail="Main prompt template required")
        
        # Start processing in background
        background_tasks.add_task(process_batch, config.dict())
        
        return {"status": "started", "message": "Processing started"}
        
    except Exception as e:
        logger.error(f"Start processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def process_batch(config: Dict[str, Any]):
    """Process batch of items"""
    start_time = datetime.now()
    try:
        processing_status["is_processing"] = True
        processing_status["is_paused"] = False
        processing_status["current"] = 0
        processing_status["eta"] = None
        processing_status["rate"] = None
        
        # Find the uploaded file
        file_info = None
        for file_id, info in uploaded_files.items():
            if info["filename"] == config["data_file"]:
                file_info = info
                break
        
        if not file_info:
            raise Exception("Uploaded file not found")
        
        data = file_info["data"]
        processing_status["total"] = len(data)
        
        # Broadcast initial status
        await broadcast_progress({
            "current": 0,
            "total": len(data),
            "group": "Starting...",
            "status": "initializing"
        })
        
        # Group data if specified
        group_by = config.get("mapping", {}).get("group_by")
        if group_by and group_by != "None":
            grouped_data = group_by_column(data, group_by)
        else:
            grouped_data = [{"group": "default", "items": data}]
        
        # Process each group
        for group_info in grouped_data:
            if processing_status["is_paused"]:
                await asyncio.sleep(1)
                continue
            
            group_name = group_info["group"]
            items = group_info["items"]
            
            processing_status["current_group"] = group_name
            
            # Broadcast group start
            await broadcast_progress({
                "current": processing_status["current"],
                "total": processing_status["total"],
                "group": group_name,
                "status": "processing_group"
            })
            
            for i, item in enumerate(items):
                if processing_status["is_paused"]:
                    await asyncio.sleep(1)
                    continue
                
                try:
                    # Process item with AI
                    result = await process_item_with_ai(item, config)
                    
                    # Store result
                    processing_results.append({
                        "item": item,
                        "result": result,
                        "group": group_name,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    processing_status["current"] += 1
                    
                    # Calculate ETA and rate
                    if processing_status["current"] > 1:
                        elapsed_time = (datetime.now() - start_time).total_seconds()
                        if elapsed_time > 0:
                            rate = processing_status["current"] / elapsed_time
                            eta = (processing_status["total"] - processing_status["current"]) / rate
                            processing_status["rate"] = rate
                            processing_status["eta"] = eta
                    
                    # Broadcast progress
                    await broadcast_progress({
                        "current": processing_status["current"],
                        "total": processing_status["total"],
                        "group": group_name,
                        "status": "processing_item"
                    })
                    
                    # Broadcast item completion
                    await broadcast_item_completed(i, group_name)
                    
                    # Small delay to prevent overwhelming
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error processing item {i}: {str(e)}")
                    await broadcast_item_error(i, str(e), group_name)
        
        # Processing completed
        processing_status["is_processing"] = False
        processing_status["is_paused"] = False
        
        await broadcast_batch_completed(processing_status["current"], 0)
        
        # Final status update
        await broadcast_progress({
            "current": processing_status["current"],
            "total": processing_status["total"],
            "group": "Completed",
            "status": "completed"
        })
        
    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}")
        processing_status["is_processing"] = False
        processing_status["is_paused"] = False
        
        # Broadcast error
        await broadcast_progress({
            "current": processing_status["current"],
            "total": processing_status["total"],
            "group": "Error",
            "status": "error",
            "error": str(e)
        })

def group_by_column(data: List[Dict[str, Any]], column: str) -> List[Dict[str, Any]]:
    """Group data by specified column"""
    groups = {}
    for item in data:
        group_value = item.get(column, "unknown")
        if group_value not in groups:
            groups[group_value] = []
        groups[group_value].append(item)
    
    return [{"group": k, "items": v} for k, v in groups.items()]

async def process_item_with_ai(item: Dict[str, Any], config: Dict[str, Any]) -> str:
    """Process single item with AI service"""
    try:
        ai_config = config["ai_config"]
        service = ai_config.get("service", "openai")
        model = ai_config.get("model", "gpt-4o")
        api_key = ai_config.get("api_key")
        
        if not api_key:
            raise Exception("API key not provided")
        
        # Prepare prompt
        prompt_template = config["prompt_template"]["main"]
        system_prompt = config["prompt_template"].get("system", "")
        
        # Replace variables in prompt
        prompt = prompt_template
        for key, value in item.items():
            if isinstance(value, str):
                prompt = prompt.replace(f"{{{key}}}", value)
        
        # Call AI service
        if service == "openai":
            result = await call_openai(api_key, model, prompt, system_prompt, ai_config.get("params", {}))
        elif service == "anthropic":
            result = await call_anthropic(api_key, model, prompt, system_prompt, ai_config.get("params", {}))
        else:
            raise Exception(f"Unsupported AI service: {service}")
        
        return result
        
    except Exception as e:
        logger.error(f"AI processing error: {str(e)}")
        raise e

async def call_openai(api_key: str, model: str, prompt: str, system_prompt: str, params: Dict[str, Any]) -> str:
    """Call OpenAI API"""
    try:
        import openai
        client = openai.AsyncOpenAI(api_key=api_key)
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=params.get("temperature", 0.7),
            max_tokens=params.get("max_tokens", 1000)
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"OpenAI API error: {str(e)}")
        raise e

async def call_anthropic(api_key: str, model: str, prompt: str, system_prompt: str, params: Dict[str, Any]) -> str:
    """Call Anthropic API"""
    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=api_key)
        
        system_content = system_prompt if system_prompt else "You are a helpful AI assistant."
        
        response = await client.messages.create(
            model=model,
            max_tokens=params.get("max_tokens", 1000),
            system=system_content,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text
        
    except Exception as e:
        logger.error(f"Anthropic API error: {str(e)}")
        raise e

@app.post("/pause_processing")
async def pause_processing():
    """Pause or resume processing"""
    try:
        if not processing_status["is_processing"]:
            raise HTTPException(status_code=400, detail="No processing in progress")
        
        processing_status["is_paused"] = not processing_status["is_paused"]
        status = "paused" if processing_status["is_paused"] else "resumed"
        
        return {"status": status}
        
    except Exception as e:
        logger.error(f"Pause processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/stop_processing")
async def stop_processing():
    """Stop processing"""
    try:
        if not processing_status["is_processing"]:
            raise HTTPException(status_code=400, detail="No processing in progress")
        
        processing_status["is_processing"] = False
        processing_status["is_paused"] = False
        
        return {"status": "stopped"}
        
    except Exception as e:
        logger.error(f"Stop processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_status")
async def get_status():
    """Get current processing status"""
    return processing_status

@app.get("/export_results")
async def export_results():
    """Export processing results"""
    try:
        if not processing_results:
            raise HTTPException(status_code=400, detail="No results to export")
        
        # Create output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"outputs/results_{timestamp}.json"
        
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(processing_results, indent=2, ensure_ascii=False))
        
        return FileResponse(
            output_file,
            media_type='application/json',
            filename=f"results_{timestamp}.json"
        )
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test_socket")
async def test_socket():
    """Test Socket.IO connection"""
    try:
        # Broadcast a test message to all connected clients
        await sio.emit('test_message', {
            'message': 'Socket.IO connection test successful!',
            'timestamp': datetime.now().isoformat(),
            'connected_clients': len(sio.rooms)
        })
        return {"status": "success", "message": "Test message sent to all clients"}
    except Exception as e:
        logger.error(f"Socket test error: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/socket_info")
async def socket_info():
    """Get Socket.IO connection information"""
    try:
        return {
            "connected_clients": len(sio.rooms),
            "rooms": list(sio.rooms.keys()),
            "server_time": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Socket info error: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(socket_app, host="0.0.0.0", port=8000)

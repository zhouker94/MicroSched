import requests
import time
import threading
import re
import uuid
import subprocess
import os
import importlib
import sys
import json
import uvicorn
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from models import WorkerInfo, WorkerID, Task, TaskPayload
from log_utils import setup_logger
from exceptions import MasterConnectionError, TaskExecutionError

MASTER_URL = "http://127.0.0.1:8000"
WORKER_ID = WorkerID(f"wrk-{uuid.uuid4().hex[:6]}")
WORKER_PORT = int(sys.argv[1]) if len(
    sys.argv) > 1 else 8001  # Dynamic HTTP server port
WORKER_URL = f"http://127.0.0.1:{WORKER_PORT}"

logger = setup_logger("worker")


class LocalObjectStore:
    """Handles saving task results to the local disk"""

    def __init__(self, worker_id: str):
        self.storage_dir = f".microsched_objects/{worker_id}"
        os.makedirs(self.storage_dir, exist_ok=True)

    def put(self, object_id: str, data: dict):
        filepath = os.path.join(self.storage_dir, f"{object_id}.json")
        with open(filepath, 'w') as f:
            json.dump(data, f)

    def get(self, object_id: str) -> dict:
        filepath = os.path.join(self.storage_dir, f"{object_id}.json")
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r') as f:
            return json.load(f)


local_store = LocalObjectStore(WORKER_ID)
local_actors: dict = {}  # Global memory dictionary to store live Actor objects

# Use a requests Session to enable HTTP Keep-Alive (Connection Pooling)
http_client = requests.Session()


def send_heartbeat():
    """Infinite heartbeat loop"""
    while True:
        try:
            info = WorkerInfo(worker_id=WORKER_ID, http_url=WORKER_URL)
            res = http_client.post(f"{MASTER_URL}/register",
                                   json=info.model_dump())
            if res.status_code == 200:
                logger.debug(f"💓 [{WORKER_ID}] Heartbeat sent successfully")
        except requests.exceptions.RequestException as e:
            logger.warning(
                f"❌ [{WORKER_ID}] Cannot reach Master: {e}, will retry..."
            )
        except Exception as e:
            logger.error(f"❌ [{WORKER_ID}] Unexpected error in heartbeat: {e}")

        # Sleep 2 seconds between heartbeats
        time.sleep(2)


def resolve_dependencies(payload: TaskPayload) -> TaskPayload:
    """Safely scans the payload for {ref:task_id} and replaces them recursively via P2P fetch."""
    payload_dict = payload.model_dump()
    # Dump to string just for fast regex scanning
    payload_str = json.dumps(payload_dict)
    pattern = re.compile(r"\{ref:(.*?)\}")
    matches = pattern.findall(payload_str)

    if not matches:
        return payload

    ref_map = {}
    for object_id in set(matches):
        logger.info(f"🔍 Resolving object reference: {object_id}")
        try:
            # 1. Ask Master for the location of the object
            loc_res = http_client.get(f"{MASTER_URL}/objects/{object_id}")
            loc_res.raise_for_status()
            worker_url = loc_res.json()["location"]

            # 2. Fetch the actual data directly from the target Worker
            logger.info(f"🚚 Fetching {object_id} directly from {worker_url}")
            data_res = http_client.get(f"{worker_url}/objects/{object_id}")
            data_res.raise_for_status()
            data = data_res.json()

            # Extract stdout as the resolved value, strip trailing newlines
            resolved_value = data.get("stdout", "").strip()
            ref_map[object_id] = resolved_value
            logger.info(f"✨ Resolved {object_id} -> {resolved_value[:30]}...")
        except requests.exceptions.RequestException as e:
            raise TaskExecutionError(
                f"Failed to resolve dependency {object_id}: {e}")

    # Recursively traverse the dictionary to safely replace strings without breaking JSON
    def replace_refs(node):
        if isinstance(node, str):
            for ref_id, val in ref_map.items():
                node = node.replace(f"{{ref:{ref_id}}}", val)
            return node
        elif isinstance(node, list):
            return [replace_refs(item) for item in node]
        elif isinstance(node, dict):
            return {k: replace_refs(v) for k, v in node.items()}
        return node

    resolved_dict = replace_refs(payload_dict)
    return TaskPayload.model_validate(resolved_dict)


def execute_task(task_id: str, payload: TaskPayload) -> dict:
    """Executes a bash command or a Python Actor method. Raises TaskExecutionError on failure."""
    try:
        # 1. Actor Creation Task
        if payload.actor_class:
            class_path = payload.actor_class
            module_name, class_name = class_path.rsplit(".", 1)

            # Dynamically import the Python class
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)

            actor_id = payload.actor_id or task_id
            args = payload.actor_args

            # Instantiate and store in memory
            local_actors[actor_id] = cls(*args)
            msg = f"🎭 Created Actor {class_name} with ID: {actor_id}"
            logger.info(msg)

            return {"stdout": msg, "stderr": ""}

        # 2. Actor Method Call Task
        elif payload.actor_method:
            actor_id = payload.actor_id
            method_name = payload.actor_method
            args = payload.actor_args

            if actor_id not in local_actors:
                raise TaskExecutionError(
                    f"Actor {actor_id} not found in this worker's memory!")

            actor_obj = local_actors[actor_id]
            method = getattr(actor_obj, method_name)

            # Execute the Python method directly in memory
            result = method(*args)
            logger.info(f"🎭 Executed {method_name} on Actor {actor_id}")

            return {"stdout": str(result), "stderr": ""}

        # 3. Actor Destruction Task
        elif payload.destroy_actor:
            actor_id = payload.actor_id
            if actor_id in local_actors:
                del local_actors[actor_id]  # Trigger Python GC
                msg = f"🗑️ Destroyed Actor with ID: {actor_id}"
                logger.info(msg)
                return {"stdout": msg, "stderr": ""}
            else:
                raise TaskExecutionError(
                    f"Actor {actor_id} not found for destruction!")

        # 4. Legacy Stateless Bash Command
        elif payload.command:
            command = payload.command
            process = subprocess.run(
                command, shell=True, capture_output=True, text=True)

            if process.returncode != 0:
                raise TaskExecutionError(
                    f"Command exited with code {process.returncode}",
                    returncode=process.returncode,
                    stdout=process.stdout,
                    stderr=process.stderr.strip()
                )

            logger.info(f"✅ Task {task_id} bash command completed!")
            if process.stdout.strip():
                logger.info(f"📄 Output:\n{process.stdout.strip()}")
            return {"stdout": process.stdout, "stderr": process.stderr}

        else:
            raise TaskExecutionError(
                "Invalid task payload: Missing command or actor instructions.")

    except TaskExecutionError:
        raise
    except Exception as e:
        raise TaskExecutionError(
            f"Subprocess execution failed: {str(e)}") from e


def work_loop():
    """Infinite loop to pull and execute tasks"""
    logger.info(f"👷 Worker [{WORKER_ID}] ready to accept tasks.")

    while True:
        try:
            info = WorkerInfo(worker_id=WORKER_ID, http_url=WORKER_URL)

            try:
                res = http_client.post(
                    f"{MASTER_URL}/tasks/pull", json=info.model_dump())
            except requests.exceptions.RequestException as e:
                raise MasterConnectionError(
                    f"Failed to pull tasks: {e}") from e

            if res.status_code == 404:
                # Worker not yet registered with Master, wait a bit
                time.sleep(1)
                continue

            if res.status_code != 200:
                logger.warning(
                    f"⚠️ Unexpected response from master: {res.status_code}")
                time.sleep(2)
                continue

            data = res.json()
            if "task_id" not in data:
                # Queue is empty, rest a bit before pulling again
                time.sleep(1)
                continue

            task = Task(**data)
            task_id = task.task_id
            payload = task.payload

            logger.info(f"📥 Pulled task: {task_id}")

            try:
                resolved_payload = resolve_dependencies(payload)
                result = execute_task(task_id, resolved_payload)
                status = "completed"

                # Save the massive result to local disk
                local_store.put(task_id, result)
            except TaskExecutionError as e:
                logger.error(f"❌ Task {task_id} failed: {e.args[0]}")
                if e.stderr:
                    logger.error(f"Stderr: {e.stderr}")
                status = "failed"
                result = {
                    "error": e.args[0],
                    "returncode": e.returncode,
                    "stdout": e.stdout,
                    "stderr": e.stderr
                }

            report_payload = {
                "worker_id": WORKER_ID,
                "status": status,
                "result": None if status == "completed" else result
            }

            try:
                report_res = http_client.post(
                    f"{MASTER_URL}/tasks/{task_id}/report", json=report_payload)
                report_res.raise_for_status()  # Raise error if HTTP status is not 2xx
            except requests.exceptions.RequestException as e:
                raise MasterConnectionError(
                    f"Failed to report task status: {e}") from e

        except MasterConnectionError as e:
            logger.error(f"📡 Master Communication Error: {e}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"❌ Unexpected Error in work loop: {e}")
            time.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the background threads tied to the FastAPI server lifecycle."""
    logger.info(
        f"🚀 Starting MicroSched Worker [{WORKER_ID}] on port {WORKER_PORT}...")

    threading.Thread(target=send_heartbeat, daemon=True).start()
    time.sleep(1)
    threading.Thread(target=work_loop, daemon=True).start()

    yield
    logger.info("🛑 Worker shutting down...")


app = FastAPI(title=f"MicroSched Worker {WORKER_ID}", lifespan=lifespan)


@app.get("/objects/{object_id}")
def get_local_object(object_id: str):
    """Endpoint for other workers to fetch data directly from this worker."""
    data = local_store.get(object_id)
    if data is None:
        raise HTTPException(
            status_code=404, detail="Object not found on this worker")
    return data


if __name__ == "__main__":
    # Run the worker as an HTTP server
    uvicorn.run(app, host="127.0.0.1", port=WORKER_PORT)

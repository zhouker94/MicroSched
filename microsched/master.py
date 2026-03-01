from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import uvicorn
import time
import asyncio
from collections import deque
import uuid
from typing import Dict, Deque
from models import WorkerStatus, TaskStatus, Task, TaskSubmission, WorkerInfo, TaskID, WorkerID, TaskReport, WorkerState, ObjectID, ObjectMetadata, TaskPayload, ActorID
from log_utils import setup_logger
from reaper import reaper_thread


# Core state: in-memory registry and task queue
workers: Dict[WorkerID, WorkerState] = {}
tasks_db: Dict[TaskID, Task] = {}
task_queue: Deque[TaskID] = deque()
task_queue_lock = asyncio.Lock()
object_store: Dict[ObjectID, ObjectMetadata] = {}

# Actor routing state
actors_db: Dict[ActorID, WorkerID] = {}
worker_task_queues: Dict[WorkerID, Deque[TaskID]] = {}

# DAG tracking state
task_pending_deps: Dict[TaskID, int] = {}
task_dependents: Dict[TaskID, list[TaskID]] = {}

logger = setup_logger("master")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Initializing MicroSched control plane...")
    reaper_task = asyncio.create_task(reaper_thread(
        workers, tasks_db, task_queue, task_queue_lock))

    yield  # Let the FastAPI application start serving requests

    logger.info("🛑 Shutdown signal received, cleaning up resources...")
    reaper_task.cancel()  # Cancel background task to prevent memory leaks

app = FastAPI(title="MicroSched Master", lifespan=lifespan)


@app.post("/register")
def register_worker(info: WorkerInfo):
    """Worker registration and heartbeat endpoint"""
    if info.worker_id in workers:
        workers[info.worker_id].last_heartbeat = time.time()
        workers[info.worker_id].http_url = info.http_url
    else:
        # Initialize a new worker state
        workers[info.worker_id] = WorkerState(
            last_heartbeat=time.time(), http_url=info.http_url)
        worker_task_queues[info.worker_id] = deque()
    return {"status": "ok", "message": "Heartbeat ACK"}


@app.post("/tasks", status_code=201)
async def submit_task(submission: TaskSubmission):
    """Endpoint for clients to submit new tasks."""
    task_id = TaskID(f"task-{uuid.uuid4().hex[:6]}")

    async with task_queue_lock:
        task = Task(
            task_id=task_id,
            payload=TaskPayload(
                command=submission.command,
                actor_class=submission.actor_class,
                actor_method=submission.actor_method,
                actor_args=submission.actor_args,
                actor_id=submission.actor_id
            ),
            dependencies=submission.dependencies
        )
        tasks_db[task_id] = task
        task_dependents[task_id] = []

        # Calculate how many dependencies are NOT completed yet
        pending_count = 0
        for dep_id in submission.dependencies:
            if dep_id in tasks_db and tasks_db[dep_id].status != TaskStatus.COMPLETED:
                pending_count += 1
                if dep_id not in task_dependents:
                    task_dependents[dep_id] = []
                task_dependents[dep_id].append(task_id)

        task_pending_deps[task_id] = pending_count

        if pending_count == 0:
            # If it's an actor method call and we know where the actor is, route it directly
            if task.payload.actor_method and task.payload.actor_id in actors_db:
                worker_task_queues[actors_db[task.payload.actor_id]].append(
                    task_id)
            else:
                task_queue.append(task_id)

    logger.info(f"📥 Task submitted: {task_id} (Pending deps: {pending_count})")
    return {"task_id": task_id}


@app.post("/tasks/pull")
async def pull_task(info: WorkerInfo):
    """Endpoint for workers to pull a task from the queue."""
    # Ensure the worker is registered
    if info.worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not registered")

    async with task_queue_lock:
        task_id = None
        worker_queue = worker_task_queues.get(info.worker_id)

        # 1. Pull from worker-specific queue first, 2. Fallback to global queue
        if worker_queue and worker_queue:
            task_id = worker_queue.popleft()
        elif task_queue:
            task_id = task_queue.popleft()

        if not task_id:
            return {"message": "No tasks available"}

        task = tasks_db[task_id]

        task.status = TaskStatus.RUNNING
        task.assigned_worker_id = info.worker_id
        workers[info.worker_id].status = WorkerStatus.BUSY

        logger.info(f"📤 Task {task_id} dispatched to worker {info.worker_id}")
        return task


@app.post("/tasks/{task_id}/report")
async def report_task(task_id: TaskID, report: TaskReport):
    """Endpoint for workers to report task completion or failure."""
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")

    task = tasks_db[task_id]

    # Security/Sanity check: ensure the worker reporting is the one assigned
    if task.assigned_worker_id != report.worker_id:
        raise HTTPException(status_code=403, detail="Worker ID mismatch")

    task.status = report.status
    workers[report.worker_id].status = WorkerStatus.IDLE

    # DAG resolution: if task completes, unblock its dependents
    if report.status == TaskStatus.COMPLETED:
        # If the completed task was an Actor Creation, register it to the worker
        if task.payload.actor_class:
            actor_id = ActorID(task.payload.actor_id or task_id)
            actors_db[actor_id] = report.worker_id
            logger.info(
                f"🎭 Registered Actor {actor_id} to Worker {report.worker_id}")

        object_store[ObjectID(task_id)] = ObjectMetadata(
            location=workers[report.worker_id].http_url
        )

        async with task_queue_lock:
            if task_id in task_dependents:
                for dependent_id in task_dependents[task_id]:
                    task_pending_deps[dependent_id] -= 1
                    if task_pending_deps[dependent_id] == 0:
                        if tasks_db[dependent_id].status == TaskStatus.PENDING:
                            logger.info(
                                f"🔓 Task {dependent_id} unblocked and queued!")

                            dep_task = tasks_db[dependent_id]
                            if dep_task.payload.actor_method and dep_task.payload.actor_id in actors_db:
                                worker_task_queues[actors_db[dep_task.payload.actor_id]].append(
                                    dependent_id)
                            else:
                                task_queue.append(dependent_id)
    elif report.status == TaskStatus.FAILED:
        async with task_queue_lock:
            cancel_queue = deque([task_id])
            while cancel_queue:
                current_id = cancel_queue.popleft()
                if current_id in task_dependents:
                    for dependent_id in task_dependents[current_id]:
                        if tasks_db[dependent_id].status == TaskStatus.PENDING:
                            tasks_db[dependent_id].status = TaskStatus.FAILED
                            logger.warning(
                                f"🚫 Task {dependent_id} canceled due to cascading upstream failure")
                            cancel_queue.append(dependent_id)

    logger.info(
        f"✅ Task {task_id} reported as {report.status.value} by {report.worker_id}"
    )

    return {"status": "ok"}


@app.get("/objects/{object_id}")
async def get_object(object_id: ObjectID):
    """Endpoint to retrieve the result of a completed task (Future resolution)."""
    if object_id not in object_store:
        raise HTTPException(
            status_code=404, detail="Object location not found")
    return object_store[object_id]


@app.get("/tasks/{task_id}")
async def get_task_status(task_id: TaskID):
    """Endpoint to check the status and result of a specific task."""
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")

    task = tasks_db[task_id]
    metadata = object_store.get(
        ObjectID(task_id)) if task.status == TaskStatus.COMPLETED else None
    return {"task_id": task.task_id, "status": task.status, "location": metadata.location if metadata else None}


if __name__ == "__main__":
    uvicorn.run("master:app", host="127.0.0.1", port=8000, reload=True)

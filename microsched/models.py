import time
from enum import Enum
from pydantic import BaseModel
from typing import Optional, NewType

WorkerID = NewType("WorkerID", str)
TaskID = NewType("TaskID", str)
ActorID = NewType("ActorID", str)
ObjectID = NewType("ObjectID", str)


class WorkerStatus(str, Enum):
    IDLE = "idle"
    BUSY = "busy"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskPayload(BaseModel):
    command: Optional[str] = None
    actor_class: Optional[str] = None
    actor_method: Optional[str] = None
    actor_args: list = []
    actor_id: Optional[ActorID] = None
    destroy_actor: bool = False


class Task(BaseModel):
    task_id: TaskID
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = time.time()
    assigned_worker_id: Optional[WorkerID] = None
    payload: TaskPayload = TaskPayload()
    dependencies: list[TaskID] = []
    actor_id: Optional[ActorID] = None


class TaskSubmission(BaseModel):
    command: Optional[str] = None
    actor_class: Optional[str] = None
    actor_method: Optional[str] = None
    actor_args: list = []
    actor_id: Optional[ActorID] = None
    destroy_actor: bool = False
    dependencies: list[TaskID] = []


class WorkerInfo(BaseModel):
    worker_id: WorkerID
    http_url: str


class WorkerState(BaseModel):
    last_heartbeat: float
    status: WorkerStatus = WorkerStatus.IDLE
    http_url: str


class TaskReport(BaseModel):
    worker_id: WorkerID
    status: TaskStatus
    result: Optional[dict] = None


class ObjectMetadata(BaseModel):
    location: str
    size_bytes: Optional[int] = None
    reference_count: int = 0
    created_at: float = time.time()

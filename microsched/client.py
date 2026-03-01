import requests
import time
import uuid
from typing import List, Optional

MASTER_URL = "http://127.0.0.1:8000"


class ObjectRef:
    """
    This is the 'Future' object. It acts as a reference to a value 
    that is currently being computed by the cluster.
    """

    def __init__(self, task_id: str):
        self.task_id = task_id

    def __repr__(self):
        return f"ObjectRef({self.task_id})"


def submit(
    command: Optional[str] = None,
    actor_class: Optional[str] = None,
    actor_method: Optional[str] = None,
    actor_args: Optional[list] = None,
    actor_id: Optional[str] = None,
    destroy_actor: bool = False,
    dependencies: Optional[List[ObjectRef]] = None
) -> ObjectRef:
    """
    Submits a task asynchronously. Returns a Future (ObjectRef) immediately.
    """
    deps = [ref.task_id for ref in (dependencies or [])]

    payload = {
        "command": command,
        "actor_class": actor_class,
        "actor_method": actor_method,
        "actor_args": actor_args or [],
        "actor_id": actor_id,
        "destroy_actor": destroy_actor,
        "dependencies": deps
    }
    res = requests.post(f"{MASTER_URL}/tasks", json=payload)
    res.raise_for_status()

    task_id = res.json()["task_id"]
    return ObjectRef(task_id)


def get(object_ref: ObjectRef) -> str:
    """
    Blocks and waits until the Future is resolved (task completes), 
    then fetches and returns the actual result.
    """
    print(f"⏳ Waiting for {object_ref} to resolve...")
    while True:
        res = requests.get(f"{MASTER_URL}/tasks/{object_ref.task_id}")
        res.raise_for_status()
        data = res.json()

        if data["status"] == "completed":
            # 1. Ask Master for the location of the object
            loc_res = requests.get(
                f"{MASTER_URL}/objects/{object_ref.task_id}")
            worker_url = loc_res.json()["location"]

            # 2. Fetch the actual data directly from the target Worker
            obj_res = requests.get(
                f"{worker_url}/objects/{object_ref.task_id}")
            return obj_res.json().get("stdout", "").strip()

        elif data["status"] == "failed":
            raise Exception(f"Task {object_ref.task_id} failed!")

        # Polling interval (in a real system, we'd use WebSockets or long-polling)
        time.sleep(0.5)


class ActorProxy:
    """
    A handle to a stateful Actor running on a remote Worker.
    """

    def __init__(self, actor_class: str, *args):
        self.actor_id = f"actor-{uuid.uuid4().hex[:8]}"
        self.creation_ref = submit(
            actor_class=actor_class,
            actor_args=list(args),
            actor_id=self.actor_id
        )

    def submit(self, method_name: str, *args, dependencies: Optional[List[ObjectRef]] = None) -> ObjectRef:
        deps = dependencies or []
        deps.append(self.creation_ref)
        return submit(
            actor_method=method_name,
            actor_args=list(args),
            actor_id=self.actor_id,
            dependencies=deps
        )

    def destroy(self, dependencies: Optional[List[ObjectRef]] = None) -> ObjectRef:
        """Submits a task to destroy the actor to free up worker memory."""
        deps = dependencies or []
        deps.append(self.creation_ref)
        return submit(
            actor_id=self.actor_id,
            destroy_actor=True,
            dependencies=deps
        )

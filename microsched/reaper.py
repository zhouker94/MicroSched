import time
import asyncio
from typing import Dict, Deque
from models import WorkerID, TaskID, Task, WorkerState, TaskStatus
from log_utils import setup_logger

logger = setup_logger("reaper")


async def reaper_thread(
    workers: Dict[WorkerID, WorkerState],
    tasks_db: Dict[TaskID, Task],
    task_queue: Deque[TaskID],
    task_queue_lock: asyncio.Lock
):
    """
    Periodically checks for dead workers and reclaims their tasks.
    """
    logger.info("🧟 Reaper thread started, watching for dead workers...")
    while True:
        now = time.time()
        dead_workers = []
        for wid, state in list(workers.items()):
            if now - state.last_heartbeat > 5:
                logger.warning(
                    f"💀 Detected worker down: {wid}, reclaiming tasks...")
                dead_workers.append(wid)

        if dead_workers:
            async with task_queue_lock:
                for wid in dead_workers:
                    # Reclaim tasks that were running on the dead worker
                    for task in tasks_db.values():
                        if task.assigned_worker_id == wid and task.status == TaskStatus.RUNNING:
                            logger.info(f"♻️ Reclaiming task {task.task_id}")
                            task.status = TaskStatus.PENDING
                            task.assigned_worker_id = None
                            # Add task back to the front of the queue for high-priority retry
                            task_queue.appendleft(task.task_id)

                    # Remove the worker from the registry
                    del workers[wid]

        await asyncio.sleep(2)

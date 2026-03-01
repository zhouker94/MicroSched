# MicroSched (msched) 🚀

MicroSched is a lightweight, high-performance distributed computing framework inspired by [Ray](https://github.com/ray-project/ray).

## Core Concepts (Vision)

To efficiently support diverse, large-scale computing workloads, MicroSched is designed around several advanced distributed system patterns:

*   **Distributed Object Store (Pass-by-Reference):** Instead of passing heavy payloads (e.g., large datasets or media files) through the Master, workers exchange data directly via peer-to-peer object references (`ObjectRef`).
*   **DAG Scheduling (Task Dependencies):** Tasks can return Futures. Downstream tasks can use these Futures as inputs, allowing the scheduler to automatically resolve dependencies and orchestrate complex Directed Acyclic Graphs (DAGs).
*   **Stateful Actors:** Support for stateful workers (Actors) to keep large application states or datasets in memory across multiple task executions, eliminating repetitive loading overhead.
*   **Decentralized Scheduling:** Shifting from a global Master bottleneck to a bottom-up approach, where local nodes handle their own queues and spill over to the global control plane only when necessary.

## Architecture

- **Master (Control Plane):** Manages cluster state, worker registry, and global DAG scheduling.
- **Worker (Data Plane):** Executes tasks, manages local objects, and reports heartbeats/metrics.
- **Reaper:** A background process ensuring fault tolerance by detecting dead workers and rescheduling their tasks.

## Roadmap

- [x] Basic Master/Worker architecture with Pull-based scheduling.
- [x] Fault tolerance (Reaper thread for dead worker detection).
- [x] **Phase 1: Futures & DAGs**
  - [x] Implement `ObjectRef` (Futures) for task outputs.
  - [x] Add `dependencies` tracking in the Master.
  - [x] Block task dispatching until dependencies are `COMPLETED`.
- [x] **Phase 2: Distributed Object Store (Done)**
  - [x] Workers store large outputs locally.
  - [x] P2P data fetching between workers using `ObjectRef`.
- [x] **Phase 3: Stateful Actors (Done)**
  - [x] Introduce dedicated workers for stateful computation (Actor registry).
  - [x] Route actor-specific tasks to the correct worker.
- [ ] **Phase 4: Optimization**
  - [ ] Bottom-up scheduling (Local fast-path dispatching).

## Getting Started

### 1. Start the Master Node
The master node acts as the control plane (default port 8000).
```bash
python master.py
```

### 2. Start Worker Nodes
You can start multiple workers on the same machine by specifying different ports.
```bash
python worker.py 8001
python worker.py 8002
```

### 3. Submit a Workflow
Use the provided Python client to submit tasks with dependencies:
```python
import client
future_a = client.submit("echo 'Hello MicroSched'")
future_b = client.submit(f"echo The result was: {{ref:{future_a.task_id}}}", dependencies=[future_a])
print(client.get(future_b))
```

```python
import client
counter = client.ActorProxy("collections.Counter")
future_a = counter.submit("update", ["apple", "apple", "banana"])
future_b = counter.submit("most_common", 1, dependencies=[future_a])
result = client.get(future_b)
counter.destroy(dependencies=[future_b])
```

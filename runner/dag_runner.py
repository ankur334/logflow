from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any
from runner.cli_runner import run_pipeline

DAGType = Dict[str, Dict[str, Any]]


def run_dag(dag: DAGType, max_workers: int = 4) -> None:
    completed, failed, in_flight = set(), set(), {}
    attempts = {n: 0 for n in dag}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while len(completed | failed) < len(dag):
            ready = [
                n for n, nd in dag.items()
                if n not in completed and n not in failed and n not in in_flight
                   and all(dep in completed for dep in nd.get("depends_on", []))
            ]
            for n in ready:
                nd = dag[n]
                attempts[n] += 1
                fut = pool.submit(run_pipeline, nd["pipeline"], **nd.get("params", {}))
                in_flight[fut] = n
            if not in_flight: break
            for fut in as_completed(list(in_flight.keys())):
                n = in_flight.pop(fut)
                nd = dag[n];
                retries = int(nd.get("retries", 0))
                try:
                    fut.result()
                    completed.add(n)
                except Exception:
                    if attempts[n] <= retries + 1:
                        # re-queue next loop
                        pass
                    else:
                        failed.add(n)
                break
    if failed:
        raise RuntimeError(f"DAG failed: {sorted(failed)}")

import sys

from runner.cli_runner import run_pipeline
from runner.dag_runner import run_dag
from utils.log import setup_logging

if __name__ == "__main__":
    setup_logging()
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python main.py pipeline <name> [k=v]... | dag <yaml-path>")
    mode = sys.argv[1]
    if mode == "pipeline":
        name = sys.argv[2]
        params = dict(a.split("=", 1) for a in sys.argv[3:] if "=" in a)
        run_pipeline(name, **params)
    elif mode == "dag":
        import yaml, pathlib
        obj = yaml.safe_load(pathlib.Path(sys.argv[2]).read_text())
        print(f"Running DAG with {obj}")
        dag = {s["name"]: {"pipeline": s["pipeline"], "depends_on": s.get("depends_on", []), "params": s.get("params", {}), "retries": s.get("retries", 0)} for s in obj["steps"]}
        print(f"DAG: {dag}")
        run_dag(dag, max_workers=obj.get("max_workers", 4))
    else:
        raise SystemExit(f"Unknown mode: {mode}")
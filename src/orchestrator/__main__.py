"""Entry point: ``python -m orchestrator`` runs the demo DAG."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())

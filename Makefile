.PHONY: test demo lint

test:
	PYTHONPATH=src python -m pytest -q

demo:
	PYTHONPATH=src python examples/run_demo.py

serve:
	PYTHONPATH=src uvicorn orchestrator.service:create_app --factory --host 127.0.0.1 --port 8000

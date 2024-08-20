ruff:
	ruff check --fix .

clean:
	find . -name "__pycache__" -exec rm -rf {} +
	find . -name "*.pyc" -exec rm -f {} +

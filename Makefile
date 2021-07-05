format:
	.venv/bin/isort **/*.py
	.venv/bin/black **/*.py

test:
	.venv/bin/python -m unittest discover

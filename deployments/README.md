

pipreqs . --force --ignore .venv,build,dist,__pycache__,*.pyc,data,logs


pip-missing-reqs . --ignore-file=.pipreqs-ignore
pip-extra-reqs . --ignore-file=.pipreqs-ignore
# Dashboard

Independent UV project for the dashboard and exploratory Marimo notebooks.
M1.1 provides development tools and a notebook smoke check. Dashboard features
and warehouse access are not implemented yet.

Use CPython 3.14.7 and UV 0.12.17. Follow the shared
[Python toolchain guide](../docs/python-toolchain.md) for installation,
verification, and dependency updates.

Run from this directory:

```bash
uv sync --locked
uv run --locked pytest
uv run --locked flake8 .
uv run --locked marimo check notebooks/toolchain_smoke.py
uv run --locked marimo edit notebooks/toolchain_smoke.py
```

The notebook generates ten integers and displays their count and sum. It uses
this project's environment and lockfile, without separate notebook dependency
metadata. Use Marimo for future exploratory notebooks.

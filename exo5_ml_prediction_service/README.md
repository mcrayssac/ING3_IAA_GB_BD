# Fare Prediction

Independent UV project for training and inference scripts. M1.1 provides
development tools and runtime checks. Data access, training, and inference are
not implemented yet.

Use CPython 3.14.7 and UV 0.12.17. Follow the shared
[Python toolchain guide](../docs/python-toolchain.md) for installation,
verification, and dependency updates.

Run from this directory:

```bash
uv sync --locked
uv run --locked pytest
uv run --locked flake8 .
```

Training and inference must be Python scripts. Document functions with
NumPy-style docstrings. Use `uv run --locked pyment -o numpydoc <script.py>`
to generate a proposed documentation patch. Review and complete its descriptions
before applying it. Pyment does not check documentation quality or run code.

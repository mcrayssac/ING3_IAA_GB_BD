# Python Toolchain

The dashboard and prediction exercises are independent, non-packaged UV
projects. Each has its own `pyproject.toml`, `.python-version`, `uv.lock`, and
local `.venv`. Runtime dependencies are empty until feature development begins.

## Shared Versions

| Tool | Version | Used by |
|---|---|---|
| CPython | 3.14.7 | Both projects |
| UV | 0.12.17 | Both projects |
| pytest | 9.1.1 | Both projects |
| Flake8 | 7.3.0 | Both projects |
| Marimo | 0.24.2 | Dashboard development |
| Pyment | 0.3.3 | Prediction development |

Python is pinned to an exact patch in each `.python-version`. The project range
is `>=3.14.7,<3.15`. UV must match the exact version above and only discovers
UV-managed Python installations. Upgrades are explicit changes to the project.
Development tools are installed by the default `uv sync` command.

References: [Python releases](https://www.python.org/downloads/),
[UV release](https://github.com/astral-sh/uv/releases/tag/0.12.17),
[UV settings](https://docs.astral.sh/uv/reference/settings/),
[UV lockfiles](https://docs.astral.sh/uv/concepts/projects/layout/#the-lockfile),
[Marimo project environments](https://docs.marimo.io/guides/package_management/notebooks_in_projects/),
and [Pyment](https://github.com/dadadel/pyment).

## Install and Select the Tools

Install the pinned UV release using the
[official installer](https://docs.astral.sh/uv/getting-started/installation/).

Bash or Zsh:

```bash
curl -LsSf https://astral.sh/uv/0.12.17/install.sh | sh
```

PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/0.12.17/install.ps1 | iex"
```

Open a new terminal if the installer changes PATH, then run `uv --version`.
It must report 0.12.17. If another UV version is already installed and you want
to keep it, replace the initial `uv` in subsequent commands with
`uvx --from uv==0.12.17 uv`. This uses the pinned runner from the tool cache.

From the repository root, enter `exo4_dashboard` or
`exo5_ml_prediction_service`. The following commands work in Bash, Zsh, and
PowerShell:

```bash
uv python install 3.14.7
uv sync --locked
uv run --locked python --version
uv run --locked python -c "import platform, sys; print(platform.python_implementation(), sys.executable); print(sys.prefix != sys.base_prefix)"
```

Expect Python 3.14.7, CPython, an executable inside that project's `.venv`, and
`True` for virtual environment isolation. No environment activation or system
Python installation is needed. Leave any unrelated activated environment first.
Initial setup requires network access to download Python and dependencies.

## Acceptance Checks

Run inside **each** Python project:

```bash
uv sync --locked
uv run --locked pytest
uv run --locked flake8 .
```

Each suite verifies the pinned CPython version and virtual environment use.
Flake8 checks four-space indentation and a 79-character line limit. It excludes
environments, caches, Marimo session files, and generated build output.
Flake8 settings live in `.flake8`, which it reads natively.

Inside the dashboard project, also run:

```bash
uv run --locked marimo check notebooks/toolchain_smoke.py
uv run --locked python -c "from pathlib import Path; Path('build').mkdir(exist_ok=True)"
uv run --locked marimo export html notebooks/toolchain_smoke.py -o build/toolchain_smoke.html
uv run --locked marimo edit notebooks/toolchain_smoke.py
```

The notebook asserts and displays a count of **10** and a sum of **45**.
[HTML export executes the notebook](https://docs.marimo.io/guides/exporting/static_html/)
and must exit successfully. Confirm the same results in the editor, then stop
the server with Ctrl+C. Use this project environment without Marimo's sandbox
mode or inline dependency metadata. Generated HTML and session state are ignored.

Inside the prediction project, verify `uv run --locked pyment --version` reports
0.3.3. For a temporary Python file containing a function with parameters, run:

```bash
uv run --locked pyment -o numpydoc <temporary-file.py>
```

Pyment generates a patch without changing the input. Inspect it for a NumPy-style
`Parameters` section containing the function arguments. Complete meaningful
descriptions and types before applying documentation patches to project code.

To repeat from fresh environments, use a disposable repository copy without
either `.venv`, then repeat all checks in both project directories. Lockfiles
must remain unchanged. No datasets, credentials, or Docker services are needed.

## Dependency and Python Updates

Use `uv add --dev 'tool==version'` for an intentional development-tool update.
Use `uv add 'library==version'` when a feature first needs a runtime dependency.
Keep common development tools aligned across both projects. Commit manifests
and regenerated lockfiles together after acceptance checks pass.

For Python patch updates, change both `.python-version` files and the minimum
`requires-python` value together, install the chosen patch with UV, run `uv lock`,
and rebuild disposable environments for verification. Changing the UV baseline
also requires updating both `required-version` settings and these instructions.

Normal development uses `uv sync --locked` and `uv run --locked <command>`.
`--locked` fails if project metadata would require changing the lockfile. Resolve
intentional changes with `uv lock`, then review the diff. Avoid editing lockfiles
manually or maintaining a parallel `requirements.txt`.

## Verification Record

Verified on **19 September 2026** on **macOS ARM64**, using all versions in the
table above. The pinned UV runner was invoked through `uvx`.

- Both projects passed two pytest checks and Flake8.
- Marimo validation and HTML export passed. The editor rendered count 10, sum
  45, and the successful assertions without cell errors.
- Pyment generated a NumPy-style parameter section without changing its input.
- Both projects passed again in disposable copies with newly created `.venv`
  directories. Both lockfiles remained byte-for-byte unchanged.
- The installed UV 0.12.2 was correctly rejected by the project version guard.

These checks complete M1.1 alongside the recorded Scala verification. Windows
and Linux were not executed here. Fresh environments reused UV's download cache.

## Troubleshooting

- Wrong UV version: use the pinned installer or the `uvx` command above.
- Wrong Python: check `.python-version`, remove conflicting `UV_PYTHON` overrides,
  and recreate the project environment with the documented commands.
- Stale lockfile: review manifest changes and run `uv lock` only when intentional.
- Download failures: check network or proxy configuration. Do not disable TLS
  verification to work around certificate errors.
- Notebook failures: run the export command in a terminal and inspect cell errors.
  Imports for future notebook features must be declared in the dashboard project.

This setup verifies development tools. Warehouse and RustFS access, dashboard
features, training, inference, packaging, and CI belong to later work. Training
and inference remain Python scripts. No application API is introduced here.

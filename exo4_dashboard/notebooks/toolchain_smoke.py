"""Verify reactive notebook execution using only generated local data."""

import marimo

__generated_with = "0.24.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    # Generated inputs keep setup independent of datasets and infrastructure.
    numbers = list(range(10))
    count = len(numbers)
    total = sum(numbers)
    assert count == 10
    assert total == 45
    return count, total


@app.cell
def _(count, mo, total):
    mo.md(f"""
    # Python toolchain check

    Generated integers: **{count}**

    Sum: **{total}**

    Both assertions passed.
    """)
    return


if __name__ == "__main__":
    app.run()

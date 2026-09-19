# Project Roadmap

[![Detailed project Gantt showing 26 tasks across M1–M8, illustrative dependency steps 00–10, optional extensions, and three transversal activities.](diagrams/roadmap-gantt.svg)](diagrams/roadmap-gantt.svg)

[Editable Excalidraw source](diagrams/roadmap-gantt.excalidraw).

The chart shows the earliest dependency sequence using equal illustrative steps. Each task occupies one step, and the delivery diamond marks completion of M8.3. The task tables below are authoritative for prerequisites.

Overlapping bars identify work that dependencies allow to proceed in parallel. This is not a capacity-balanced schedule for two people, and equal bar lengths do not represent equal effort. Step numbers are neither dates nor duration estimates. Optional tasks remain outside the required delivery path.

T1 and T2 span foundations through completion of final acceptance in M8.1. T3 continues through delivery preparation. These bands describe ongoing activities, rather than additional prerequisites that must finish before every task.

Build a reproducible pipeline using yellow taxi records for **May, June, and July 2026**.

The tasks below provide a basis for allocation between both collaborators. Each task has a stable ID, an expected output, and explicit prerequisites. A prerequisite's accepted output must be available before the dependent task can be completed. Dates, effort estimates, and owners remain unset.

## Milestones

### M1. Project Foundations

| ID | Task / expected output | Depends on |
|---|---|---|
| M1.1 | Establish development tools and verify Scala, UV, and Marimo setup | None |
| M1.2 | Define architecture, component responsibilities, and interfaces | None |
| M1.3 | Prepare and verify RustFS, Spark, and PostgreSQL | M1.1, M1.2 |

M1.1 is complete. The [Scala toolchain](scala-toolchain.md) and
[Python toolchain](python-toolchain.md) provide pinned terminal workflows and
verified smoke checks, including Marimo execution. Python checks also pass in
fresh environments without changing either lockfile. These local checks do not
complete the architecture work in M1.2 or infrastructure checks in M1.3.

### M2. Data Retrieval — Exercise 1

| ID | Task / expected output | Depends on |
|---|---|---|
| M2.1 | Identify the three monthly files, dictionaries, and lookup data | None |
| M2.2 | Implement local retrieval of the selected Parquet files | M1.1, M2.1 |
| M2.3 | Upload local files to RustFS and verify availability | M2.2, M1.3 |
| M2.4 | Automate direct retrieval into RustFS with repeatable execution | M2.3 |

### M3. Data Validation and Cleaning — Exercise 2, Branch 1

| ID | Task / expected output | Depends on |
|---|---|---|
| M3.1 | Define the shared data contract and cleaning rules | M1.2, M2.1 |
| M3.2 | Implement shared validation and cleaning in Spark | M3.1, M2.3 |
| M3.3 | Publish cleaned historical Parquet files for prediction | M3.2 |

M3.1 and M3.2 establish validation shared by both ingestion branches. M3.3 writes the cleaned historical files to RustFS for branch 1.

### M4. Warehouse and SQL Analytics — Exercises 3 and 2, Branch 2

| ID | Task / expected output | Depends on |
|---|---|---|
| M4.1 | Define business questions and the dimensional model | M1.2, M2.1 |
| M4.2 | Create warehouse tables, constraints, and reference data | M4.1, M1.3 |
| M4.3 | Implement warehouse ingestion from the shared validated data | M4.2, M3.2 |
| M4.4 | Implement analytical SQL queries and expected results | M4.3 |

M4.2 provides `creation.sql` and `insertion.sql`. M4.3 applies warehouse transformations in memory within the shared Spark job, after the schema is available.

### M5. Dashboard — Exercise 4

| ID | Task / expected output | Depends on |
|---|---|---|
| M5.1 | Explore cleaned data in Marimo and select dashboard indicators | M3.3, M4.1 |
| M5.2 | Establish the dashboard's PostgreSQL connection and data access | M4.3 |
| M5.3 | Build charts and filters using the agreed indicators and queries | M5.1, M5.2, M4.4 |

### M6. Fare Prediction — Exercise 5

| ID | Task / expected output | Depends on |
|---|---|---|
| M6.1 | Define the prediction target, usable features, and evaluation method | M3.1 |
| M6.2 | Train a baseline, compare models, and record evaluation results | M6.1, M3.3 |
| M6.3 | Deliver inference scripts and saved model artifacts | M6.2 |

Training and inference use Python scripts. The prediction target is the price paid, with features available at inference time.

### M7. Optional Extensions

| ID | Task / expected output | Depends on |
|---|---|---|
| M7.1 | Optionally orchestrate retrieval and both ingestion branches | M2.4, M3.3, M4.3 |
| M7.2 | Optionally build an interactive prediction interface | M6.3 |
| M7.3 | Optionally document metadata and lineage with DataHub | M3.3, M4.3 |

M7.1 covers optional exercise 6 using Airflow or Argo Workflows. M7.2 can use Streamlit. Each extension is independent and remains secondary to the required exercises.

### M8. Final Delivery

| ID | Task / expected output | Depends on |
|---|---|---|
| M8.1 | Reproduce and accept the complete workflow across all three months | M2.4, M3.3, M4.4, M5.3, M6.3 |
| M8.2 | Consolidate the final report, screenshots, results, and limitations | M8.1 |
| M8.3 | Prepare the GitHub, source ZIP, and Teams submission package | M8.2 |

## Transversal Activities

These activities run alongside the milestones rather than forming a final development phase.

### T1. Integration

**Related milestones: M1–M7, consolidated in M8.1.**

Agree on interfaces during foundations and verify connections as their inputs and outputs become available. Check retrieval into shared validation, both ingestion branches, warehouse access from the dashboard, and cleaned Parquet access from prediction. Consolidate these checks during the complete workflow demonstration in M8.1.

### T2. Testing & Quality

**Related milestones: M1–M7, final acceptance in M8.1.**

Establish testing tools during foundations and validate each deliverable within its task. Check ingestion rules, processing behavior, SQL results, dashboard consistency, and training/inference inputs. Evaluate prediction against the assignment target of RMSE below 10, or justify an alternative metric in the report. Complete final acceptance in M8.1.

### T3. Documentation & Reproducibility

**Related milestones: M1–M8.**

Maintain setup instructions, execution commands, code documentation, diagrams, and technical decisions throughout development. Collect evidence and screenshots as each task is completed. Consolidate these materials into the report and submission package in M8.2–M8.3.

## Sequencing and Completion

- Milestone numbers group work. They do not impose complete sequential execution.
- Cleaning can begin after the initial RustFS upload in M2.3, while direct retrieval in M2.4 is developed.
- Warehouse modeling in M4.1 can begin before cleaning implementation finishes.
- Both ingestion branches use shared validation from M3.2. Warehouse loading requires the schema from M4.2, but does not wait for the Parquet export in M3.3.
- Dashboard and prediction tasks can proceed independently once their respective inputs are available.
- Optional extensions do not block required delivery. Any extension included in the submission must also be verified and documented before delivery.

A task is complete when its expected output exists, relevant checks pass, affected connections work, and necessary documentation is updated. Dependent tasks rely on that accepted output. A milestone is complete when its tasks and relevant transversal activities are satisfied.

For later work allocation, consider parallel opportunities such as development setup and architecture, retrieval automation and cleaning, warehouse modeling and validation, or dashboard and prediction development. Agree on shared interfaces and handoff outputs before dividing the work. These opportunities do not assign either collaborator or assume their availability.

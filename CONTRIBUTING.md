# SparkStore — Contributing Guidelines

Thank you for contributing to **SparkStore**! This guide outlines the workflow, version control practices, branch naming rules, and code standards for the V4C Data Engineering training exercise.

---

## Version Control & Branching Strategy

To maintain a clean and trackable Git history, all contributors must adhere to strict feature branching, multi-environment branch structures, and commit message conventions.

### 1. Primary Branch Structure (`main` and `prod`)

The repository uses a dual-primary branch model for environment promotion:
- **`main`:** The integration and staging branch where approved feature branches are merged and tested before production release.
- **`prod`:** The production-ready branch reflecting the live, deployed state of the Databricks lakehouse. Direct pushes to `prod` are strictly prohibited; releases must be promoted via a pull request from `main` with appropriate administrative approval.

### 2. Feature Branch Naming (`F-xx-short-description`)

All new features, bug fixes, or pipeline updates must be developed on a dedicated branch branched off `main`.

- **Format:** `F-[TaskID]-[short-description-in-kebab-case]`
- **Examples:**
  - `F-01-autoloader-bronze-ingestion`
  - `F-05-unity-catalog-rbac-grants`
  - `F-12-lakeflow-job-orchestration`

### 3. Commit Message Conventions

Commit messages must follow the conventional commits standard. Version numbers (e.g., `v1.0.0`) should only be used in official release tags or dedicated release/version-bump commits when merging into `prod`, not in routine feature or bugfix commits:

- `feat(ingestion): add Auto Loader schema evolution handling for CSV files`
- `fix(streaming): correct timestamp parsing in Kafka streaming consumer`
- `docs: update architecture diagram and data flow description`
- `test: add data quality expectation rules for transaction amounts`
- `release: bump version to v1.0.0 for initial production readiness`

### 4. Pull Request (PR) & Promotion Workflow

- Every feature branch must first be merged into `main` via a Pull Request requiring at least **one peer review** and approval.
- Promotion from `main` to `prod` requires passing all integration checks, automated Lakeflow pipeline tests, and formal sign-off.
- Ensure all Lakeflow pipelines, tests, and data quality checks pass successfully prior to requesting review.

---

## Code & Development Standards

### 1. Notebooks & Python Code

- Keep notebooks modular; use reusable Python functions or Unity Catalog volume scripts where applicable to avoid code duplication.
- Hard-coded credentials, passwords, tokens, or storage keys are **strictly prohibited**. Always use Azure Key Vault secrets or Databricks secret scopes.

### 2. Unity Catalog & Governance

- All production tables must be registered under the appropriate Unity Catalog catalog and schema structure (`bronze`, `silver`, `gold`).
- Ensure explicit ownership, table comments, and column descriptions are provided for all core datasets.

### 3. Testing & Validation

- Include validation checks and data quality expectations (e.g., `Quantity > 0`, null checks on primary keys) for all Silver and Gold transformations.
- Document performance before and after applying optimizations (such as clustering, liquid clustering, or Z-ordering).

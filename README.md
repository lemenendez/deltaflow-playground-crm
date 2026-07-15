# CRM Scenario Playground

This repository is a scenario-oriented playground for Deltaflow.

It supports two usage modes:

- Shared mode: same scenario behavior across flavors by keeping common logic in `internal/scenario`.
- Self-contained mode: a user can run a single flavor folder independently, with scenario + wiring inside that flavor.

## Repository Layout

- `internal/scenario`: shared scenario code used by all flavors
- `flavors/postgres-es`: Postgres + Elasticsearch flavor
- `flavors/postgres-redis`: Postgres + Redis flavor
- `flavors/sqlite-es`: SQLite + Elasticsearch flavor
- `flavors/sqlite-redis`: SQLite + Redis flavor

Module path:

- `github.com/lemenendez/deltaflow-playground-crm`

Deltaflow version currently used in this repository:

- `github.com/lemenendez/deltaflow v0.11.2`

## Flavor Status

- [x] `flavors/postgres-es` (implemented)
- [ ] `flavors/postgres-redis` (pending)
- [ ] `flavors/sqlite-es` (pending)
- [ ] `flavors/sqlite-redis` (pending)

## Adaptation Rules (When Pasting Existing Code)

Use these rules to migrate old code into this layout.

0. Choose the target mode first
- If the code is expected to be reused by multiple flavors, put it in `internal/scenario`.
- If the code is specific to one flavor or users will run only one flavor folder, keep scenario + wiring self-contained in that flavor folder.

1. Move domain/scenario behavior to `internal/scenario`
- Keep use cases, fixtures, assertions, scenario setup helpers, and deterministic test flow here.
- Shared code must not depend on flavor-specific runtime details.

1b. Keep each flavor runnable on its own
- Each `flavors/<name>` folder should have everything needed to run its target scenario without requiring users to navigate other flavor folders.
- A flavor may duplicate small scenario setup pieces when that improves local usability and clarity.

2. Keep infrastructure wiring inside each flavor folder
- DB setup, connection strings, container/local infra, and projector/applier runtime wiring stay in `flavors/*`.
- Each flavor should only adapt the shared scenario contract and runtime dependencies.

3. Introduce clear boundaries with small interfaces
- Define interfaces in `internal/scenario` for things that vary by flavor.
- Implement those interfaces per flavor.

4. Keep imports stable and explicit
- Use module-relative imports, for example:
	- `github.com/lemenendez/deltaflow-playground-crm/internal/scenario`

5. Normalize naming
- Keep package names and file names aligned with scenario intent.
- Prefer `scenario`-oriented names over flavor names in shared code.

## Self-Contained Flavor Guideline

If a user is focused on one scenario + one flavor, prefer this shape inside that flavor:

- Scenario entrypoint test(s)
- Flavor runtime wiring
- Local fixtures specific to that flavor
- Minimal dependency on `internal/scenario` (or none, if complete isolation is preferred)

This keeps onboarding simple for single-flavor users while still allowing shared mode for cross-flavor consistency.

## Suggested Shared Contract Shape

Put shared contracts in `internal/scenario`, for example:

- `type Runtime interface { ... }`
- `type FixtureBuilder interface { ... }`
- `func RunScenario(t *testing.T, rt Runtime)`

Then in each flavor folder:

- Build a runtime implementation for that flavor.
- Call the shared runner with the concrete runtime.

## Migration Checklist

When pasting code from the previous project:

- Decide if this chunk is shared or flavor-local.
- Remove hardcoded old import paths.
- Move reusable logic to `internal/scenario` first.
- Keep only adapters/wiring in `flavors/<name>`.
- Replace duplicated test flow with shared scenario runner calls.
- Ensure each flavor has an entrypoint test that executes the same scenario.

When optimizing for self-contained flavors:

- Keep scenario flow directly in the target `flavors/<name>` folder.
- Avoid cross-flavor references.
- Duplicate small helpers when needed to keep the folder independently runnable.

## Next Step

Paste the first code chunk and target flavor. The adaptation can then be done incrementally by splitting shared scenario logic from flavor-specific wiring.

## GitHub Security Checklist

Use this checklist to harden the repository configuration in GitHub after cloning or creating a new environment.

- Enable Dependabot alerts
- Enable Dependabot security updates
- Enable Secret scanning
- Enable Push protection for secrets
- Enable Code scanning alerts (CodeQL)
- Protect `main` branch
- Require pull requests before merge
- Require at least 1 approving review
- Require status checks to pass before merge
- Restrict direct pushes to `main`
- Include administrators in branch protection rules (recommended)
- Restrict who can dismiss pull request reviews (recommended)






# TicketRush - Agent Guide

Flash-sale ticketing platform built as a portfolio project to show distributed-systems skills
(Kafka, Kubernetes, failure handling). The owner is a junior developer: explain key design
decisions briefly as you go.

**Central invariant:** never sell more tickets than exist, and never create or charge the same
order twice. Every change must keep this true.

## Read before working

1. [docs/SPEC.md](docs/SPEC.md) - the full brief, milestones and acceptance criteria.
2. [docs/DESIGN.md](docs/DESIGN.md) - approved versions, layout, event schemas, topics, partitions.
3. [docs/PROGRESS.md](docs/PROGRESS.md) - which milestone is current and what is left.

## Milestone workflow (mandatory)

- Work on exactly one milestone at a time, on branch `milestone-<n>` cut from `main`.
- Never start the next milestone. When the current one is done:
  1. Run its acceptance command and paste the real output.
  2. Update docs/PROGRESS.md: what exists, the verification result, what is NOT done or not verified.
  3. Stop and wait for the owner's go-ahead.
- Acceptance is proven by running a command, never by reading code.
- For M2, run the contention and replay tests 10 times and report every run.

## Ask before building

Stop and ask the owner when:
- A requirement is ambiguous.
- A choice is hard to reverse: topic names, partition counts, event schemas, keys, DB schema shape.
- You want to deviate from docs/SPEC.md or docs/DESIGN.md.
- You want to add a dependency (state why; record it in DESIGN.md's dependency table).

Record every approved decision in the decision log in docs/DESIGN.md.

## Engineering rules

- **TDD for money, inventory and idempotency:** write the failing test first, see it fail, then implement.
- Unit tests for domain logic; Testcontainers integration tests for Kafka and DB paths. JUnit 5 + AssertJ.
  Use Awaitility for async assertions, never `Thread.sleep`.
- Keep it simple: no speculative abstractions, no single-implementation interfaces, no extra
  services beyond the architecture in SPEC.md.
- Money is `long` cents + currency string. Never `double` or `float`.
- Consumers are idempotent (dedupe by `messageId` / `orderId`). Commit offsets only after successful processing.
- Handle SIGTERM gracefully (Spring graceful shutdown, stop consumers before closing producers).
- Config via environment variables. No secrets in the repo. Every variable goes in `.env.example`.
- Dockerfiles: multi-stage, non-root user.
- Use the versions in DESIGN.md section 1. Check for newer stable versions only when a milestone
  introduces a new tool, then record the choice.

## Git

- Conventional commits: `feat:`, `fix:`, `test:`, `refactor:`, `docs:`, `chore:`, `ci:`, `perf:`.
- Small commits, one logical change each.
- One branch per milestone. Do not push or merge to `main` without the owner's approval.

## Writing style

- Never use the en dash or em dash characters in any output (code, comments, docs, commits).
  Use a plain hyphen "-".
- Keep explanations short and concrete; say what is not verified.

## Commands

Filled in as milestones land. Keep this list accurate.

| Purpose | Command |
|---|---|
| Build + all tests | `mvn verify` |
| M1 acceptance | `mvn verify -pl e2e-tests -am` |
| Local stack | `docker compose -f deploy/compose/docker-compose.yml up -d` |

## Environment notes

- Docker is required (Testcontainers and Compose).
- `kind` and `helm` are not installed yet; they are needed from Milestone 3.

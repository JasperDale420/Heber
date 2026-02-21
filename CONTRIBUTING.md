# Contributing

## Getting Started

1. Create or switch to a feature branch.
2. Install dependencies with `uv`.
3. Copy `.env.example` to `.env`.
4. Run tests before and after changes.

## Branch Strategy

- Use short-lived feature branches.
- Keep commits focused and small.
- Use clear commit messages tied to one slice of work.

## Pull Request Process

1. Ensure tests/lint/type checks are green.
2. Update `CHANGELOG.md`.
3. Include a short RCA + validation summary in the PR body.
4. If Docker/runtime behavior changed, include rebuild and log-validation notes.

## Code Style

- Python 3.11+
- Ruff for linting/formatting
- Type hints for new code
- Structured logging with actionable context

## Testing Requirements

- Follow TDD for behavior changes and bug fixes.
- Add regression tests for every production bug fixed.
- Keep tests deterministic (no real network in unit tests).

## Commit Messages

Recommended format:

- `watch: ...`
- `writer: ...`
- `ops: ...`
- `docs: ...`
- `chore: ...`

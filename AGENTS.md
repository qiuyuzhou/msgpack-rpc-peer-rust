# Agent Instructions

## Agent skills

### Issue tracker

Issue tracker configuration is contributor-local.

Before using an issue-tracking skill:

1. Resolve the Git common directory:

   `git rev-parse --path-format=absolute --git-common-dir`

2. Read `<git-common-dir>/agent/issue-tracker.md`.

3. Follow the tracker configuration found there.

If the file does not exist, ask the contributor to
configure their preferred tracker.

Never assume GitHub Issues is the default.
Never commit personal tracker configuration.
Never reference personal tracker issues or tasks in code, commit messages, pull requests, or GitHub comments.

### Triage labels

Triage state labels use the canonical defaults; follow any contributor-local mapping. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout. See `docs/agents/domain.md`.

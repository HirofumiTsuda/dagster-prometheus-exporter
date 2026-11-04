---
name: create-issue
description: Create a GitHub issue in this repo with the right labels, and check that the labels were really applied. Use whenever the user asks to open, file or "cut" an issue. Writes the title and body in English, even if the conversation is in Japanese.
---

# Create issue

Create an issue in this repo, label it, and read the labels back.

## Steps

1. **Check it is not a duplicate.** `gh issue list --state all --search "<keywords>"`.
   If one exists, comment on it or reopen it instead.

2. **Write the title and body in English**, whatever language the conversation
   is in. Use the shape of the existing issues:
   - A problem or feature: `## Problem`, `## Proposal`, then `## Not covered`
     and `## Done when` when they help. State what was measured and what was
     not. Link the umbrella issue or the pull request it follows up.
   - A bug: the sections of `.github/ISSUE_TEMPLATE/bug_report.md`.
   - Text posted under the maintainer's name: short plain sentences, no em
     dashes.

3. **Pick the labels** from this repo's set. One for the kind, then the areas
   it touches:

   | Label | Use for |
   |---|---|
   | `bug` | Something does not work |
   | `enhancement` | A new metric, option or capability |
   | `documentation` | Docs only |
   | `refactor` | Restructuring with no behavior change |
   | `testing` | Unit tests, e2e, test infrastructure |
   | `performance` | Slowness or load on Dagster or the exporter |
   | `security` | Token handling, hardening, supply chain |
   | `investigation` | Needs measurement or research before a fix is chosen |
   | `area/collector` | Metrics, collectors, GraphQL queries |
   | `area/server` | HTTP server, `/healthz`, `/readyz`, the scrape loop |
   | `area/helm` | The Helm chart |
   | `area/dev-stack` | docker-compose, kind, the local dev environment |
   | `dagster-plus` | Specific to Dagster+ |
   | `github_actions`, `docker`, `go`, `python:uv` | The change lives in that part of the repo |
   | `good first issue`, `help wanted` | Taken by us now (the maintainer fixes them), so add only if a contributor is wanted |

4. **Create it with `gh api`, and send the labels as JSON.** Do not use
   `-f 'labels[]=...'`: with the installed `gh` the issue is created with no
   labels, and nothing says so. Six issues were filed that way and none had a
   label.

   ```bash
   gh api -X POST repos/HirofumiTsuda/dagster-prometheus-exporter/issues \
     --input - <<'EOF'
   {"title": "...", "labels": ["enhancement", "area/collector"], "body": "..."}
   EOF
   ```

   Build the JSON with `python3 -c 'import json; ...'` when the body has quotes
   or backticks, instead of escaping by hand.

5. **Read the labels back before saying they are set.** The create response
   has them:

   ```bash
   gh issue view <number> --json labels -q '[.labels[].name]|join(",")'
   ```

   If the list is empty or short, add them with
   `gh api -X POST repos/.../issues/<number>/labels --input -` and
   `{"labels": [...]}`, then read again. Report the labels that are actually
   on the issue, not the ones that were asked for.

6. **Give the user the issue URL** and the labels that are on it.

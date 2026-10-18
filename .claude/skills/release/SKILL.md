---
name: release
description: Cut an exporter release (vX.Y.Z) and the matching Helm chart release (chart-vX.Y.Z) in the right order, with release notes and the chart metadata that is easy to forget. Use whenever the user asks to prepare, tag, publish or check a release, or to bump the chart.
---

# Release the exporter and the chart

The exporter image and the Helm chart are released separately, by two tags:

| Tag | Fires | Publishes |
|---|---|---|
| `vX.Y.Z` | `docker-publish.yml` | `ghcr.io/hirofumitsuda/dagster-prometheus-exporter:X.Y.Z`, `:X.Y`, `:latest` |
| `chart-vX.Y.Z` | `helm-publish.yml` | `oci://ghcr.io/hirofumitsuda/charts/dagster-prometheus-exporter` |

The chart's default image tag is its `appVersion`, so **the image must exist
before the chart that points at it**. A chart bump PR opened first fails its
e2e, which pulls a tag that is not there yet.

Tagging and publishing are hard to undo. Draft everything first, show it, and
get an explicit yes before creating either release.

## 1. Decide the versions

- Exporter: while on 0.x, a new option or metric, or a change that can break
  an existing dashboard, alert or configuration, is a minor bump. Fixes only
  is a patch.
- Chart: patch bumps (0.1.x) have been the convention, even for new values.
  Say so if a change makes you want to depart from it.
- A chart-only release (templates, values, alerts, docs) skips the exporter
  tag and keeps `appVersion`.

## 2. Collect what changed

`git log <last v tag>..HEAD`, then read the PR bodies, not only the titles.
Sort each change into: behavior that can break existing setups, added, fixed,
docs. Dependabot bumps need no line unless they matter to users.

Check `main` CI is green on the commit you will tag.

## 3. Draft the release notes

Write English notes for both releases into the scratchpad first. Follow the
shape of the previous notes (`gh release view <tag>`).

- Open with one sentence, then a `## ⚠️` section for anything that changes
  existing behavior: a new label value, a series that disappears, a setting
  that is now rejected at startup, a changed chart default. Say who is
  affected and what to do.
- Then `✨ Added`, `🐛 Fixes`, `📝 Docs`.
- **`## 🙏 Thanks`**: name every external contributor with the PR or issue
  they did, including someone whose PR was reworked or replaced, and someone
  whose investigation led to a fix. Check with
  `git log <tag>..HEAD --format='%an'` and the closed PRs, since the git author
  name is often not the GitHub handle.
- The chart notes link to the exporter release and end with a
  `**Full Changelog**` compare link between the two chart tags.
- No em dashes in text posted under the maintainer's name.

## 4. Publish the exporter

1. `gh release create vX.Y.Z --target main --title vX.Y.Z --notes-file <file>`.
2. Wait for the Docker publish run for that tag to finish.
3. Confirm the image exists and runs: pull `:X.Y.Z`, check it starts as uid
   65532.

## 5. Bump and publish the chart

Branch `chore/bump-chart-X.Y.Z`, one PR, then merge it when CI is green.

- `Chart.yaml`: `version`, `appVersion` (no `v` prefix, it must match a real
  image tag), and `artifacthub.io/changes`.
- `artifacthub.io/changes` is **replaced**, not appended: it lists only this
  release's changes. Each entry has a `kind` (`added`, `changed`, `fixed`,
  `security`, ...) and a `description`. Write "0.6.1 to 0.7.0", not
  `0.6.1 -> 0.7.0`: Artifact Hub drops the `>`.
- `--version X.Y.Z` in **both** the root `README.md` and
  `charts/dagster-prometheus-exporter/README.md`. A grep for the old version
  finds anything else.
- `helm lint charts/dagster-prometheus-exporter`.

After the merge, `gh release create chart-vX.Y.Z --target main` with the chart
notes, and wait for Helm Publish.

## 6. Verify and follow up

- `helm show chart oci://ghcr.io/hirofumitsuda/charts/dagster-prometheus-exporter --version X.Y.Z`
  shows the new version and appVersion.
- Artifact Hub shows the version and its changes (it lags a few minutes):
  `curl https://artifacthub.io/api/v1/packages/helm/dagster-prometheus-exporter/dagster-prometheus-exporter`.
- Reply on issues that were waiting for the release, and tell contributors
  their change shipped.
- Record the release in the maintainer's working notes.

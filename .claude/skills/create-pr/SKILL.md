---
name: create-pr
description: Create (or update) a GitHub pull request for the current branch in this repo. Use whenever the user asks to open, create, or update a PR. Fills in this repo's PR template and writes the title/body in English, even if the conversation is in Japanese.
---

# Create PR

Create a pull request for the current branch against `main`, following this
repo's PR template.

## Steps

1. **Name the branch `<type>/<short-slug>`** when creating a new branch for
   the work (before starting, not at PR time). `<type>` mirrors Conventional
   Commit types — `feat`, `fix`, `docs`, `refactor`, `chore`, `test`, `ci`,
   `perf`, `build` — picked to match the nature of the change. `<slug>` is a
   short kebab-case description (e.g. `docs/contributing-and-issue-templates`,
   `fix/dashboard-location-label`). Do **not** put issue numbers in the
   branch name — link issues from the PR body's `Ref` section instead (see
   step 4), since a branch name shouldn't need updating if the issue's scope
   shifts.

2. **Check state.** Run `git status`, `git log main..HEAD`, and
   `git diff main...HEAD` to see what's actually being shipped. If there are
   uncommitted changes relevant to the PR, ask the user before including
   them — don't silently commit unrelated work.

3. **Push the branch** if it isn't already tracked upstream:
   `git push -u origin <branch>`.

4. **Read the template** at `.github/PULL_REQUEST_TEMPLATE.md` (sections:
   `What` / `Why` / `QA` / `Ref`) and fill in each section based on the
   actual diff and commits — don't invent content that isn't supported by
   the change. Reference any related issue in the `Ref` section (e.g.
   `Closes #26`), rather than encoding it in the branch name.

5. **Language: always write the PR title and body in English**, regardless
   of what language the conversation with the user is in. This applies to
   both new PRs and edits to existing ones.

6. **Create or update the PR using `gh api`, not `gh pr create --body` /
   `gh pr edit --body`.** In this repo/account, `gh pr create` and
   `gh pr edit` intermittently fail with:

   ```
   GraphQL: Projects (classic) is being deprecated in favor of the new
   Projects experience ... (repository.pullRequest.projectCards)
   ```

   This comes from the `gh` CLI's own follow-up GraphQL query for extra
   fields (deprecated project cards) after the mutation, not from the
   mutation itself — the PR create/update can partially succeed while `gh`
   still reports an error, which is confusing. Avoid it entirely by calling
   the REST API directly:

   - New PR:
     ```bash
     gh api -X POST repos/{owner}/{repo}/pulls \
       -f title="..." \
       -f head="<branch>" \
       -f base="main" \
       -f body="$(cat <<'EOF'
     ...body text...
     EOF
     )"
     ```
   - Update an existing PR's body/title:
     ```bash
     gh api -X PATCH repos/{owner}/{repo}/pulls/{number} \
       -f body="$(cat <<'EOF'
     ...body text...
     EOF
     )"
     ```

   Get `{owner}/{repo}` from `git remote get-url origin` if not already
   known.

7. **Verify** by reading back the result (`gh pr view <number> --json
   url,title,body -q ...` or the API response) and report the PR URL to the
   user.

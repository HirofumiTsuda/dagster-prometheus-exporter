---
name: how-what-why
description: Decide where each piece of information goes when writing or reviewing code in this repo — How in the code, What in the tests, Why in the commit message, Why-not in code comments. Use whenever writing or changing Go code, tests, code comments or commit messages, and when reviewing a diff for comments that belong somewhere else.
---

# How / What / Why / Why not

Each kind of information has one home. Putting it anywhere else makes it go
stale, repeat itself, or get lost.

| Where | What it holds | Question it answers |
| --- | --- | --- |
| Code | How it works | "How does this do it?" |
| Tests | What it does | "What is it supposed to do?" |
| Commit message | Why it changed | "Why was this change made?" |
| Code comment | Why not | "Why isn't this done the obvious way?" |

## Code: How

The code itself is the only place that always matches what runs. Make it
readable through names and structure instead of explaining it in comments.

- If a comment is needed to follow *what a block does*, rename or extract
  instead.
- Do not narrate: `// loop over collectors` above a loop adds nothing.

## Tests: What

A test is the executable statement of behavior. Someone who wants to know what
the code promises should be able to read the test names and understand it.

- Name tests as the behavior: `TestLoadRejectsCacheTTLShorterThanTwoIntervals`,
  not `TestLoad2`.
- Assert the observable result, not the implementation path.
- Give an assertion a message when the failure would otherwise be cryptic.
- A new behavior and a bug fix both need a test that fails without the change.

## Commit message: Why

The reason for a change does not live in the code, and it is the thing that is
hardest to recover later. Put it in the commit message (and the PR body, which
the squash commit reuses).

- Subject: what changed, in one line, imperative.
- Body, first: the problem or motivation, with the evidence if there is some
  (a measurement, an issue, a failure). Then a short summary of the approach.
  Do not walk through the diff; the diff already shows that.
- Written in English, even if the conversation is not.
- Reference issues in the body or PR (`Ref #140`), not in comments.

## Code comments: Why not

A comment is for what a reader cannot see in the code and would get wrong:
the road not taken. Write one when a reasonable person would try the obvious
approach, and it would be a mistake.

Examples of the kind to write (the first is already in `graphql.go`):

- `graphQLClient` does not follow redirects, because the client would forward
  `Dagster-Cloud-Api-Token` to wherever the redirect points.
- `ticks(limit: 1)` needs a time bound, or Dagster's batch loader can return
  an empty list; a mock that does not model it hides the failure.
- Do not buffer the whole response with `io.ReadAll` just to count its bytes;
  a counting reader keeps the memory use unchanged.

A comment should:

- name the tempting alternative and say why it fails or what it costs;
- state a constraint or a trap that is invisible from the code;
- stay true as long as the code stays the same.

A comment should not:

- restate what the code does (that is the code's job, and tests cover What);
- explain why the change was made, or mention an issue number or "added for
  #142" (that is history, and belongs in the commit message);
- describe how it used to work.

## Before committing

Read your own diff with these checks:

1. For each new comment: is it a Why-not? If it explains the motivation for
   the change, move that to the commit message. If it restates the code,
   delete it or improve the names.
2. Is there behavior with no test that states it? Add one.
3. Does the commit message say why, before how?
4. Is there a "why not" the code is missing, a place where the next person
   will "simplify" it into a bug? Add a comment there.

## Existing code

Much existing code has long rationale comments written before this rule. Apply
it to the lines you write or change. Do not rewrite comments in code you are
not otherwise touching, and do not mass-edit them in a feature PR.

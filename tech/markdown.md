# Markdown (the documentation files)

## What it is

**Markdown** is a way of writing formatted text in a plain text file, using a few simple marks. A line that starts with `# ` is a big heading, `## ` a smaller one; `**word**` is bold; `` `code` `` is code in the middle of a sentence; a line starting with `- ` is a bullet in a list; a block between two lines of three backticks (```` ``` ````) is a block of code, and the word after the opening backticks (`bash`, `json`, `text`) names its language. A program (GitHub, your editor, a documentation site) turns the marks into formatted pages. Files end in `.md`.

## Why this repository uses it

The template requires documents a learner reads before and after running anything: what the project is, how it is designed, and why. Markdown files sit next to the code in the repository, so they are versioned with it and show up as formatted pages on GitHub.

## Where it appears

| File | What it is for |
| --- | --- |
| `README.md` | The front page: what the project is, prerequisites, the commands, the lesson's "done when" checklist with proof, what is simplified, and project structure. |
| `ARCHITECTURE.md` | How the code is shaped and why: components, the pattern (ports and adapters), the data flow, decisions and trade-offs, failure modes, limits, how to extend. |
| `docs/adr/0001-...md` to `0003-...md` | One **architecture decision record** each: a short note on one decision. |
| `docs/RUNBOOK.md` | The runbook for "the balances look wrong": a table with one row per failure, with the check to run, the signal you will see, the cause and the fix. |
| `CHANGELOG.md` | The list of changes by version (see `tech/license-and-versioning.md`). |
| `tech/README.md` and the other files in `tech/` | These notes. |

## How it works underneath

Markdown is only text. Nothing "runs". A renderer reads it line by line: a line that starts with a heading mark becomes a heading; a table is lines with `|` between the cells and a second line of dashes; a link is written `[text](address)`. Because it is just text, Git can show exactly which words changed between two versions.

## Walkthrough of this repository's use

### Headings and the page outline

Every Markdown file starts with one `#` heading (the title) and uses `##` for sections and `###` for sub-sections. The outline of the repository's documents:

- `README.md`: Prerequisites; Setup, run, test, stop; The SQL; Done when; Stretch goals; What is simplified for teaching; Project structure; Author and license.
- `ARCHITECTURE.md`: Components and what each owns; Pattern: ports and adapters; Data flow (with a diagram); Decisions and trade-offs; Failure modes the lesson teaches; Limits of the teaching build against production; How to extend it.

### Tables

A table has a header row, a line of dashes that marks it as a table, and one line per row, with `|` between cells. The README's prerequisites and "Done when" sections are tables. A `|` inside a cell would break it, which is why commands that contain a pipe are put in code blocks and not in cells.

### Code and file links

`inline code` marks names of files, commands and values. Fenced code blocks hold longer text exactly as it should be typed, and the language word after the opening backticks tells the renderer how to colour it. A link to another file in the same repository is written with a **relative path**, such as `[ARCHITECTURE.md](ARCHITECTURE.md)` or `[ADR 0001](docs/adr/0001-key-events-by-order-id-with-six-partitions.md)`; GitHub opens the file at that path.

### Architecture decision records (ADRs)

The three files in `docs/adr/` each follow the same five headings:

```markdown
# 1. Record the event id in the same database transaction as the balance update

Status: accepted, 2026-10-26

## Context

Kafka delivers at least once. A consumer that writes to PostgreSQL and then commits its offset has a gap: a crash between the two redelivers a payment that is already in the balance. The consumer must leave the balance right whatever the timing of a crash, a rebalance or an eviction.

## Decision

Keep a table `processed_events` with `event_id` as primary key. For each payment, in one database transaction, insert the event id (`ON CONFLICT DO NOTHING`) and, only if a row was inserted, add the amount to the balance. Commit the Kafka offset after the database commit. A redelivered event inserts nothing and is skipped.
```

- **Title**, numbered, one decision. **Status** with a date.
- **Context**: the forces that made a decision necessary.
- **Decision**: what was chosen.
- **Alternatives considered**: what else was possible, and why it was not chosen. It is the most useful section for a learner, because it shows the design was a choice.
- **Consequences**: what follows from the choice, good and bad.

The three records here are: record the event id in the same database transaction as the balance update (0001), ports and adapters around Kafka and PostgreSQL (0002), and psycopg 3 with `kafka-python` 3.0.12 as the clients (0003). Numbers are never reused, and a decision that is replaced gets a new record marking the old one superseded.

### Diagrams

`ARCHITECTURE.md` draws its data flow with **Mermaid**, a small text language for diagrams. A fenced block whose language word is `mermaid` is drawn as a picture by GitHub and by many editors; in a plain viewer it shows as text, which is still readable. The block:

```markdown
flowchart LR
    producer["Producer<br/>10,000 payments"]
    subgraph kc["Kafka cluster"]
        topic[("payments<br/>6 partitions, key accountId")]
        dlt[("Dead-letter topic<br/>payments-dlq")]
        gc{{"Group coordinator<br/>(a broker)"}}
    end
    updater["Balance updater<br/>group balance-updater"]
    subgraph pg["PostgreSQL"]
        balances[("balances<br/>running totals")]
        seen[("Processed events<br/>processed_events")]
    end
    producer -- "payments" --> topic
    topic -- "poll" --> updater
    updater -- "commit offset (after the DB commit)" --> topic
    updater -- "1. insert eventId" --> seen
    updater -- "2. add amount (same transaction)" --> balances
    updater -- "bad record + error" --> dlt
    updater -. "heartbeat" .-> gc
```

- `flowchart LR` says: a flow chart that reads left to right.
- `subgraph kc["..."] ... end` draws a box with a title around the lines inside it.
- `topic[("payments<br/>6 partitions, key accountId")]` defines a node (a shape) with an id (`topic`) and a label; `[( )]` draws a cylinder, the usual shape for stored data, `{{ }}` a hexagon, `[ ]` a plain box. `<br/>` is a line break inside the label.
- `producer -- "payments" --> topic` draws an arrow from one node to another with text on it. A dotted arrow (`-. "heartbeat" .->`) is used for control traffic, as against the data arrows; the numbered labels `1. insert eventId` and `2. add amount (same transaction)` show the order of the two database writes.

## Try it yourself

```bash
grep -n '^#' ARCHITECTURE.md
```

Expected: the line numbers and titles of every heading, which is the outline above. Most editors show a rendered preview of a `.md` file if you open it; GitHub does it automatically.

## Mistakes people make

- Using spaces in a link address; use `-` in file names so links need no escaping.
- Forgetting the blank line before a list or a table, which makes the renderer treat it as part of the paragraph above.
- Documents that describe code that no longer exists. This is why the notes are written after the code is final and updated with it.

## Check yourself

1. What does a line starting with `## ` become? (A second-level heading.)
2. Which ADR heading explains what else was possible? (Alternatives considered.)
3. How do you link to another file in the same repository? (A relative path inside `[text](path)`.)

## Further reading

- CommonMark, the Markdown specification: https://commonmark.org/
- GitHub Flavored Markdown (tables and fenced code): https://github.github.com/gfm/
- Architecture decision records, Michael Nygard's original post: https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions

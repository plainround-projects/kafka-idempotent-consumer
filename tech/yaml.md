# YAML

## What it is

YAML is a plain-text format for settings. It describes the same things as JSON (text, numbers, lists, name-value maps) but uses indentation instead of braces, so it is easier to read. `#` starts a comment.

## Why this repository uses it

Two tools read their instructions as YAML: Docker Compose (`docker-compose.yml`) and GitHub Actions (`.github/workflows/ci.yml`).

## Where it appears

| Path | What it holds |
| --- | --- |
| `docker-compose.yml` | The Kafka container (see `tech/docker-compose.md`). |
| `.github/workflows/ci.yml` | The continuous-integration jobs (see `tech/github-actions.md`). |

## How it works underneath

A YAML file is a tree. A line `name: value` is a **mapping** entry. Lines starting with `- ` are a **list**. Indentation (spaces only, never tabs) shows which entries belong to which parent. Text rarely needs quotes, but quotes are needed when a value could be misread: `"9092:9092"` is quoted because some YAML readers treat `number:number` as a base-60 number. A `>-` after a key starts a **folded block**: following indented lines are joined into one line with single spaces.

## Walkthrough of this repository's use

```yaml
services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

`services:` is a mapping; `kafka:` is one entry; `image:` and `ports:` are entries inside it. `ports:` holds a list with one item, the quoted string `"9092:9092"`.

```yaml
        options: >-
          --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
          --health-interval 5s
          --health-timeout 10s
          --health-retries 20
          --health-start-period 10s
```

`options: >-` starts a folded block. The five `--health-...` lines become one long line: `--health-cmd "..." --health-interval 5s ...`. GitHub passes that line to Docker. The `-` after `>` removes the final newline.

```yaml
on: [push, pull_request]
```

`on: [push, pull_request]` is a **flow list**: a list written on one line in square brackets.

## Try it yourself

1. Parse a YAML file with the same tool the repository uses for checking: `docker compose config > /dev/null && echo valid`.
2. See Compose's own normalised view of the file: `docker compose config | head -12`.
3. Break it on purpose: copy the file, replace the indentation of `image:` with a tab, run `docker compose -f copy.yml config`; it reports `yaml: line 6: found character that cannot start any token`.

## Mistakes people make

- Tabs for indentation: not allowed.
- Wrong indentation moves an entry to another parent without any warning.
- Unquoted values that YAML reinterprets (`no`, `on`, `1e3`, `09`). The Actions key `on` is special-cased by GitHub.
- Forgetting the space after `:`. `key:value` is one text value.

## Check yourself

1. What shows nesting in YAML? *Indentation with spaces.*
2. How is a list written? *Lines starting with `- `, or `[a, b]` on one line.*
3. What does `>-` do? *Starts a folded block: lines joined by spaces, no trailing newline.*
4. Why quote `"9092:9092"`? *So a parser cannot read it as a number.*
5. What starts a comment? *`#`.*

## Further reading

- YAML 1.2.2 specification: https://yaml.org/spec/1.2.2/

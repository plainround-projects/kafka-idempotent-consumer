# EditorConfig

## What it is

`.editorconfig` is a small settings file that many text editors read to apply the same basic style: character encoding, line endings, indentation. It is not a program; your editor (or a plugin) reads it.

## Why this repository uses it

The template asks for it in every repository, so files edited in different editors keep the same indentation and line endings.

## Where it appears

| Path | What it does |
| --- | --- |
| `.editorconfig` | The settings. |

## How it works underneath

When you open `src/x.py`, the editor looks for `.editorconfig` in the folder and its parents and applies every `[section]` whose pattern matches the file name. Later sections override earlier ones. `root = true` stops the search from going further up.

## Walkthrough of this repository's use

```ini
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
trim_trailing_whitespace = true
indent_style = space
indent_size = 4

[*.{yml,yaml,json,toml}]
indent_size = 2

[Makefile]
indent_style = tab

[*.md]
trim_trailing_whitespace = false
```

- `[*]` matches every file: UTF-8 text, Unix line endings (`lf`), a final newline at the end of each file, trailing spaces trimmed, spaces (not tabs), four per level.
- `[*.{yml,yaml,json,toml}]`: two spaces for these formats, which are conventionally indented less.
- `[Makefile]`: tabs, because Make requires a tab to start a recipe line (`tech/make.md`).
- `[*.md]`: do not trim trailing spaces in Markdown, where two trailing spaces can mean a line break.

## Try it yourself

1. `cat .editorconfig` shows the file.
2. Open `Makefile` in an editor with EditorConfig support: pressing Tab inserts a real tab.
3. `grep -c "^	" Makefile` counts recipe lines starting with a tab.

## Mistakes people make

- Assuming it is enforced. Only editors with support apply it; ruff enforces Python layout separately.
- Mixing tabs and spaces in a Makefile.

## Check yourself

1. Is `.editorconfig` a program? *No, a settings file read by editors.*
2. What does `root = true` do? *Stops the search for more settings above this folder.*
3. Why does `Makefile` use tabs? *Make requires a tab at the start of recipe lines.*
4. Why not trim spaces in Markdown? *Trailing spaces can be meaningful there.*
5. Which settings differ for YAML and TOML? *Indentation is two spaces.*

## Further reading

- EditorConfig specification: https://spec.editorconfig.org/

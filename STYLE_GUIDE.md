# Project Style Guide

## Python Style (PEP 8)

### Naming Conventions
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `BASE_DIR`, `REQUIRED_TOOLS`)
- **Classes**: `PascalCase` (e.g., `UnitSpec`, `FilterQualityUnit`)
- **Functions**: `snake_case` (e.g., `run_command`, `load_state`)
- **Private functions**: `_snake_case` (internal use only)
- **Variables**: `snake_case` (e.g., `session_dir`, `artifact_name`)

### Formatting
- **Imports**: One per line, grouped (stdlib, third-party, local)
- **Line length**: Max 100 characters (prefer 88)
- **Spacing**: 2 spaces for indentation (not tabs)
- **Blank lines**: 2 between top-level definitions, 1 between methods

### Parameter Names
- Use full descriptive names: `session` not `sess`, `session_dir` not `sdir`
- Channel names: `channel` not `ch`
- Avoid single-letter variables except in comprehensions

## JavaScript Style

### Naming Conventions
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `SESSION_ID`, `UNITS_META`)
- **Classes**: `PascalCase` (e.g., `UnitCard`, `PipelineBuilder`)
- **Functions**: `camelCase` (e.g., `startSession`, `uploadReads`)
- **Variables**: `camelCase` (e.g., `sessionId`, `unitMeta`)
- **Private**: `_camelCase` (e.g., `_internalHelper`)

### Formatting
- **Indentation**: 2 spaces
- **Semicolons**: Use consistently
- **Quotes**: Single quotes for strings
- **Braces**: Always use braces, even for single-line blocks
- **Line length**: Max 100 characters

### Best Practices
- Use descriptive variable names: `response` not `r`, `data` not `j`
- Avoid inline conditionals in function calls
- Use `const` by default, `let` when reassignment needed
- Prefer arrow functions for callbacks

## CSS Style

### Naming Conventions
- **Classes/IDs**: `kebab-case` (e.g., `unit-card`, `pipeline-builder`)
- **Custom properties**: `--kebab-case` (e.g., `--nav-bg`, `--accent-color`)

### Formatting
- **Indentation**: 2 spaces
- **Properties**: One per line
- **Selectors**: One per line
- **Grouping**: Related properties together (layout, typography, colors)

## HTML Style

### Naming
- **Files**: `kebab-case.html` (e.g., `bulk.html` not `blk.html`)
- **IDs**: `kebab-case`
- **Classes**: `kebab-case`

### Structure
- Semantic HTML5 elements
- Consistent indentation (2 spaces)
- Attributes: double quotes
- Self-closing tags: include slash

## File Naming

- **Python**: `snake_case.py`
- **JavaScript**: `camelCase.js` or `kebab-case.js` (be consistent)
- **CSS**: `kebab-case.css`
- **HTML**: `kebab-case.html`


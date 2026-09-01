# StateForge Python Engineering Rules

These rules apply to every change in this repository. Follow the explicit
project requirements first, then the Python Packaging User Guide, PEP 8,
PEP 257, PEP 484, and the Python Guide.

## Production-code structure

- Give each module one cohesive responsibility and keep dependencies explicit.
- Do not structure a file as though it must be self-contained. Reuse an
  existing project boundary when logic belongs there.
- Introduce a helper, class, or module only when it improves reuse, clarity, or
  separation of concerns. Do not create wrappers that merely rename one call.
- Keep related mutable state in one typed object. Do not maintain parallel
  dictionaries or collections whose keys and lifecycles must remain aligned.
- Prefer direct control flow over abstraction layers that make readers jump
  between many small functions to understand one operation.
- Keep security and protocol validation linear when a single review surface is
  clearer. Any complexity suppression must be narrow and explain this reason.
- Avoid hidden coupling, mutable global state, circular imports, wildcard
  imports, and catch-all utility modules.
- Use private third-party interfaces only when the dependency documents that
  interface and the reason is visible at the integration boundary.

## Python contracts

- Use absolute imports, descriptive names, complete type annotations, and
  Google-style PEP 257 docstrings.
- Keep untrusted input validation separate from static type annotations.
- Do not use `Any`, `cast()`, `# type: ignore`, or lint suppressions to conceal
  a design problem.
- Preserve the `src/` package layout and modern `pyproject.toml` packaging.
- Keep runtime, development, and documentation dependencies separated.

## Enforcement

- Ruff formatting, linting, and complexity rules are mandatory.
- Strict mypy checking is mandatory for production source.
- Tests are automated behavioral gates. Do not substitute ad hoc manual tests,
  and do not manually review test structure unless the user requests it.
- Build and inspect the wheel and source distribution after packaging changes.
- Build MkDocs with `--strict` after documentation or public API changes.
- Do not report completion until every applicable configured check passes, or
  clearly identify a check that could not run.


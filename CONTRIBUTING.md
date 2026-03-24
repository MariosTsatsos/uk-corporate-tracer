# Contributing

Contributions are welcome. Please read this before opening a PR.

## What's useful

- Bug fixes and robustness improvements
- Support for additional Land Registry datasets (e.g. LEASES in property output)
- Better handling of edge cases (dissolved companies, non-UK PSC, LLPs)
- Performance improvements for bulk data processing
- Additional output formats (e.g. Excel, JSON)
- Tests

## What to avoid

- Do not include real personal data, API keys, or case-specific information in any PR
- Do not add dependencies without good reason — keep the install footprint small

## How to contribute

1. Fork the repo
2. Create a branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Ensure `config.py` is **not** tracked (`git status` should not show it)
5. Open a pull request with a clear description of what you changed and why

## Reporting issues

Open a GitHub issue. Include:
- Python version
- Which step failed
- The error message (redact any personal data before posting)

## Code style

- Follow the existing style (no type annotations, no docstring overload)
- Keep functions focused — one responsibility per function
- Print progress to stdout using the existing `[STEP N]` format

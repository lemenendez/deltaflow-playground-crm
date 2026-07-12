# Security Policy

## Supported Branches

Security fixes are accepted for these branches:

- `main`
- `develop`

## Reporting a Vulnerability

Please do not open public issues for potential vulnerabilities.

Use one of these channels:

- GitHub Security Advisories (preferred): open a private report in the repository Security tab.
- If Security Advisories are unavailable, contact repository maintainers directly and include a minimal reproduction.

Please include:

- Affected files and branch
- Reproduction steps
- Impact assessment
- Suggested fix (if available)

We will acknowledge reports as quickly as possible and coordinate disclosure once a fix is available.

## Hardening Notes for This Repository

This repository is a local playground, but GitHub-side security controls should still be enabled:

- Dependabot alerts and security updates
- Secret scanning
- Code scanning (CodeQL)
- Branch protection on `main`
- Require pull request reviews and passing checks before merge

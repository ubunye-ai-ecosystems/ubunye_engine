# Security Policy

## Supported Versions

We release security patches for the following versions:

| Version | Supported          |
|---------|--------------------|
| 0.1.x   | ✅ Supported        |
| < 0.1   | ❌ Not supported    |

---

## Reporting a Vulnerability

If you discover a security vulnerability in Ubunye Engine:

1. **Do not** open a public issue.
2. Email the maintainers at: **uaie@gmail.com**  
   or use GitHub’s [private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-security-advisories).
3. Include as much detail as possible:
   - Steps to reproduce
   - Affected versions and environments
   - Potential impact

We will:
- Acknowledge receipt within **48 hours**.
- Provide a status update within **5 business days**.
- Work with you on a coordinated disclosure timeline.

---

## Best Practices for Users

- Always run the **latest release**: `pip install -U ubunye-engine`.
- Keep Spark and Python patched.
- Store sensitive values (JDBC passwords, tokens) in **environment variables** or secret managers (not configs).
- Enable telemetry only if your environment allows secure metrics/log shipping.

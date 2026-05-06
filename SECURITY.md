# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |

## Security Features

### Authentication
- JWT-based authentication with httpOnly refresh tokens
- Access tokens expire after 24 hours
- Refresh tokens expire after 7 days
- All state-changing operations require authentication

### Authorization
- All stock data endpoints require valid Bearer token
- Token validation on every request
- User account activation status checked

### Data Protection
- Passwords hashed with bcrypt
- Database credentials stored in environment variables only
- No hardcoded secrets in source code
- SQL injection prevention via parameterized queries

### Transport Security
- HTTPS recommended for production
- CORS configured via environment variable
- Security headers in Nginx (CSP, HSTS, X-Frame-Options, etc.)

### Input Validation
- Comprehensive validators for all user inputs (symbols, dates, intervals, limits)
- Symbol format validation (3-4 uppercase letters)
- Date validation (no future dates)
- Enum validation for exchanges, segments, chart types

### Resource Management
- Database connection pooling configured
- Proper cleanup with try/finally blocks
- No resource leaks on shutdown

### Docker Security
- Containers run as non-root users
- Internal Docker network isolation
- MySQL root restricted to localhost

## Reporting a Vulnerability

Please report security vulnerabilities to [security@example.com](mailto:security@example.com).

We will respond within 48 hours and aim to fix critical issues within 90 days.

## Best Practices for Deployment

1. **Generate strong secrets:**
   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```

2. **Use HTTPS in production:**
   - Configure TLS termination at load balancer or Nginx
   - Set `SECURE_COOKIES=true` in production

3. **Configure CORS properly:**
   - Set `CORS_ORIGINS` to specific origins, not `*`

4. **Enable rate limiting:**
   - Already configured; adjust `RATE_LIMIT_REQUESTS` and `RATE_LIMIT_WINDOW_SECONDS`

5. **Monitor logs:**
   - All requests logged with request IDs
   - Check for repeated 401/403 patterns

6. **Keep dependencies updated:**
   - Run `uv sync --upgrade` regularly
   - Review security advisories

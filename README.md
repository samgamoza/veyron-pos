# Veyron POS

This project is a Flask-based POS, admin, inventory, and owner dashboard for Veyron's Cakes and Pastries.

## Current Online-Ready Phase

This codebase is now prepared for internet deployment with:

- environment-based configuration
- SQLite for local development or PostgreSQL via `DATABASE_URL`
- production WSGI entrypoint via `wsgi.py`
- health endpoint at `/healthz`
- secure session cookie defaults for production
- Render/Procfile deployment scaffolding
- owner/admin/cashier remote login support
- owner alert tracking for low stock, voids/refunds, and suspicious inventory adjustments
- SMTP email alert hooks controlled from owner settings

## Current Limits

- PostgreSQL support is now wired into the app, but you should still validate your production database and seed data before going live.
- In-app backup and restore remain SQLite-only. For PostgreSQL, use host-managed backups or `pg_dump` / point-in-time restore.
- Actual cloud deployment still requires your Render or hosting account, plus a real PostgreSQL connection string.

## Local Run

1. Create a virtual environment and install dependencies:

```bash
pip install -r requirements.txt
```

2. Copy `.env.example` to `.env` and adjust values if needed.

3. For local development, you can leave `DATABASE_URL` blank and use SQLite. For online deployment, set `DATABASE_URL` to your PostgreSQL connection string.

4. Run the app:

```bash
python veyron-pos.py
```

## Render Deployment

1. Push the project to GitHub.
2. Create a new Render Web Service.
3. Render can use `render.yaml`, or you can set:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn wsgi:app`
4. Set environment variables:
   - `APP_ENV=production`
   - `FLASK_DEBUG=0`
   - `SECRET_KEY=<long-random-secret>`
   - `DATABASE_URL=<your-postgresql-connection-string>`
   - `BACKUP_DIR=/opt/render/project/src/backups`
   - `ALERT_TO_EMAIL=<owner-email>`
   - `ALERT_FROM_EMAIL=<sender-email>`
   - `SMTP_HOST=<smtp-host>`
   - `SMTP_PORT=587`
   - `SMTP_USERNAME=<smtp-user>`
   - `SMTP_PASSWORD=<smtp-password>`
5. Verify `/healthz` after deploy.

## Recommended Next Phase

To make the owner-facing remote access truly production-grade, the next work should be:

1. Validate the remaining reporting and admin flows against a real PostgreSQL database.
2. Add password change/reset and remove seeded testing credentials.
3. Add object storage for downloadable backups if deploying to an ephemeral host.
4. Add alert acknowledgement or read/unread workflow for the owner dashboard.

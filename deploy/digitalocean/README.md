# Deploy Figwork on DigitalOcean

This app ships as **one Docker image**: FastAPI serves the API under `/api/*` and the built React SPA from the same host (no separate Vite dev server in production).

## 1. Create a Droplet

- **Ubuntu 22.04 LTS**, at least **2 GB RAM** (4 GB recommended if you run heavy map scoring in-process).
- Add **SSH keys**, enable **monitoring** if you like.

## 2. Install Docker

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker "$USER"
```

Log out and back in so `docker` works without `sudo`.

## 3. Ship code to the server

Either `git clone` this repo on the Droplet or `rsync` / `scp` a tarball. From the repo root on the server you should see `Dockerfile` and `docker-compose.yml`.

## 4. Configure environment

**Option A — prod overlay (recommended)**  
`docker-compose.prod.yml` loads committed `deploy/digitalocean/env.defaults`, then merges an optional **`.env`** in the repo root (Docker Compose **2.24+**; if `.env` is unsupported, run `touch .env` or upgrade Compose).

```bash
cp deploy/digitalocean/env.example .env
nano .env   # secrets + FIGWORK_AUTH_ENABLED=1, FIGWORK_ALLOWED_ORIGINS, API keys, …
```

Values in `.env` override the same keys from `env.defaults`.

**Option B — template only**  
Use `deploy/digitalocean/env.example` as a checklist and export variables however you prefer (`export …` before `docker compose` is not injected into the container unless you wire it yourself — prefer `.env` + `env_file`).

**Auth**

- Set `FIGWORK_AUTH_ENABLED=1` and a long random `FIGWORK_AUTH_SECRET` (e.g. `openssl rand -hex 32`).
- Define users in `FIGWORK_ACCOUNTS_JSON` (see `env.example`). Prefer bcrypt hashes:

  ```bash
  python3 scripts/hash_figwork_password.py
  ```

- Set `FIGWORK_COOKIE_SECURE=1` when users reach the app over **HTTPS**.
- Set `FIGWORK_ALLOWED_ORIGINS` to your real site URL(s), comma-separated (must include the exact origin the browser uses, e.g. `https://app.figwork.ai`). Wildcard `*` is rejected when auth is on (browsers cannot use `*` with credentials).
- Optional: `FIGWORK_TRUSTED_HOSTS=app.example.com` enables Starlette `TrustedHostMiddleware` (useful behind a known hostname).

**Precomputed map data (required for the map page)**  
The image **does not** include `data_pipeline/artifacts/` (excluded via `.dockerignore`, ~700 MB).  The prod overlay bind-mounts the host directory into the container read-only at `/app/data_pipeline/artifacts`.  Either:

```bash
# A) copy the latest pipeline run from your workstation
rsync -avh data_pipeline/artifacts/ root@<droplet-ip>:/opt/figwork/data_pipeline/artifacts/

# B) run the data pipeline on the server (see docs/runbooks/data-refresh.md)
```

Without artifacts the API still boots and `/health` is green, but `/api/scores/_ranked` returns an empty list and the map shows no shading.  The SDR/WorkTrigger workspace works either way (its data lives in `/data/worktrigger.sqlite3`).

## 5. Run

**Production (recommended)** — after creating `.env` from `env.example` and filling secrets:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

If Compose errors on `required: false` (versions before **2.24**), use the legacy overlay instead (single `.env` file — merge `env.defaults` into `.env` by hand first):

```bash
docker compose -f docker-compose.yml -f deploy/docker-compose.prod.legacy.yml up -d --build
```

**Smoke test on the Droplet without secrets** — base compose only (auth off, localhost CORS):

```bash
docker compose up -d --build
```

The app listens on **port 8080** inside the container; compose maps it to the host’s **8080**.

- **SQLite (WorkTrigger)** persists on the named volume mounted at `/data` inside the container (`WORKTRIGGER_DB_PATH=/data/worktrigger.sqlite3`).

## 6. Firewall

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
# If you expose 8080 directly (no reverse proxy):
# sudo ufw allow 8080/tcp
sudo ufw enable
```

## 7. HTTPS (recommended)

Put **Caddy** or **nginx** on the host (ports 80/443) and reverse-proxy to `127.0.0.1:8080`. A full example is in `deploy/digitalocean/Caddyfile.example`.

Then set `FIGWORK_ALLOWED_ORIGINS=https://app.example.com` and `FIGWORK_COOKIE_SECURE=1`.

## 8. Health checks

- **`GET /health`** — process is up (Docker `HEALTHCHECK` uses this).
- **`GET /health/ready`** — WorkTrigger SQLite is readable (returns **503** if the DB cannot be opened — point a load balancer health check here for stricter readiness).

## DigitalOcean App Platform (alternative)

- **Build**: Dockerfile at repo root.
- **HTTP port**: `8080`.
- Set the same environment variables in the App Platform UI (use encrypted secrets).
- Attach a **persistent disk** mounted at `/data` so `worktrigger.sqlite3` survives redeploys.

## Local smoke (Docker)

```bash
docker compose up --build
# open http://localhost:8080  (if FIGWORK_AUTH_ENABLED=0 in .env)
```

When auth is enabled, you’ll get the sign-in screen first; after login, the session cookie is sent on all `/api` requests automatically (see `frontend/src/main.tsx`).

# VaultSpeed Agent Docker Image

Run the VaultSpeed Agent in a Linux container. The image downloads the agent via the API on first run and starts it.

## Quick start

**1. Go to project and create volume directory**

```bash
cd /Users/amata/Documents/WorkDir/Cursor/docker_image_training
mkdir -p /Users/amata/Documents/WorkDir/agent_docker_rundir
```

**2. Build the image**

```bash
docker build -t vaultspeed-agent .
```

**3. Run the agent**

```bash
docker run -it --rm \
  -e VS_ENVIR=training-eu \
  -e VS_USER="student24@taaf.be" \
  -e 'VS_PASSWORD=taafVSdemo24!!' \
  -v /Users/amata/Documents/WorkDir/agent_docker_rundir:/opt/vaultspeed-agent/agent_root \
  vaultspeed-agent
```

First run downloads the agent, configures it, and starts it. Later runs reuse the agent in the volume. Stop with **Ctrl+C**.

**Run from Docker Desktop (Mac):**

The project includes `docker-compose.yml` with the same env and volume. You can:

- **Terminal:** From this folder run `docker compose up`. Builds if needed, starts the agent, shows logs. Stop with **Ctrl+C**.
- **Docker Desktop:** Open Docker Desktop → **Containers** → use **Create** / **Import** and select this project folder (where `docker-compose.yml` lives). Docker will show the Compose project; start it from the UI. Or open the folder in your IDE and use Docker Desktop’s Compose integration if available.

## Options

- **Volume:** Logs, config, and `vs-agent.jar` are in `/Users/amata/Documents/WorkDir/agent_docker_rundir/agent/`. Replace `vs-agent.jar` there to upgrade; next run uses the new jar.
- **Shell inside container:** Add `--entrypoint /bin/bash` to the `docker run` command.
- **Other env:** `VS_ENVIR` (default `app`), `CONNSTR` (JDBC connection string). Password with `!` or `$`: use single quotes.

## What’s in the image

Ubuntu 24.04, OpenJDK 21, Python, and scripts that call the VaultSpeed API to download the agent and start it. No JDK file in `jdk/` needed; optional: put an OpenJDK 21 Linux `.tar.gz` in `jdk/` to use your own JDK.

## Files

`Dockerfile` · `scripts/download_and_start_agent.sh` · `scripts/vaultspeed_api.py` · `requirements.txt` · `jdk/` (optional)

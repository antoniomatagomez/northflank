#!/bin/bash
# Download VaultSpeed agent via REST API (using Python) and start it.
# Requires: VS_USER, VS_PASSWORD env vars (or pass as args).
# Optional: CONNSTR, AGENT_PARENT_FOLDER, VS_ENVIR

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_HOME="$(dirname "$SCRIPT_DIR")"
PYTHON_SCRIPT="${SCRIPT_DIR}/vaultspeed_api.py"

# Parameters: can be env vars or positional args
VS_USER="${VS_USER:-$1}"
VS_PASSWORD="${VS_PASSWORD:-$2}"
CONNSTR="${CONNSTR:-postgres.url=jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres}"
AGENT_PARENT_FOLDER="${AGENT_PARENT_FOLDER:-${AGENT_HOME}/agent_root}"
VS_ENVIR="${VS_ENVIR:-app}"
AGENT_ZIP_FILENAME="agent.zip"
AGENT_ZIP_FILEPATH="${AGENT_PARENT_FOLDER}/${AGENT_ZIP_FILENAME}"
AGENT_FOLDER="agent"
AGENT_FOLDER_PATH="${AGENT_PARENT_FOLDER}/${AGENT_FOLDER}"
HOME_FOLDER="/home"
if [ -x "${JAVA_HOME}/bin/java" ]; then
    JAVA_CMD="${JAVA_HOME}/bin/java"
elif command -v java >/dev/null 2>&1; then
    JAVA_CMD="java"
else
    echo "Error: Java not found. Set JAVA_HOME or ensure java is on PATH." >&2
    exit 1
fi

if [ -z "$VS_USER" ] || [ -z "$VS_PASSWORD" ]; then
    echo "Usage: VS_USER=user VS_PASSWORD=pass $0 [user] [password]" >&2
    echo "  Or pass username and password as first and second arguments." >&2
    exit 1
fi

mkdir -p "$AGENT_PARENT_FOLDER"
cd "$AGENT_PARENT_FOLDER"

# Skip download if agent dir already has vs-agent.jar (e.g. volume with replaced jar)
if [ -f "${AGENT_FOLDER_PATH}/vs-agent.jar" ]; then
    echo ">>> Using existing agent (vs-agent.jar present in volume); skipping download."
else
    # Use Python to get token and download agent
    echo ">>> Getting bearer token and downloading agent via REST API (Python)"
    OUTPUT=$(python3 "$PYTHON_SCRIPT" --username "$VS_USER" --password "$VS_PASSWORD" --envir "$VS_ENVIR" --download "$AGENT_ZIP_FILEPATH") || exit 1
    echo "$OUTPUT"
    LAST_LINE=$(echo "$OUTPUT" | tail -n1)
    if [ -n "$LAST_LINE" ] && [ "${LAST_LINE#VS_TOKEN=}" != "$LAST_LINE" ]; then
        export VS_TOKEN="${LAST_LINE#VS_TOKEN=}"
    fi

    if [ ! -f "$AGENT_ZIP_FILEPATH" ]; then
        echo ">>> Download failed: agent.zip not found. Check credentials and network." >&2
        exit 1
    fi

    echo ">>> Unzipping ${AGENT_ZIP_FILENAME}"
    unzip -o "$AGENT_ZIP_FILEPATH" -d "$AGENT_PARENT_FOLDER"
fi

# Update paths in config (agent home and log dir)
echo ">>> Updating agent config paths"
if [ -f "${AGENT_FOLDER_PATH}/client.properties" ]; then
    sed -i "s|${HOME_FOLDER}|${AGENT_PARENT_FOLDER}|g" "${AGENT_FOLDER_PATH}/client.properties"
fi
if [ -f "${AGENT_FOLDER_PATH}/logging.properties" ]; then
    sed -i "s|\./log|${AGENT_FOLDER_PATH}/log|g" "${AGENT_FOLDER_PATH}/logging.properties"
fi

# Add connection string if not already present
if [ -f "${AGENT_FOLDER_PATH}/connections.properties" ] && ! grep -q "postgres.url=" "${AGENT_FOLDER_PATH}/connections.properties" 2>/dev/null; then
    echo ">>> Adding connection string to connections.properties"
    echo "$CONNSTR" >> "${AGENT_FOLDER_PATH}/connections.properties"
fi

echo ">>> Starting VaultSpeed agent in foreground (Java: $JAVA_CMD)"
"$JAVA_CMD" -version || true
exec "$JAVA_CMD" \
    -Djava.util.logging.config.file="${AGENT_FOLDER_PATH}/logging.properties" \
    -jar "${AGENT_FOLDER_PATH}/vs-agent.jar" \
    "propsfile=${AGENT_FOLDER_PATH}/client.properties"

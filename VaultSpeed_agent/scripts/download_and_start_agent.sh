#!/bin/bash
# Download VaultSpeed agent via REST API (using Python) and start it.
# Requires: VS_USER, VS_PASSWORD env vars (or pass as args).
# Optional: CONNSTR, CONNECTIONS_PROPERTIES_B64 (base64 of full connections.properties — replaces zip file after unzip),
# AGENT_PARENT_FOLDER, VS_ENVIR, VS_FORCE_AGENT_DOWNLOAD (1/true/yes: clear cached zip/agent and re-download)

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

# So Airflow / other UID 50000 processes on the shared RWX volume can read configs (zip often ships 600).
umask 000

mkdir -p "$AGENT_PARENT_FOLDER"
cd "$AGENT_PARENT_FOLDER"

# After credential / VS_ENVIR changes, the volume may still hold another user's agent; force a fresh download.
_force="${VS_FORCE_AGENT_DOWNLOAD:-}"
case "$_force" in 1|true|TRUE|yes|YES) _force_on=1 ;; *) _force_on=0 ;; esac
if [ "$_force_on" = "1" ]; then
    echo ">>> VS_FORCE_AGENT_DOWNLOAD: removing ${AGENT_ZIP_FILEPATH} and ${AGENT_FOLDER_PATH} for fresh download"
    rm -f "$AGENT_ZIP_FILEPATH"
    rm -rf "${AGENT_FOLDER_PATH}"
fi

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

# Copy FMC_Deploy.sh from image into the agent directory on the volume.
# VaultSpeed references it via fmc_deploy.cmd in connections.properties.
FMC_DEPLOY_SRC="${AGENT_HOME}/FMC_Deploy.sh"
FMC_DEPLOY_DST="${AGENT_FOLDER_PATH}/FMC_Deploy.sh"
if [ -f "$FMC_DEPLOY_SRC" ]; then
    echo ">>> Copying FMC_Deploy.sh to ${FMC_DEPLOY_DST}"
    cp "$FMC_DEPLOY_SRC" "$FMC_DEPLOY_DST"
    chmod 777 "$FMC_DEPLOY_DST"
fi

# Update paths in config (agent home and log dir)
echo ">>> Updating agent config paths"
if [ -f "${AGENT_FOLDER_PATH}/client.properties" ]; then
    sed -i "s|${HOME_FOLDER}|${AGENT_PARENT_FOLDER}|g" "${AGENT_FOLDER_PATH}/client.properties"
fi
if [ -f "${AGENT_FOLDER_PATH}/logging.properties" ]; then
    sed -i "s|\./log|${AGENT_FOLDER_PATH}/log|g" "${AGENT_FOLDER_PATH}/logging.properties"
fi

# Write Snowflake RSA private key if provided (used by Snowflake JDBC private_key_file).
if [ -n "${SNOWFLAKE_RSA_KEY_B64:-}" ]; then
    RSA_KEY_REMOTE_NAME="${SNOWFLAKE_RSA_KEY_REMOTE_NAME:-rsa_key.p8}"
    RSA_KEY_TARGET="${AGENT_PARENT_FOLDER}/${RSA_KEY_REMOTE_NAME}"
    echo ">>> Writing Snowflake RSA key to ${RSA_KEY_TARGET}"
    if printf '%s' "$SNOWFLAKE_RSA_KEY_B64" | base64 -d >"${RSA_KEY_TARGET}" 2>/dev/null; then
        :
    elif printf '%s' "$SNOWFLAKE_RSA_KEY_B64" | base64 --decode >"${RSA_KEY_TARGET}" 2>/dev/null; then
        :
    elif printf '%s' "$SNOWFLAKE_RSA_KEY_B64" | base64 -D >"${RSA_KEY_TARGET}" 2>/dev/null; then
        :
    else
        echo "ERROR: base64 decode of SNOWFLAKE_RSA_KEY_B64 failed." >&2
        exit 1
    fi
    chmod 600 "${RSA_KEY_TARGET}" 2>/dev/null || true
fi

# Replace connections.properties wholesale (workshop / per-lab JDBC), or append CONNSTR line only.
if [ -n "${CONNECTIONS_PROPERTIES_B64:-}" ]; then
    echo ">>> Writing connections.properties from CONNECTIONS_PROPERTIES_B64"
    if printf '%s' "$CONNECTIONS_PROPERTIES_B64" | base64 -d >"${AGENT_FOLDER_PATH}/connections.properties" 2>/dev/null; then
        :
    elif printf '%s' "$CONNECTIONS_PROPERTIES_B64" | base64 --decode >"${AGENT_FOLDER_PATH}/connections.properties" 2>/dev/null; then
        :
    elif printf '%s' "$CONNECTIONS_PROPERTIES_B64" | base64 -D >"${AGENT_FOLDER_PATH}/connections.properties" 2>/dev/null; then
        :
    else
        echo "ERROR: base64 decode of CONNECTIONS_PROPERTIES_B64 failed." >&2
        exit 1
    fi
elif [ -f "${AGENT_FOLDER_PATH}/connections.properties" ] && ! grep -q "postgres.url=" "${AGENT_FOLDER_PATH}/connections.properties" 2>/dev/null; then
    echo ">>> Adding connection string to connections.properties"
    echo "$CONNSTR" >> "${AGENT_FOLDER_PATH}/connections.properties"
fi

# Shared RWX volume: widen permissions after Java is up. We use chmod -R 777 plus an explicit find pass
# (some RWX/NFS setups apply -R oddly on subtrees like agent/). Do not swallow failures silently.
AGENT_POST_START_CHMOD_DELAY_SEC="${AGENT_POST_START_CHMOD_DELAY_SEC:-25}"

# Extra seconds to sleep between further chmod passes after the first (comma-separated). Unset defaults to 60,180; set to empty to disable repeats.
AGENT_CHMOD_REPEAT_DELAYS_SEC="${AGENT_CHMOD_REPEAT_DELAYS_SEC-60,180}"

apply_chmod_777_agent_root() {
    local d="$AGENT_PARENT_FOLDER"
    [ -d "$d" ] || return 0
    echo ">>> chmod -R 777 ${d}"
    if ! chmod -R 777 "$d"; then
        echo "WARN: chmod -R 777 exited non-zero ($?); running find-based chmod anyway" >&2
    fi
    # Per file/dir: clears setgid dirs and fixes *.properties even if -R skipped entries
    echo ">>> find … chmod 777 (dirs then files) under ${d}"
    find "$d" -type d -exec chmod 777 {} + 2>/dev/null || find "$d" -type d -exec chmod 777 {} \;
    find "$d" -type f -exec chmod 777 {} + 2>/dev/null || find "$d" -type f -exec chmod 777 {} \;
    # Proof in logs (first properties file we see)
    sample="$(find "$d" -maxdepth 3 -name '*.properties' -print 2>/dev/null | head -1)"
    if [ -n "$sample" ]; then
        echo ">>> sample after chmod: $(ls -l "$sample" 2>/dev/null || true)"
    fi
}

chmod_watch_background() {
    (
        sleep "$AGENT_POST_START_CHMOD_DELAY_SEC" || exit 0
        apply_chmod_777_agent_root
        if [ -n "${AGENT_CHMOD_REPEAT_DELAYS_SEC:-}" ]; then
            local part
            # shellcheck disable=SC2086
            IFS=',' read -r -a _parts <<<"${AGENT_CHMOD_REPEAT_DELAYS_SEC// /}"
            for part in "${_parts[@]}"; do
                [ -z "$part" ] && continue
                sleep "$part" || exit 0
                echo ">>> repeat chmod after sleep ${part}s"
                apply_chmod_777_agent_root
            done
        fi
    ) &
}

echo ">>> Starting VaultSpeed agent (Java: $JAVA_CMD)"
"$JAVA_CMD" -version || true
"$JAVA_CMD" \
    -Djava.util.logging.config.file="${AGENT_FOLDER_PATH}/logging.properties" \
    -Dnet.snowflake.jdbc.enableBouncyCastleProvider=true \
    -jar "${AGENT_FOLDER_PATH}/vs-agent.jar" \
    "propsfile=${AGENT_FOLDER_PATH}/client.properties" &
JAVA_PID=$!

cleanup_java() {
    kill -TERM "$JAVA_PID" 2>/dev/null || true
    wait "$JAVA_PID" 2>/dev/null || true
}
trap cleanup_java INT TERM

echo ">>> Background: first chmod in ${AGENT_POST_START_CHMOD_DELAY_SEC}s, repeats: ${AGENT_CHMOD_REPEAT_DELAYS_SEC:-none}"
chmod_watch_background

wait "$JAVA_PID"
JAVA_EXIT=$?
trap - INT TERM
exit "$JAVA_EXIT"

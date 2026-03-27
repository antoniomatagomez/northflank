# VaultSpeed Agent - Linux image with Open JDK 21
# Place your OpenJDK 21 tarball (e.g. OpenJDK21*.tar.gz) in ./jdk/ before building
# Base: Ubuntu 24.04 has OpenJDK 21 in repos (Debian Bookworm does not)
# T

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV AGENT_HOME=/opt/vaultspeed-agent

# Install runtime deps: Python, unzip, curl, ca-certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    unzip \
    curl \
    ca-certificates \
    vim-tiny \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Open JDK (place your .tar.gz in ./jdk/ when building)
COPY jdk/ /tmp/jdk_install/
RUN set -e; \
    if ls /tmp/jdk_install/*.tar.gz 1>/dev/null 2>&1; then \
        cd /tmp/jdk_install && tar -xzf *.tar.gz -C /opt; \
        cd /opt && for d in jdk* java*; do [ -d "$d" ] && [ -f "$d/bin/java" ] && mv "$d" jdk && break; done; \
        if [ ! -f /opt/jdk/bin/java ]; then \
            rm -rf /opt/jdk; \
            for x in /opt/*/bin/java; do [ -f "$x" ] && mv "$(dirname "$(dirname "$x")")" /opt/jdk && break; done; \
        fi; \
        if [ ! -f /opt/jdk/bin/java ] && [ -d /opt/jdk ] && [ -f /opt/jdk/jre/bin/java ]; then \
            mkdir -p /opt/jdk/bin && ln -sf ../jre/bin/java /opt/jdk/bin/java; \
        fi; \
    fi; \
    rm -rf /tmp/jdk_install

# If no JDK was copied, install OpenJDK 21 from the repo (Ubuntu 24.04 has it)
# Use actual JVM path so it works on both amd64 and arm64 (e.g. Apple Silicon)
RUN if [ ! -d /opt/jdk ] || [ ! -f /opt/jdk/bin/java ]; then \
    apt-get update && apt-get install -y --no-install-recommends openjdk-21-jdk-headless && \
    jvm_dir=$(ls -d /usr/lib/jvm/java-21-openjdk-* 2>/dev/null | head -1) && \
    ln -sf "$jvm_dir" /opt/jdk && rm -rf /var/lib/apt/lists/*; \
    fi

# Verify Java is available (fail build if not)
RUN /opt/jdk/bin/java -version

ENV JAVA_HOME=/opt/jdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# App directory and scripts
WORKDIR ${AGENT_HOME}
COPY requirements.txt .
# Use a venv to avoid PEP 668 "externally-managed-environment" on Ubuntu 24.04
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt
ENV PATH="/opt/venv/bin:${PATH}"

COPY scripts/ ./scripts/
RUN chmod +x ./scripts/*.sh

# Default env (override at run)
ENV VS_ENVIR=app
ENV AGENT_PARENT_FOLDER=${AGENT_HOME}/agent_root

# Mount this path to access logs and config: agent_root/agent/*.properties, agent_root/agent/log/
VOLUME ["${AGENT_HOME}/agent_root"]

ENTRYPOINT ["/opt/vaultspeed-agent/scripts/download_and_start_agent.sh"]

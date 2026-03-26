#!/usr/bin/env python3
"""
VaultSpeed REST API helpers: login (bearer token) and download agent.
Used by download_and_start_agent.sh.
"""
import argparse
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("Error: 'requests' is required. Install with: pip install requests", file=sys.stderr)
    sys.exit(1)

DEFAULT_BASE = "https://app.vaultspeed.com"
DEFAULT_ENVIR = "app"


def get_base_url(envir: str) -> str:
    return f"https://{envir}.vaultspeed.com"


def get_bearer_token(username: str, password: str, base_url: str) -> str:
    """Authenticate and return access_token."""
    url = f"{base_url}/api/login"
    payload = {"username": username, "password": password}
    r = requests.post(url, json=payload, headers={"Content-type": "application/json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    token = data.get("access_token")
    if not token:
        raise ValueError("No access_token in login response")
    return token


def download_agent(token: str, save_path: Path, base_url: str) -> None:
    """Download agent.zip using Bearer token."""
    url = f"{base_url}/api/agent/download"
    r = requests.get(
        url,
        headers={
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        stream=True,
        timeout=120,
    )
    r.raise_for_status()
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def main():
    parser = argparse.ArgumentParser(description="VaultSpeed API: get token and/or download agent")
    parser.add_argument("--username", "-u", required=True, help="VaultSpeed username")
    parser.add_argument("--password", "-p", required=True, help="VaultSpeed password")
    parser.add_argument("--envir", "-e", default=DEFAULT_ENVIR, help="VaultSpeed environment (default: app)")
    parser.add_argument("--token-only", action="store_true", help="Only print bearer token (for shell use)")
    parser.add_argument("--download", metavar="PATH", help="Download agent.zip to this path")
    args = parser.parse_args()

    base_url = get_base_url(args.envir)

    try:
        token = get_bearer_token(args.username, args.password, base_url)
    except requests.RequestException as e:
        print(f"Login failed: {e}", file=sys.stderr)
        if hasattr(e, "response") and e.response is not None and e.response.text:
            print(e.response.text[:500], file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Login error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.token_only:
        print(token)
        return

    if args.download:
        try:
            download_agent(token, Path(args.download), base_url)
            print(f"Downloaded agent to {args.download}")
        except requests.RequestException as e:
            print(f"Download failed: {e}", file=sys.stderr)
            sys.exit(1)
        # Output token on last line so shell can capture it (e.g. VS_TOKEN=$(...))
        print(f"VS_TOKEN={token}")
    else:
        print(token)


if __name__ == "__main__":
    main()

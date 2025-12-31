from pathlib import Path


def _load_env_file() -> None:
    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        env_path = parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key and key not in os.environ:
                    os.environ[key] = value
            break


import os  # noqa: E402

_load_env_file()

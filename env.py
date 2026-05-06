"""Local env helper.

Loads variables from a local .env file into os.environ so your project can
read configuration without relying on an external `env` package which may
be incompatible with your Python version.

Usage:
  - Create a .env file next to this repo root with lines like:
	  OPENAQ_API_KEY=your_api_key_here
  - Import `env` and use `env.API_KEY` in your code (this is what
	`producer.py` expects).
"""
import os
from pathlib import Path


def _load_dotenv(env_path=None) -> None:
	if env_path is None:
		env_path = Path(__file__).parent / ".env"
	env_path = Path(env_path)
	if not env_path.exists():
		return

	for raw_line in env_path.read_text(encoding="utf-8").splitlines():
		line = raw_line.strip()
		if not line or line.startswith("#"):
			continue
		if "=" not in line:
			continue
		key, val = line.split("=", 1)
		key = key.strip()
		val = val.strip()
		if (val.startswith('"') and val.endswith('"')) or (
			val.startswith("'") and val.endswith("'")
		):
			val = val[1:-1]
		if key not in os.environ:
			os.environ[key] = val


_load_dotenv()

# Backwards-compatible attribute expected by producer.py
API_KEY = os.environ.get("OPENAQ_API_KEY") or os.environ.get("API_KEY") or ""

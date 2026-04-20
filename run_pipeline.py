#!/usr/bin/env python3
"""Bootstrap and run the monthly ridership pipeline in the repo virtualenv."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from typing import Sequence


MIN_PYTHON = (3, 10)
VENV_DIRNAME = ".venv"
INSTALL_STAMP = ".mta_ridership_install_stamp"
PIPELINE_SCRIPT = Path("pipelines") / "monthly_ridership_update.py"


def project_root() -> Path:
    return Path(__file__).resolve().parent


def repo_venv_python(base_dir: Path, os_name: str | None = None) -> Path:
    if (os_name or os.name) == "nt":
        return base_dir / VENV_DIRNAME / "Scripts" / "python.exe"
    return base_dir / VENV_DIRNAME / "bin" / "python"


def install_stamp_path(base_dir: Path) -> Path:
    return base_dir / VENV_DIRNAME / INSTALL_STAMP


def ensure_supported_python() -> None:
    if sys.version_info < MIN_PYTHON:
        required = ".".join(str(part) for part in MIN_PYTHON)
        current = ".".join(str(part) for part in sys.version_info[:3])
        raise RuntimeError(
            f"Python {required}+ is required. Current: {current} ({sys.executable})"
        )


def same_interpreter(path_a: Path, path_b: Path) -> bool:
    try:
        return path_a.exists() and path_b.exists() and path_a.samefile(path_b)
    except OSError:
        return path_a.resolve() == path_b.resolve()


def needs_dependency_install(base_dir: Path) -> bool:
    pyproject_path = base_dir / "pyproject.toml"
    stamp_path = install_stamp_path(base_dir)
    if not stamp_path.exists():
        return True
    return pyproject_path.stat().st_mtime > stamp_path.stat().st_mtime


def touch_install_stamp(base_dir: Path) -> None:
    stamp_path = install_stamp_path(base_dir)
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    stamp_path.touch()


def run_checked(cmd: Sequence[str], *, cwd: Path, label: str) -> None:
    try:
        subprocess.run(list(cmd), cwd=str(cwd), check=True)
    except subprocess.CalledProcessError as exc:
        joined = " ".join(str(part) for part in cmd)
        raise RuntimeError(
            f"{label} failed with exit code {exc.returncode}: {joined}"
        ) from exc


def maybe_warn_about_socrata_setup(base_dir: Path) -> None:
    env_path = base_dir / ".env"
    env_token = os.getenv("SOCRATA_APP_TOKEN", "").strip()
    if not env_path.exists():
        if not env_token:
            print("Warning: No .env file found. Anonymous Socrata requests may hit rate limits.")
        return
    if env_token:
        return
    try:
        env_text = env_path.read_text(encoding="utf-8")
    except OSError:
        print("Warning: Could not read .env.")
        return
    env_values: dict[str, str] = {}
    for raw_line in env_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_values[key.strip()] = value.strip()
    app_token = env_values.get("SOCRATA_APP_TOKEN", "")
    if not app_token or app_token == "your_app_token_here":
        print("Warning: SOCRATA_APP_TOKEN is blank or placeholder in .env.")


def run_pipeline_command(base_dir: Path, python_path: Path, argv: Sequence[str]) -> int:
    cmd = [str(python_path), str(base_dir / PIPELINE_SCRIPT), *argv]
    completed = subprocess.run(cmd, cwd=str(base_dir), check=False)
    return completed.returncode


def relaunch_inside_venv(base_dir: Path, python_path: Path, argv: Sequence[str]) -> int:
    cmd = [str(python_path), str(base_dir / "run_pipeline.py"), *argv]
    completed = subprocess.run(cmd, cwd=str(base_dir), check=False)
    return completed.returncode


def main(argv: Sequence[str] | None = None, *, base_dir: Path | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    root = project_root() if base_dir is None else base_dir.resolve()

    try:
        ensure_supported_python()
        current_python = Path(sys.executable).resolve()
        venv_python = repo_venv_python(root)
        venv_is_new = False

        if not venv_python.exists():
            print("Creating virtual environment in .venv...")
            run_checked(
                [str(current_python), "-m", "venv", str(root / VENV_DIRNAME)],
                cwd=root,
                label="Virtual environment creation",
            )
            venv_is_new = True

        if venv_is_new:
            )

        if venv_is_new or needs_dependency_install(root):
            reason = "new virtual environment" if venv_is_new else "pyproject.toml changed"
            print(f"Installing project dependencies ({reason})...")
            run_checked(
                [str(venv_python), "-m", "pip", "install", "-e", "."],
                cwd=root,
                label="Project dependency install",
            )
            touch_install_stamp(root)

        if not same_interpreter(current_python, venv_python):
            print("Relaunching inside the project virtual environment...")
            return relaunch_inside_venv(root, venv_python, args)

        maybe_warn_about_socrata_setup(root)
        return run_pipeline_command(root, venv_python, args)
    except RuntimeError as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

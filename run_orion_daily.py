import os
import sys
import json
import time
import shutil
import subprocess
import platform
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from shutil import copytree, rmtree, ignore_patterns
from typing import Optional, Dict, Any


CRACEN_NOTEBOOK = "CRACEN.ipynb"
STRATEGIES_DIRNAME = "STRATEGIES"
SIGNALS_DIRNAME = "signals"
STATUS_DIRNAME = "status"


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def find_orion_home() -> Path:
    env = os.environ.get("ORION_HOME")
    if env:
        p = Path(env).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"ORION_HOME points to missing path: {p}")
        return p
    return Path(__file__).resolve().parent


def run_cmd(cmd, cwd: Path, env=None):
    p = subprocess.run(cmd, cwd=str(cwd), env=env, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def run_notebook(nb_path: Path, cwd: Path, env: dict):
    if shutil.which("papermill") is None:
        code, _, _ = run_cmd([sys.executable, "-m", "papermill", "-h"], cwd=cwd, env=env)
        if code != 0:
            return 2, "", "papermill not installed. Install: pip install papermill"

    out_nb = cwd / STATUS_DIRNAME / f"last_{nb_path.stem}_out.ipynb"
    out_nb.parent.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "papermill", str(nb_path), str(out_nb), "-k", "python3"]
    return run_cmd(cmd, cwd=cwd, env=env)


def write_status(orion_home: Path, status: dict):
    st_dir = orion_home / STATUS_DIRNAME
    st_dir.mkdir(parents=True, exist_ok=True)
    (st_dir / "latest.json").write_text(json.dumps(status, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    md = []
    md.append("# OriON Daily Status\n")
    md.append(f"- Updated (UTC): **{status.get('updated_at_utc')}**")
    md.append(f"- Host: **{status.get('host')}**\n")

    md.append("## GitHub")
    gh = status.get("github", {})
    md.append(f"- strategies repo: `{gh.get('strategies_repo')}`")
    if gh.get("strategies_sha"): md.append(f"- strategies sha: `{gh.get('strategies_sha')}`")
    if gh.get("strategies_updated") is not None: md.append(f"- strategies updated: **{gh.get('strategies_updated')}**")
    if gh.get("strategies_error"): md.append(f"- strategies error: `{gh.get('strategies_error')}`")
    md.append(f"- results repo: `{gh.get('results_repo')}`")
    if gh.get("results_commit"): md.append(f"- results commit: `{gh.get('results_commit')}`")
    if gh.get("results_pushed") is not None: md.append(f"- results pushed: **{gh.get('results_pushed')}**")
    if gh.get("results_error"): md.append(f"- results error: `{gh.get('results_error')}`")
    if gh.get("results_layout"): md.append(f"- results layout: `{gh.get('results_layout')}`")
    if gh.get("results_subdir") is not None: md.append(f"- results subdir: `{gh.get('results_subdir')}`")
    md.append("")

    md.append("## Datum API")
    da = status.get("datum_api", {})
    md.append(f"- ok: **{da.get('ok')}**")
    if da.get("config_path"): md.append(f"- config: `{da.get('config_path')}`")
    if da.get("credentials_path"): md.append(f"- credentials: `{da.get('credentials_path')}`")
    if da.get("staged_config_path"): md.append(f"- staged config: `{da.get('staged_config_path')}`")
    if da.get("staged_credentials_path"): md.append(f"- staged credentials: `{da.get('staged_credentials_path')}`")
    if da.get("error"): md.append(f"- error: `{da.get('error')}`")
    md.append("")

    md.append("## CRACEN")
    cr = status.get("cracen", {})
    md.append(f"- ok: **{cr.get('ok')}**")
    if cr.get("error"): md.append(f"- error: `{cr.get('error')}`")
    md.append(f"- final: `{cr.get('final_path')}`\n")

    md.append("## Strategies")
    for k, v in (status.get("strategies") or {}).items():
        ok = "✅" if v.get("ok") else "❌"
        extra = f" ({int(v.get('duration_sec', 0))}s)" if v.get("duration_sec") else ""
        if v.get("error"): extra += f" — {v['error']}"
        md.append(f"- {ok} **{k}**{extra}")

    if status.get("fatal_error"):
        md.append("\n## Fatal")
        md.append(f"- `{status.get('fatal_error')}`")

    (st_dir / "latest.md").write_text("\n".join(md) + "\n", encoding="utf-8")


# ---------------------------
# GitHub token (HARD RULE)
# ---------------------------
def read_github_token(orion_home: Path) -> str:
    """
    HARD RULE:
      Read GitHub PAT ONLY from OriON/ops/access_token.json
    Never read OriON/access_token.json (Datum).
    """
    p = (orion_home / "ops" / "access_token.json").resolve()
    if not p.exists():
        raise FileNotFoundError(f"Missing GitHub token file: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    token = data.get("token") or data.get("github_pat") or data.get("pat")
    if not token or not isinstance(token, str):
        raise RuntimeError(f"GitHub token not found in {p}. Expected key: token/github_pat/pat")
    return token.strip()


def normalize_repo_url_to_https(repo_url: str) -> str:
    """
    Accepts:
      - https://github.com/user/repo.git
      - git@github.com:user/repo.git
    Returns HTTPS form (without token).
    """
    s = repo_url.strip()
    if s.startswith("https://"):
        return s
    if s.startswith("git@github.com:"):
        return "https://github.com/" + s.split("git@github.com:", 1)[1]
    return s


def with_token_https(repo_url_https: str, token: str) -> str:
    """
    Inject token into HTTPS URL:
      https://github.com/u/r.git -> https://<token>@github.com/u/r.git
    """
    if not token:
        return normalize_repo_url_to_https(repo_url_https)
    url = normalize_repo_url_to_https(repo_url_https)
    if url.startswith("https://"):
        return "https://" + token + "@" + url[len("https://"):]
    return url


def ls_remote_head_sha(repo_url: str, branch: str, token: str) -> str:
    tmp_base = Path(tempfile.gettempdir())
    url = with_token_https(repo_url, token)
    code, so, se = run_cmd(["git", "ls-remote", url, f"refs/heads/{branch}"], cwd=tmp_base)
    if code != 0:
        raise RuntimeError(f"git ls-remote failed: {se.strip() or so.strip()}")
    if not so.strip():
        raise RuntimeError("git ls-remote returned empty output")
    return so.strip().split()[0][:12]


def clone_depth1(repo_url: str, branch: str, dst: Path, token: str):
    url = with_token_https(repo_url, token)
    if dst.exists():
        shutil.rmtree(dst, ignore_errors=True)
    code, so, se = run_cmd(
        ["git", "clone", "--depth", "1", "--branch", branch, url, str(dst)],
        cwd=Path(tempfile.gettempdir())
    )
    if code != 0:
        raise RuntimeError(f"git clone failed: {se.strip() or so.strip()}")


def update_strategies_clone_swap(repo_url: str, branch: str, dst_dir: Path, token: str) -> Dict[str, Any]:
    """
    Clone+swap STRATEGIES atomically.
    Returns {"updated":bool,"sha":str|None,"error":str|None,"backup":str|None}
    """
    out = {"updated": False, "sha": None, "error": None, "backup": None}

    remote_sha = ls_remote_head_sha(repo_url, branch, token)
    out["sha"] = remote_sha

    local_sha = None
    if dst_dir.exists() and (dst_dir / ".git").exists():
        c, so, _ = run_cmd(["git", "rev-parse", "--short=12", "HEAD"], cwd=dst_dir)
        if c == 0:
            local_sha = so.strip()

    if local_sha and local_sha == remote_sha:
        return out  # no update

    tmp_dir = Path(tempfile.gettempdir()) / f"orion_strategies_{remote_sha}_{int(time.time())}"
    clone_depth1(repo_url, branch, tmp_dir, token)

    # sanity: must have at least one .ipynb somewhere
    found_ipynb = list(tmp_dir.rglob("*.ipynb"))
    if not found_ipynb:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        out["error"] = "No notebooks found in strategies repo"
        return out

    backup_dir = None
    if dst_dir.exists():
        backup_dir = dst_dir.parent / f"{dst_dir.name}_backup_{local_sha or int(time.time())}"
        i = 0
        while backup_dir.exists():
            i += 1
            backup_dir = dst_dir.parent / f"{dst_dir.name}_backup_{local_sha or int(time.time())}_{i}"
        shutil.move(str(dst_dir), str(backup_dir))

    shutil.move(str(tmp_dir), str(dst_dir))
    out["updated"] = True
    out["backup"] = str(backup_dir) if backup_dir else None
    return out


def push_results_to_repo(
    results_repo_url: str,
    branch: str,
    token: str,
    orion_home: Path,
    *,
    results_layout: str = "root",
    results_subdir: str = "",
) -> Dict[str, Any]:
    """
    Clone results repo into temp, overwrite signals/ and status/ into:
      - layout=root: repo_root/(signals,status)
      - layout=subdir OR results_subdir provided: repo_root/<subdir>/(signals,status)
    Then commit & push.
    """
    out = {"pushed": False, "commit": None, "error": None}

    ts = utc_now_iso()
    tmp_base = Path(tempfile.gettempdir())
    tmp_dir = tmp_base / f"orion_stats_{int(time.time())}"

    subdir = (results_subdir or "").strip().strip("/").strip("\\")
    use_subdir = (results_layout.lower() == "subdir") or bool(subdir)

    try:
        url = with_token_https(results_repo_url, token)
        c, so, se = run_cmd(["git", "clone", "--depth", "1", "--branch", branch, url, str(tmp_dir)], cwd=tmp_base)
        if c != 0:
            out["error"] = f"git clone failed: {se.strip() or so.strip()}"
            return out

        src_signals = orion_home / SIGNALS_DIRNAME
        src_status = orion_home / STATUS_DIRNAME
        if not src_signals.exists() and not src_status.exists():
            out["error"] = "No signals/ or status/ to push"
            return out

        base = tmp_dir / subdir if use_subdir else tmp_dir
        base.mkdir(parents=True, exist_ok=True)

        dest_signals = base / SIGNALS_DIRNAME
        dest_status = base / STATUS_DIRNAME

        if dest_signals.exists():
            rmtree(str(dest_signals), ignore_errors=True)
        if dest_status.exists():
            rmtree(str(dest_status), ignore_errors=True)

        if src_signals.exists():
            copytree(str(src_signals), str(dest_signals), dirs_exist_ok=True, ignore=ignore_patterns("*.tmp", "*.lock"))
        if src_status.exists():
            copytree(str(src_status), str(dest_status), dirs_exist_ok=True, ignore=ignore_patterns("*.tmp", "*.lock"))

        add_signals = str(Path(subdir) / SIGNALS_DIRNAME) if use_subdir else SIGNALS_DIRNAME
        add_status = str(Path(subdir) / STATUS_DIRNAME) if use_subdir else STATUS_DIRNAME

        run_cmd(["git", "add", add_signals], cwd=tmp_dir)
        run_cmd(["git", "add", add_status], cwd=tmp_dir)

        code, _, _ = run_cmd(["git", "diff", "--cached", "--quiet"], cwd=tmp_dir)
        if code == 0:
            return out  # nothing to commit

        run_cmd(["git", "config", "user.name", "orion-bot"], cwd=tmp_dir)
        run_cmd(["git", "config", "user.email", "orion-bot@local"], cwd=tmp_dir)

        cm = f"orion: update signals/status {ts}"
        c2, so2, se2 = run_cmd(["git", "commit", "-m", cm], cwd=tmp_dir)
        if c2 != 0:
            out["error"] = f"git commit failed: {se2.strip() or so2.strip()}"
            return out

        c3, so3, _ = run_cmd(["git", "rev-parse", "HEAD"], cwd=tmp_dir)
        if c3 == 0:
            out["commit"] = so3.strip() or None

        c4, so4, se4 = run_cmd(["git", "push", "origin", branch], cwd=tmp_dir)
        if c4 != 0:
            out["error"] = f"git push failed: {se4.strip() or so4.strip()}"
            return out

        out["pushed"] = True
        return out

    finally:
        try:
            rmtree(str(tmp_dir), ignore_errors=True)
        except Exception:
            pass


# ---------------------------
# Datum API secrets (as you had)
# ---------------------------
def resolve_datum_secrets(repo_root: Path, orion_home: Path):
    candidates = [
        (repo_root / "datum_api_config.json", repo_root / "datum_api_credentials.json"),
        (orion_home / "datum_api_config.json", orion_home / "datum_api_credentials.json"),
    ]
    for cfg, creds in candidates:
        if cfg.exists() and creds.exists():
            return cfg, creds

    checked = []
    for cfg, creds in candidates:
        checked.append(str(cfg))
        checked.append(str(creds))
    raise FileNotFoundError(
        "Datum API secrets not found. Expected BOTH files:\n"
        "- datum_api_config.json\n"
        "- datum_api_credentials.json\n"
        "Checked paths:\n" + "\n".join(f"- {p}" for p in checked)
    )


def stage_datum_secrets(repo_root: Path, orion_home: Path):
    src_cfg = repo_root / "datum_api_config.json"
    src_creds = repo_root / "datum_api_credentials.json"
    if not src_cfg.exists():
        raise FileNotFoundError(f"Missing: {src_cfg}")
    if not src_creds.exists():
        raise FileNotFoundError(f"Missing: {src_creds}")

    dst_cfg = orion_home / "datum_api_config.json"
    dst_creds = orion_home / "datum_api_credentials.json"
    shutil.copy2(src_cfg, dst_cfg)
    shutil.copy2(src_creds, dst_creds)
    return dst_cfg, dst_creds


def cleanup_datum_secrets(orion_home: Path):
    for name in ["datum_api_config.json", "datum_api_credentials.json"]:
        p = orion_home / name
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def load_ops_config(orion_home: Path) -> Dict[str, Any]:
    cfg_path = (orion_home / "ops" / "config.json").resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing ops config: {cfg_path}")
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    cfg.setdefault("strategies_repo", "https://github.com/bohdan6992/OriON-strategies.git")
    cfg.setdefault("strategies_branch", "main")
    cfg.setdefault("results_repo", "https://github.com/bohdan6992/OriON-stats.git")
    cfg.setdefault("results_branch", "main")
    cfg.setdefault("results_layout", "root")     # root | subdir
    cfg.setdefault("results_subdir", "")
    cfg.setdefault("use_clone_swap", True)
    return cfg


def main():
    t0 = time.time()
    host = platform.node()

    orion_home = find_orion_home()
    repo_root = orion_home.parent  # level above OriON (datum secrets live here)

    (orion_home / SIGNALS_DIRNAME).mkdir(parents=True, exist_ok=True)
    (orion_home / STATUS_DIRNAME).mkdir(parents=True, exist_ok=True)

    final_path = orion_home / "CRACEN" / "final.parquet"

    env = os.environ.copy()
    env["ORION_HOME"] = str(orion_home)
    env["FINAL_PARQUET_PATH"] = str(final_path)
    env["SIGNALS_DIR"] = str(orion_home / SIGNALS_DIRNAME)

    status = {
        "job": "OriON daily",
        "updated_at_utc": None,
        "host": host,
        "github": {
            "strategies_repo": None,
            "strategies_sha": None,
            "strategies_updated": None,
            "strategies_error": None,
            "results_repo": None,
            "results_commit": None,
            "results_pushed": None,
            "results_error": None,
            "results_layout": None,
            "results_subdir": None,
        },
        "datum_api": {
            "ok": False,
            "config_path": None,
            "credentials_path": None,
            "staged_config_path": None,
            "staged_credentials_path": None,
            "error": None,
        },
        "cracen": {"ok": False, "final_path": str(final_path), "error": None},
        "strategies": {},
        "durations_sec": {},
        "fatal_error": None,
    }

    staged = None
    try:
        # --------------------------
        # 0) Read ops config + GitHub token (ONLY from ops)
        # --------------------------
        ops_cfg = load_ops_config(orion_home)
        token = read_github_token(orion_home)

        strategies_repo = ops_cfg.get("strategies_repo")
        strategies_branch = ops_cfg.get("strategies_branch", "main")
        results_repo = ops_cfg.get("results_repo")
        results_branch = ops_cfg.get("results_branch", "main")
        results_layout = ops_cfg.get("results_layout", "root")
        results_subdir = ops_cfg.get("results_subdir", "")
        use_clone_swap = bool(ops_cfg.get("use_clone_swap", True))

        status["github"]["strategies_repo"] = strategies_repo
        status["github"]["results_repo"] = results_repo
        status["github"]["results_layout"] = results_layout
        status["github"]["results_subdir"] = results_subdir

        # --------------------------
        # 1) Update STRATEGIES from git (clone + swap) if enabled
        # --------------------------
        if use_clone_swap:
            try:
                upd = update_strategies_clone_swap(
                    repo_url=strategies_repo,
                    branch=strategies_branch,
                    dst_dir=(orion_home / STRATEGIES_DIRNAME),
                    token=token,
                )
                status["github"]["strategies_sha"] = upd.get("sha")
                status["github"]["strategies_updated"] = bool(upd.get("updated"))
                if upd.get("error"):
                    status["github"]["strategies_error"] = upd.get("error")
            except Exception as ex:
                status["github"]["strategies_error"] = str(ex)[:2000]
        else:
            status["github"]["strategies_updated"] = False

        # --------------------------
        # 2) Resolve + stage Datum API secrets into OriON cwd
        # --------------------------
        cfg_path, creds_path = resolve_datum_secrets(repo_root, orion_home)
        status["datum_api"]["config_path"] = str(cfg_path)
        status["datum_api"]["credentials_path"] = str(creds_path)

        if cfg_path.parent == repo_root and creds_path.parent == repo_root:
            dst_cfg, dst_creds = stage_datum_secrets(repo_root, orion_home)
            staged = (dst_cfg, dst_creds)
            status["datum_api"]["staged_config_path"] = str(dst_cfg)
            status["datum_api"]["staged_credentials_path"] = str(dst_creds)
        else:
            status["datum_api"]["staged_config_path"] = str(cfg_path)
            status["datum_api"]["staged_credentials_path"] = str(creds_path)

        env["DATUM_API_CONFIG_PATH"] = str(cfg_path)
        env["DATUM_API_CREDENTIALS_PATH"] = str(creds_path)
        env["DATUM_CONFIG_PATH"] = str(cfg_path)
        env["DATUM_CREDENTIALS_PATH"] = str(creds_path)
        env["DATUM_API_CFG_PATH"] = str(cfg_path)
        env["DATUM_API_CREDS_PATH"] = str(creds_path)
        status["datum_api"]["ok"] = True

        # --------------------------
        # 3) run CRACEN
        # --------------------------
        cracen_nb = (orion_home / STRATEGIES_DIRNAME / CRACEN_NOTEBOOK)
        if not cracen_nb.exists():
            status["updated_at_utc"] = utc_now_iso()
            status["cracen"]["ok"] = False
            status["cracen"]["error"] = f"Missing: {cracen_nb}"
            write_status(orion_home, status)
            return 1

        t_cr = time.time()
        code, so, se = run_notebook(cracen_nb, cwd=orion_home, env=env)
        status["durations_sec"]["cracen"] = time.time() - t_cr

        if code != 0:
            status["updated_at_utc"] = utc_now_iso()
            status["cracen"]["ok"] = False
            status["cracen"]["error"] = (se.strip() or so.strip() or f"exit {code}")[:2000]
            write_status(orion_home, status)
            return 1

        if not final_path.exists():
            status["updated_at_utc"] = utc_now_iso()
            status["cracen"]["ok"] = False
            status["cracen"]["error"] = f"CRACEN finished but missing final.parquet: {final_path}"
            write_status(orion_home, status)
            return 1

        status["cracen"]["ok"] = True

        # --------------------------
        # 4) run strategies (all notebooks except CRACEN)
        # --------------------------
        strat_dir = (orion_home / STRATEGIES_DIRNAME)
        for nb in sorted(strat_dir.glob("*.ipynb")):
            if nb.name == CRACEN_NOTEBOOK:
                continue
            name = nb.stem
            t_s = time.time()
            code, so, se = run_notebook(nb, cwd=orion_home, env=env)
            info = {"ok": (code == 0), "duration_sec": time.time() - t_s}
            if code != 0:
                info["error"] = (se.strip() or so.strip() or f"exit {code}")[:2000]
            status["strategies"][name] = info

        # --------------------------
        # 5) write status
        # --------------------------
        status["updated_at_utc"] = utc_now_iso()
        status["durations_sec"]["total"] = time.time() - t0
        write_status(orion_home, status)

        # --------------------------
        # 6) push results to OriON-stats (signals + status)
        # --------------------------
        res_push = push_results_to_repo(
            results_repo_url=results_repo,
            branch=results_branch,
            token=token,
            orion_home=orion_home,
            results_layout=results_layout,
            results_subdir=results_subdir,
        )
        status["github"]["results_pushed"] = bool(res_push.get("pushed"))
        status["github"]["results_commit"] = res_push.get("commit")
        status["github"]["results_error"] = res_push.get("error")
        write_status(orion_home, status)

        if not status["cracen"]["ok"]:
            return 1
        if status["github"]["results_error"]:
            return 1
        return 0

    except Exception as e:
        status["updated_at_utc"] = utc_now_iso()
        status["fatal_error"] = str(e)[:2000]
        write_status(orion_home, status)
        return 1

    finally:
        if staged is not None:
            cleanup_datum_secrets(orion_home)


if __name__ == "__main__":
    raise SystemExit(main())

import json
import logging
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _metadata_path(file_path: str) -> str:
    return f"{file_path}.download_meta.json"


def _read_metadata(file_path: str) -> Dict[str, Any]:
    meta_path = _metadata_path(file_path)
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
    except Exception:
        logging.getLogger("DownloadRetry").warning(
            "Failed to read download retry metadata | path=%s",
            meta_path,
            exc_info=True,
        )
    return {}


def _write_metadata(file_path: str, payload: Dict[str, Any]) -> None:
    meta_path = _metadata_path(file_path)
    os.makedirs(os.path.dirname(meta_path) or ".", exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def get_download_metadata(file_path: str) -> Dict[str, Any]:
    return dict(_read_metadata(file_path))


def register_downloaded_file(file_path: str, source_url: str, suggested_title: str) -> None:
    existing = _read_metadata(file_path)
    payload = {
        "source_url": str(source_url or "").strip(),
        "suggested_title": str(suggested_title or "").strip(),
        "original_filename": os.path.basename(file_path),
        "retry_count": int(existing.get("retry_count") or 0),
        "created_at": existing.get("created_at") or _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "last_retry_reason": existing.get("last_retry_reason", ""),
        "last_retry_status": existing.get("last_retry_status", ""),
        "last_retry_error": existing.get("last_retry_error", ""),
    }
    _write_metadata(file_path, payload)


def resolve_download_target_path(directory: str, filename: str, source_url: str) -> str:
    target_dir = os.path.abspath(directory or ".")
    os.makedirs(target_dir, exist_ok=True)

    safe_name = os.path.basename(str(filename or "").strip()) or "file.bin"
    exact_path = os.path.join(target_dir, safe_name)
    return exact_path


def cleanup_download_duplicates(
    directory: str,
    filename: str,
    source_url: str,
    keep_path: str,
    *,
    logger: logging.Logger | None = None,
) -> list[str]:
    active_logger = logger or logging.getLogger("DownloadRetry")
    target_dir = os.path.abspath(directory or ".")
    safe_name = os.path.basename(str(filename or "").strip())
    source = str(source_url or "").strip()
    keep_abs_path = os.path.abspath(keep_path)

    if not safe_name or not source:
        return []

    base, ext = os.path.splitext(safe_name)
    removed_paths: list[str] = []

    try:
        pattern = re.compile(rf"^{re.escape(base)}(?:_(\d+))?{re.escape(ext)}$", flags=re.IGNORECASE)
        entries = list(os.listdir(target_dir))
    except FileNotFoundError:
        return []

    for entry in entries:
        entry_path = os.path.abspath(os.path.join(target_dir, entry))
        if entry_path == keep_abs_path or not os.path.isfile(entry_path):
            continue
        if not pattern.match(entry):
            continue

        metadata = _read_metadata(entry_path)
        is_numeric_variant = _path_suffix_rank(entry_path) > 0
        if metadata.get("source_url") != source and not is_numeric_variant:
            continue

        try:
            os.remove(entry_path)
            clear_download_metadata(entry_path)
            removed_paths.append(entry_path)
        except FileNotFoundError:
            clear_download_metadata(entry_path)
        except Exception:
            active_logger.warning(
                "Failed to remove duplicate downloaded file | keep=%s | duplicate=%s",
                keep_abs_path,
                entry_path,
                exc_info=True,
            )

    return removed_paths


def _path_suffix_rank(file_path: str) -> int:
    name = os.path.basename(file_path)
    stem, _ext = os.path.splitext(name)
    match = re.search(r"_(\d+)$", stem)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 999999


def _metadata_sort_timestamp(file_path: str, metadata: Dict[str, Any]) -> float:
    for key in ("updated_at", "created_at"):
        raw_value = str(metadata.get(key) or "").strip()
        if not raw_value:
            continue
        try:
            normalized = raw_value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized).timestamp()
        except Exception:
            continue
    try:
        return os.path.getmtime(file_path)
    except Exception:
        return 0.0


def _choose_duplicate_keep_path(entries: list[Dict[str, Any]]) -> str:
    ranked = sorted(
        entries,
        key=lambda entry: (
            _path_suffix_rank(entry["path"]),
            len(os.path.basename(entry["path"])),
            os.path.basename(entry["path"]).lower(),
        ),
    )
    return ranked[0]["path"]


def _choose_duplicate_source_path(entries: list[Dict[str, Any]]) -> str:
    ranked = sorted(
        entries,
        key=lambda entry: (
            _metadata_sort_timestamp(entry["path"], entry["metadata"]),
            _path_suffix_rank(entry["path"]) == 0,
            os.path.getsize(entry["path"]) if os.path.exists(entry["path"]) else 0,
        ),
        reverse=True,
    )
    return ranked[0]["path"]


def cleanup_download_duplicates_in_tree(
    root_dir: str,
    *,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    active_logger = logger or logging.getLogger("DownloadRetry")
    root_abs = os.path.abspath(root_dir or ".")
    groups: Dict[tuple[str, str], list[Dict[str, Any]]] = {}

    for current_root, _dirs, files in os.walk(root_abs):
        for entry in files:
            if not entry.endswith(".download_meta.json"):
                continue
            file_path = os.path.join(current_root, entry[: -len(".download_meta.json")])
            if not os.path.exists(file_path):
                clear_download_metadata(file_path)
                continue
            metadata = _read_metadata(file_path)
            source_url = str(metadata.get("source_url") or "").strip()
            if not source_url:
                continue
            groups.setdefault((current_root, source_url), []).append(
                {
                    "path": os.path.abspath(file_path),
                    "metadata": metadata,
                }
            )

    summary = {
        "root_dir": root_abs,
        "groups_scanned": len(groups),
        "groups_with_duplicates": 0,
        "files_removed": 0,
        "files_rewritten": 0,
        "results": [],
    }

    for (_group_root, source_url), entries in groups.items():
        if len(entries) <= 1:
            continue

        keep_path = _choose_duplicate_keep_path(entries)
        source_path = _choose_duplicate_source_path(entries)
        removed_paths: list[str] = []
        rewritten = False

        if not dry_run and source_path != keep_path:
            os.makedirs(os.path.dirname(keep_path) or ".", exist_ok=True)
            shutil.copy2(source_path, keep_path)
            source_metadata = dict(_read_metadata(source_path) or _read_metadata(keep_path))
            source_metadata["original_filename"] = os.path.basename(keep_path)
            source_metadata["updated_at"] = _utc_now_iso()
            _write_metadata(keep_path, source_metadata)
            rewritten = True

        for entry in entries:
            duplicate_path = entry["path"]
            if duplicate_path == keep_path:
                continue
            removed_paths.append(duplicate_path)
            if dry_run:
                continue
            try:
                if os.path.exists(duplicate_path):
                    os.remove(duplicate_path)
                clear_download_metadata(duplicate_path)
            except Exception:
                active_logger.warning(
                    "Failed to remove duplicate from tree cleanup | keep=%s | duplicate=%s",
                    keep_path,
                    duplicate_path,
                    exc_info=True,
                )

        summary["groups_with_duplicates"] += 1
        summary["files_removed"] += len(removed_paths)
        summary["files_rewritten"] += int(rewritten)
        summary["results"].append(
            {
                "source_url": source_url,
                "keep_path": keep_path,
                "source_path": source_path,
                "removed_paths": removed_paths,
                "rewritten": rewritten,
            }
        )

    return summary


def clear_download_metadata(file_path: str) -> None:
    meta_path = _metadata_path(file_path)
    if not os.path.exists(meta_path):
        return
    try:
        os.remove(meta_path)
    except Exception:
        logging.getLogger("DownloadRetry").warning(
            "Failed to remove download retry metadata | path=%s",
            meta_path,
            exc_info=True,
        )


def ensure_non_empty_downloaded_file(
    file_path: str,
    *,
    reason: str,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    active_logger = logger or logging.getLogger("DownloadRetry")
    result: Dict[str, Any] = {
        "path": file_path,
        "exists": False,
        "non_empty": False,
        "size": 0,
        "retry_attempted": False,
        "retry_succeeded": False,
    }

    if not os.path.isfile(file_path):
        return result

    result["exists"] = True

    try:
        file_size = os.path.getsize(file_path)
    except OSError as exc:
        result["error"] = str(exc)
        return result

    result["size"] = file_size
    if file_size > 0:
        result["non_empty"] = True
        return result

    active_logger.warning(
        "Empty downloaded file detected | path=%s | reason=%s",
        file_path,
        reason,
    )

    retry_result = retry_downloaded_file_once(
        file_path,
        reason=reason,
        logger=active_logger,
    )
    result["retry_attempted"] = bool(retry_result.get("attempted"))
    result["retry_succeeded"] = bool(retry_result.get("succeeded"))
    result["retry_result"] = retry_result

    if not os.path.isfile(file_path):
        result["exists"] = False
        result["size"] = 0
        return result

    result["exists"] = True
    try:
        file_size = os.path.getsize(file_path)
    except OSError as exc:
        result["error"] = str(exc)
        result["size"] = 0
        return result

    result["size"] = file_size
    result["non_empty"] = file_size > 0
    return result


def retry_downloaded_file_once(
    file_path: str,
    *,
    reason: str,
    logger: logging.Logger | None = None,
) -> Dict[str, Any]:
    active_logger = logger or logging.getLogger("DownloadRetry")
    metadata = _read_metadata(file_path)
    if not metadata:
        return {"attempted": False, "succeeded": False, "skipped_reason": "metadata_missing"}

    source_url = str(metadata.get("source_url") or "").strip()
    if not source_url:
        return {"attempted": False, "succeeded": False, "skipped_reason": "source_url_missing"}

    retry_count = int(metadata.get("retry_count") or 0)
    if retry_count >= 1:
        return {"attempted": False, "succeeded": False, "skipped_reason": "retry_limit_reached"}

    metadata["retry_count"] = retry_count + 1
    metadata["updated_at"] = _utc_now_iso()
    metadata["last_retry_reason"] = str(reason or "").strip()
    metadata["last_retry_status"] = "running"
    metadata["last_retry_error"] = ""
    _write_metadata(file_path, metadata)

    suggested_title = (
        str(metadata.get("suggested_title") or "").strip()
        or os.path.basename(file_path)
        or "file.bin"
    )

    try:
        from backend.services.eis_service import download_file_with_real_name

        target_dir = os.path.dirname(file_path) or "."
        os.makedirs(target_dir, exist_ok=True)

        with tempfile.TemporaryDirectory(dir=target_dir) as temp_dir:
            redownloaded_path = download_file_with_real_name(
                source_url,
                temp_dir,
                suggested_title,
            )
            if os.path.exists(file_path):
                os.remove(file_path)
            shutil.move(redownloaded_path, file_path)

        metadata["updated_at"] = _utc_now_iso()
        metadata["last_retry_status"] = "success"
        _write_metadata(file_path, metadata)
        active_logger.warning(
            "Redownload retry succeeded | path=%s | reason=%s",
            file_path,
            reason,
        )
        return {"attempted": True, "succeeded": True, "path": file_path}
    except Exception as exc:
        metadata["updated_at"] = _utc_now_iso()
        metadata["last_retry_status"] = "failed"
        metadata["last_retry_error"] = str(exc)
        _write_metadata(file_path, metadata)
        active_logger.warning(
            "Redownload retry failed | path=%s | reason=%s | error=%s",
            file_path,
            reason,
            exc,
        )
        return {
            "attempted": True,
            "succeeded": False,
            "error": str(exc),
            "path": file_path,
        }

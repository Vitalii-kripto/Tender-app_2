import logging
import os
import time
from typing import Callable, Optional

from backend.services.download_retry_service import ensure_non_empty_downloaded_file


ARCHIVE_EXTENSIONS = (".zip", ".7z", ".rar")
SIDECAR_SUFFIXES = (".download_meta.json",)


def _default_archive_unpacker(path: str) -> None:
    from backend.services.archive_service import archive_service

    archive_service.unpack_directory(path)


def _build_tender_paths(local_path: str, tender_id: str, documents_root: str) -> list[str]:
    paths_to_check: list[str] = []
    normalized_local_path = (local_path or "").strip()
    if normalized_local_path:
        paths_to_check.append(normalized_local_path)

    standard_dir = os.path.join(documents_root, tender_id)
    if standard_dir not in paths_to_check:
        paths_to_check.append(standard_dir)

    return paths_to_check


def _is_sidecar_file(path: str) -> bool:
    low = (path or "").lower()
    return any(low.endswith(suffix) for suffix in SIDECAR_SUFFIXES)


def _is_empty_file(path: str) -> bool:
    try:
        return os.path.isfile(path) and os.path.getsize(path) <= 0
    except OSError:
        return True


def _is_supporting_non_goods_file(path: str) -> bool:
    low = os.path.basename(path or "").lower()
    markers = (
        "нмцк",
        "нмцд",
        "обоснован",
        "смет",
        "протокол",
        "report_",
        "отчет",
    )
    return any(marker in low for marker in markers)


def _is_archive_file(path: str) -> bool:
    return str(path or "").lower().endswith(ARCHIVE_EXTENSIONS)


def collect_visible_tender_files(
    path: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> tuple[list[str], list[str]]:
    document_files: list[str] = []
    archive_files: list[str] = []
    active_logger = logger or logging.getLogger("TenderDocumentInventory")

    if not os.path.exists(path):
        return document_files, archive_files

    def append_candidate(candidate_path: str) -> None:
        if _is_sidecar_file(candidate_path):
            return
        is_archive = _is_archive_file(candidate_path)
        if _is_empty_file(candidate_path):
            ensure_non_empty_downloaded_file(
                candidate_path,
                reason=(
                    "empty_archive_detected_during_inventory"
                    if is_archive
                    else "empty_file_detected_during_inventory"
                ),
                logger=active_logger,
            )
        if is_archive:
            archive_files.append(candidate_path)
            return
        if _is_empty_file(candidate_path):
            return
        document_files.append(candidate_path)

    if os.path.isfile(path):
        append_candidate(path)
        return document_files, archive_files

    if os.path.isdir(path):
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                append_candidate(os.path.join(root, filename))

    return document_files, archive_files


def collect_tender_input_files(
    local_path: str,
    tender_id: str,
    documents_root: str,
    *,
    archive_unpacker: Optional[Callable[[str], None]] = None,
    logger: Optional[logging.Logger] = None,
) -> tuple[list[str], bool]:
    all_files: list[str] = []
    has_archives = False
    unpacker = archive_unpacker or _default_archive_unpacker

    for path in _build_tender_paths(local_path, tender_id, documents_root):
        if not os.path.exists(path):
            continue

        if os.path.isdir(path):
            try:
                unpacker(path)
            except Exception as exc:
                if logger:
                    logger.error("Error unpacking docs in %s: %s", path, exc)

        if os.path.isfile(path):
            path_files, archive_files = collect_visible_tender_files(path, logger=logger)
            has_archives = has_archives or bool(archive_files)
            for candidate in path_files:
                if candidate not in all_files:
                    all_files.append(candidate)
            continue

        if os.path.isdir(path):
            path_files, archive_files = collect_visible_tender_files(path, logger=logger)
            has_archives = has_archives or bool(archive_files)
            for candidate in path_files:
                if candidate not in all_files:
                    all_files.append(candidate)

    return all_files, has_archives


def wait_for_tender_input_files(
    local_path: str,
    tender_id: str,
    documents_root: str,
    *,
    archive_unpacker: Optional[Callable[[str], None]] = None,
    logger: Optional[logging.Logger] = None,
    max_wait_seconds: float = 15.0,
    poll_interval_seconds: float = 1.0,
    settle_seconds: float = 2.0,
) -> tuple[list[str], bool, float]:
    all_files, has_archives = collect_tender_input_files(
        local_path,
        tender_id,
        documents_root,
        archive_unpacker=archive_unpacker,
        logger=logger,
    )
    if all_files or max_wait_seconds <= 0:
        return all_files, has_archives, 0.0

    poll_interval_seconds = max(0.1, float(poll_interval_seconds))
    settle_seconds = max(0.0, float(settle_seconds))
    start_time = time.monotonic()
    stable_snapshot: Optional[tuple[str, ...]] = None
    stable_files: list[str] = []
    stable_has_archives = False
    stable_since: Optional[float] = None

    if logger:
        logger.info(
            "[GOODS_PACKET_WAIT] tender_id=%s wait_seconds=%.1f poll_interval_seconds=%.1f reason='no_files_detected_yet'",
            tender_id,
            max_wait_seconds,
            poll_interval_seconds,
        )

    while time.monotonic() - start_time < max_wait_seconds:
        time.sleep(poll_interval_seconds)
        all_files, has_archives = collect_tender_input_files(
            local_path,
            tender_id,
            documents_root,
            archive_unpacker=archive_unpacker,
            logger=logger,
        )
        if all_files:
            now = time.monotonic()
            snapshot = tuple(sorted(all_files))

            if all(_is_supporting_non_goods_file(path) for path in all_files):
                if logger:
                    logger.info(
                        "[GOODS_PACKET_WAIT_SUPPORT_FILES_ONLY] tender_id=%s files_detected=%s waiting_for_primary_goods_docs=true",
                        tender_id,
                        len(all_files),
                    )
                continue

            if stable_snapshot != snapshot:
                stable_snapshot = snapshot
                stable_files = list(all_files)
                stable_has_archives = has_archives
                stable_since = now
                if logger:
                    logger.info(
                        "[GOODS_PACKET_WAIT_FILES_DETECTED] tender_id=%s files_detected=%s settle_seconds=%.1f",
                        tender_id,
                        len(all_files),
                        settle_seconds,
                    )
                if settle_seconds <= 0:
                    waited_seconds = now - start_time
                    if logger:
                        logger.info(
                            "[GOODS_PACKET_WAIT_DONE] tender_id=%s waited_seconds=%.1f files_detected=%s",
                            tender_id,
                            waited_seconds,
                            len(all_files),
                        )
                    return all_files, has_archives, waited_seconds
                continue

            if stable_since is not None and (now - stable_since) >= settle_seconds:
                waited_seconds = now - start_time
                if logger:
                    logger.info(
                        "[GOODS_PACKET_WAIT_DONE] tender_id=%s waited_seconds=%.1f files_detected=%s stable_for_seconds=%.1f",
                        tender_id,
                        waited_seconds,
                        len(all_files),
                        now - stable_since,
                    )
                return all_files, has_archives, waited_seconds

    waited_seconds = time.monotonic() - start_time
    if stable_files:
        if logger:
            logger.warning(
                "[GOODS_PACKET_WAIT_TIMEOUT] tender_id=%s waited_seconds=%.1f files_detected=%s stable_for_seconds=%.1f",
                tender_id,
                waited_seconds,
                len(stable_files),
                0.0 if stable_since is None else max(0.0, time.monotonic() - stable_since),
            )
        return stable_files, stable_has_archives, waited_seconds
    if logger:
        logger.warning(
            "[GOODS_PACKET_WAIT_TIMEOUT] tender_id=%s waited_seconds=%.1f files_detected=0",
            tender_id,
            waited_seconds,
        )
    return all_files, has_archives, waited_seconds

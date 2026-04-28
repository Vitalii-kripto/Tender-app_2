import os
import shutil
import zipfile
import subprocess
from typing import List
from backend.logger import logger
from backend.services.download_retry_service import (
    clear_download_metadata,
    retry_downloaded_file_once,
)

class ArchiveService:
    SUPPORTED_EXTENSIONS = ('.zip', '.rar', '.7z')

    @classmethod
    def is_archive(cls, file_path: str) -> bool:
        return file_path.lower().endswith(cls.SUPPORTED_EXTENSIONS)

    @classmethod
    def _count_non_empty_files(cls, file_paths: List[str]) -> int:
        non_empty_files = 0
        for path in file_paths:
            try:
                if os.path.getsize(path) > 0:
                    non_empty_files += 1
            except OSError:
                continue
        return non_empty_files

    @classmethod
    def _remove_files_best_effort(cls, file_paths: List[str]) -> None:
        for path in file_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                continue

    @classmethod
    def unpack(cls, file_path: str, extract_dir: str, allow_download_retry: bool = True) -> List[str]:
        """
        Unpacks an archive into a specified directory.
        Returns a list of extracted file paths.
        """
        if not os.path.exists(file_path):
            logger.error(f"Archive not found: {file_path}")
            return []

        os.makedirs(extract_dir, exist_ok=True)
        extracted_files = []
        failure_reason = ""

        try:
            if os.path.getsize(file_path) <= 0:
                failure_reason = "archive_file_empty"
                logger.error("Archive file is empty | archive=%s", file_path)
        except OSError as exc:
            failure_reason = f"archive_stat_failed: {exc}"
            logger.error("Failed to read archive size | archive=%s | error=%s", file_path, exc)

        ext = os.path.splitext(file_path)[1].lower()
        if not failure_reason:
            if ext == '.zip':
                try:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                        extracted_files = [os.path.join(extract_dir, name) for name in zip_ref.namelist()]
                    logger.info(f"Successfully unpacked ZIP: {file_path}")
                except Exception as e:
                    failure_reason = str(e)
                    logger.error(f"Error unpacking ZIP {file_path}: {e}")
            elif ext in ('.rar', '.7z'):
                try:
                    if ext == '.7z':
                        import py7zr
                        with py7zr.SevenZipFile(file_path, mode='r') as z:
                            z.extractall(path=extract_dir)
                            extracted_files = [os.path.join(extract_dir, name) for name in z.getnames()]
                        logger.info(f"Successfully unpacked 7Z: {file_path}")
                    elif ext == '.rar':
                        import rarfile
                        with rarfile.RarFile(file_path) as r:
                            r.extractall(extract_dir)
                            extracted_files = [os.path.join(extract_dir, name) for name in r.namelist()]
                        logger.info(f"Successfully unpacked RAR: {file_path}")
                except ImportError as ie:
                    failure_reason = str(ie)
                    logger.warning(f"Missing library for {ext}: {ie}. Archive {file_path} cannot be extracted.")
                except Exception as e:
                    failure_reason = str(e)
                    logger.error(f"Error unpacking archive {file_path}: {e}")
            else:
                failure_reason = f"Unsupported archive extension: {ext}"

        final_files = []
        for root, _, files in os.walk(extract_dir):
            for f in files:
                final_files.append(os.path.join(root, f))

        if not failure_reason and final_files:
            non_empty_files = cls._count_non_empty_files(final_files)
            if non_empty_files == 0:
                failure_reason = "archive_extract_produced_only_empty_files"
                logger.error(
                    "Archive extracted only empty files | archive=%s | extracted_files=%s",
                    file_path,
                    len(final_files),
                )

        if failure_reason and allow_download_retry:
            retry_reason = failure_reason or "archive_empty_after_unpack"
            retry_result = retry_downloaded_file_once(
                file_path,
                reason=f"archive_unpack_failed: {retry_reason}",
                logger=logger,
            )
            if retry_result.get("succeeded"):
                shutil.rmtree(extract_dir, ignore_errors=True)
                os.makedirs(extract_dir, exist_ok=True)
                logger.warning(
                    "Archive unpack will be retried after redownload | archive=%s",
                    file_path,
                )
                return cls.unpack(file_path, extract_dir, allow_download_retry=False)
            if final_files:
                logger.warning(
                    "Archive unpack produced only a partial result and could not be recovered by redownload | archive=%s | extracted_files=%s",
                    file_path,
                    len(final_files),
                )

        if final_files and cls._count_non_empty_files(final_files) == 0:
            logger.warning(
                "Removing unusable empty extracted files after failed unpack | archive=%s | extracted_files=%s",
                file_path,
                len(final_files),
            )
            cls._remove_files_best_effort(final_files)
            return []

        return final_files

    @classmethod
    def unpack_directory(cls, dir_path: str) -> None:
        """
        Recursively finds all archives in the directory, unpacks them into subfolders,
        and deletes the original archives. Runs until no more archives are found (to handle nested).
        """
        if not os.path.isdir(dir_path):
            return
            
        unpacked_any_archive = True
        while unpacked_any_archive:
            unpacked_any_archive = False
            current_files = []
            for root, _, files in os.walk(dir_path):
                for f in files:
                    current_files.append(os.path.join(root, f))
            
            for file_path in current_files:
                if cls.is_archive(file_path):
                    unpack_dir = os.path.splitext(file_path)[0] + "_unpacked"
                    extracted_files = cls.unpack(file_path, unpack_dir)
                    if extracted_files:
                        unpacked_any_archive = True
                        try:
                            os.remove(file_path)
                            clear_download_metadata(file_path)
                        except Exception as e:
                            logger.error(f"Failed to remove extracted archive {file_path}: {e}")
                    else:
                        logger.warning(
                            "Archive left in place after failed unpack | archive=%s",
                            file_path,
                        )

archive_service = ArchiveService()

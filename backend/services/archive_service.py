import os
import shutil
import zipfile
import subprocess
from typing import List
from backend.logger import logger

class ArchiveService:
    SUPPORTED_EXTENSIONS = ('.zip', '.rar', '.7z')

    @classmethod
    def is_archive(cls, file_path: str) -> bool:
        return file_path.lower().endswith(cls.SUPPORTED_EXTENSIONS)

    @classmethod
    def unpack(cls, file_path: str, extract_dir: str) -> List[str]:
        """
        Unpacks an archive into a specified directory.
        Returns a list of extracted file paths.
        """
        if not os.path.exists(file_path):
            logger.error(f"Archive not found: {file_path}")
            return []

        os.makedirs(extract_dir, exist_ok=True)
        extracted_files = []

        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.zip':
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                    extracted_files = [os.path.join(extract_dir, name) for name in zip_ref.namelist()]
                logger.info(f"Successfully unpacked ZIP: {file_path}")
            except Exception as e:
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
                logger.warning(f"Missing library for {ext}: {ie}. Archive {file_path} cannot be extracted.")
            except Exception as e:
                logger.error(f"Error unpacking archive {file_path}: {e}")
        
        final_files = []
        for root, _, files in os.walk(extract_dir):
            for f in files:
                final_files.append(os.path.join(root, f))
        return final_files

    @classmethod
    def unpack_directory(cls, dir_path: str) -> None:
        """
        Recursively finds all archives in the directory, unpacks them into subfolders,
        and deletes the original archives. Runs until no more archives are found (to handle nested).
        """
        if not os.path.isdir(dir_path):
            return
            
        found_archives = True
        while found_archives:
            found_archives = False
            current_files = []
            for root, _, files in os.walk(dir_path):
                for f in files:
                    current_files.append(os.path.join(root, f))
            
            for file_path in current_files:
                if cls.is_archive(file_path):
                    found_archives = True
                    unpack_dir = os.path.splitext(file_path)[0] + "_unpacked"
                    cls.unpack(file_path, unpack_dir)
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.error(f"Failed to remove extracted archive {file_path}: {e}")

archive_service = ArchiveService()

import copy
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AnalogSearchJobService:
    def __init__(self, ttl_seconds: int = 7200):
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()

    def _get_public_payload_locked(self, job_id: str) -> Optional[dict[str, Any]]:
        payload = self._jobs.get(job_id)
        if not payload:
            return None
        public_payload = copy.deepcopy(payload)
        public_payload.pop("_updated_ts", None)
        public_payload.pop("_context", None)
        return public_payload

    def _cleanup_expired_locked(self, now_ts: float) -> None:
        expired_ids = [
            job_id
            for job_id, payload in self._jobs.items()
            if now_ts - float(payload.get("_updated_ts", now_ts)) > self._ttl_seconds
        ]
        for job_id in expired_ids:
            self._jobs.pop(job_id, None)

    def create_job(
        self,
        *,
        query: str,
        mode: str,
        result: Optional[dict[str, Any]] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> str:
        now_ts = time.time()
        now_iso = _utc_now_iso()
        job_id = str(uuid.uuid4())
        with self._lock:
            self._cleanup_expired_locked(now_ts)
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "stage": "Ожидает internet/AI-уточнения",
                "query": query,
                "mode": mode,
                "retry_count": 0,
                "next_retry_at": None,
                "created_at": now_iso,
                "updated_at": now_iso,
                "_updated_ts": now_ts,
                "_context": copy.deepcopy(context) if context is not None else {},
                "result": copy.deepcopy(result),
                "error": "",
            }
        return job_id

    def get_job_context(self, job_id: str) -> Optional[dict[str, Any]]:
        now_ts = time.time()
        with self._lock:
            self._cleanup_expired_locked(now_ts)
            payload = self._jobs.get(job_id)
            if not payload:
                return None
            return copy.deepcopy(payload.get("_context") or {})

    def update_job_context(self, job_id: str, context: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        now_ts = time.time()
        now_iso = _utc_now_iso()
        with self._lock:
            payload = self._jobs.get(job_id)
            if not payload:
                return None
            payload["_context"] = copy.deepcopy(context) if context is not None else {}
            payload["updated_at"] = now_iso
            payload["_updated_ts"] = now_ts
            return copy.deepcopy(payload["_context"])

    def start_job(self, job_id: str, stage: str = "Ищу аналоги в интернете") -> None:
        now_ts = time.time()
        now_iso = _utc_now_iso()
        with self._lock:
            payload = self._jobs.get(job_id)
            if not payload:
                return
            payload.update({
                "status": "running",
                "stage": stage,
                "next_retry_at": None,
                "updated_at": now_iso,
                "_updated_ts": now_ts,
                "error": "",
            })

    def schedule_retry(
        self,
        job_id: str,
        *,
        delay_seconds: int,
        reason: str,
        result: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        now_ts = time.time()
        now_iso = _utc_now_iso()
        delay_seconds = max(1, int(delay_seconds))
        retry_at_ts = now_ts + delay_seconds
        retry_at_iso = datetime.fromtimestamp(retry_at_ts, timezone.utc).isoformat()
        stage = f"ИИ-поиск временно недоступен, повтор через {delay_seconds} сек"
        with self._lock:
            payload = self._jobs.get(job_id)
            if not payload:
                return None
            payload.update({
                "status": "waiting_retry",
                "stage": stage,
                "retry_count": int(payload.get("retry_count") or 0) + 1,
                "next_retry_at": retry_at_iso,
                "updated_at": now_iso,
                "_updated_ts": now_ts,
                "error": str(reason or "").strip(),
            })
            if result is not None:
                payload["result"] = copy.deepcopy(result)
            return self._get_public_payload_locked(job_id)

    def complete_job(self, job_id: str, result: dict[str, Any]) -> None:
        now_ts = time.time()
        now_iso = _utc_now_iso()
        with self._lock:
            payload = self._jobs.get(job_id)
            if not payload:
                return
            payload.update({
                "status": "completed",
                "stage": "Готово",
                "next_retry_at": None,
                "updated_at": now_iso,
                "_updated_ts": now_ts,
                "result": copy.deepcopy(result),
                "error": "",
            })

    def fail_job(self, job_id: str, error: str) -> None:
        now_ts = time.time()
        now_iso = _utc_now_iso()
        with self._lock:
            payload = self._jobs.get(job_id)
            if not payload:
                return
            payload.update({
                "status": "error",
                "stage": "Ошибка фоновой дообработки",
                "next_retry_at": None,
                "updated_at": now_iso,
                "_updated_ts": now_ts,
                "error": str(error or "").strip(),
            })

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        now_ts = time.time()
        with self._lock:
            self._cleanup_expired_locked(now_ts)
            return self._get_public_payload_locked(job_id)

    def build_missing_job_payload(
        self,
        job_id: str,
        *,
        error: str = "",
    ) -> dict[str, Any]:
        now_iso = _utc_now_iso()
        message = str(error or "").strip() or (
            "Фоновая дообработка недоступна: задача не найдена. "
            "Вероятно, сервер был перезапущен; сохранен предварительный результат."
        )
        return {
            "job_id": job_id,
            "status": "error",
            "stage": "Фоновая дообработка недоступна",
            "query": "",
            "mode": "",
            "retry_count": 0,
            "next_retry_at": None,
            "created_at": now_iso,
            "updated_at": now_iso,
            "result": None,
            "error": message,
        }


analog_search_job_service = AnalogSearchJobService()

import uuid
from typing import Dict, Any

class JobService:
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}

    def create_job(self, tender_ids: list[str]) -> str:
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            "status": "running",
            "tenders": {
                tid: {
                    "stage": "Подготовка документов",
                    "progress": 0,
                    "status": "pending",
                    "file_statuses": []
                } for tid in tender_ids
            }
        }
        return job_id

    def get_job(self, job_id: str) -> Dict[str, Any]:
        return self.jobs.get(job_id)

    def update_tender_stage(self, job_id: str, tender_id: str, stage: str, progress: int, status: str = "running"):
        if job_id in self.jobs and tender_id in self.jobs[job_id]["tenders"]:
            self.jobs[job_id]["tenders"][tender_id]["stage"] = stage
            self.jobs[job_id]["tenders"][tender_id]["progress"] = progress
            self.jobs[job_id]["tenders"][tender_id]["status"] = status

    def complete_tender(self, job_id: str, tender_id: str, result: Dict[str, Any]):
        if job_id in self.jobs and tender_id in self.jobs[job_id]["tenders"]:
            self.jobs[job_id]["tenders"][tender_id].update(result)
            self.jobs[job_id]["tenders"][tender_id]["status"] = result.get("status", "success")
            self.jobs[job_id]["tenders"][tender_id]["stage"] = "Готово" if result.get("status") != "error" else "Ошибка"
            self.jobs[job_id]["tenders"][tender_id]["progress"] = 100

    def check_job_completion(self, job_id: str):
        if job_id in self.jobs:
            all_done = all(
                t["status"] in ["success", "error", "partial"] 
                for t in self.jobs[job_id]["tenders"].values()
            )
            if all_done:
                self.jobs[job_id]["status"] = "completed"

job_service = JobService()

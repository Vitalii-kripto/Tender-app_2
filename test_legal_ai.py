import asyncio
import os
import json
from backend.services.legal_analysis_service import LegalAnalysisService

async def test_legal_ai():
    service = LegalAnalysisService()
    
    # Test 1: Missing application composition (should be found)
    print("--- Test 1: Application Composition ---")
    files_1 = [
        {
            "filename": "Извещение.docx",
            "content": "Состав заявки: 1. Анкета участника. 2. Выписка из ЕГРЮЛ. 3. Декларация о соответствии."
        },
        {
            "filename": "Проект контракта.docx",
            "content": "Срок поставки: 30 дней с даты заключения контракта."
        }
    ]
    
    res_1 = service.analyze_full_package(files_1, tender_id="test_1")
    print("Test 1 completed. Check logs.")
    
    # Test 2: Contradiction in delivery deadline
    print("\n--- Test 2: Contradiction in Delivery Deadline ---")
    files_2 = [
        {
            "filename": "Извещение.docx",
            "content": "Срок поставки: 15 дней с даты заключения контракта."
        },
        {
            "filename": "Проект контракта.docx",
            "content": "Срок поставки: 30 дней с даты заключения контракта."
        }
    ]
    
    res_2 = service.analyze_full_package(files_2, tender_id="test_2")
    print("Test 2 completed. Check logs.")

if __name__ == "__main__":
    asyncio.run(test_legal_ai())

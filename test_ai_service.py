import sys
import os

# Add the current directory to sys.path to allow imports
sys.path.append(os.getcwd())

try:
    from backend.services.ai_service import AiService
    service = AiService()
    print(f"AiService initialized. Methods: {dir(service)}")
    if hasattr(service, 'test_model_availability'):
        print("SUCCESS: test_model_availability found.")
    else:
        print("ERROR: test_model_availability NOT found.")
except Exception as e:
    print(f"CRITICAL ERROR: {e}")

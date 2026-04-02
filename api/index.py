import sys
import os

# Redirect stdout and stderr to a file for debugging
log_file_path = os.path.join(os.getcwd(), "api_index_stdout_stderr.log")
sys.stdout = open(log_file_path, "w", encoding="utf-8", buffering=1)
sys.stderr = sys.stdout

print("🚀 api/index.py is starting!")

try:
    from backend.main import app
    print("✅ backend.main imported successfully in api/index.py")
except Exception as e:
    print(f"❌ backend.main import failed in api/index.py: {e}")
    import traceback
    traceback.print_exc()

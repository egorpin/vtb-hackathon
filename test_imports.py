import sys
import os
print("Python path:", sys.path)
print("Current directory:", os.getcwd())
print("Files in directory:", os.listdir('.'))

try:
    from config import DB_CONFIG
    print("✓ config.py imported successfully")
except ImportError as e:
    print("✗ config.py import failed:", e)

try:
    from metrics import MetricsCollector
    print("✓ metrics.py imported successfully")
except ImportError as e:
    print("✗ metrics.py import failed:", e)

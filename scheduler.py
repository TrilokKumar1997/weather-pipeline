import schedule
import time
from etl_pipeline import run_pipeline

print("Weather pipeline scheduler started...")
print("Running pipeline every hour...")

run_pipeline()

schedule.every(1).hours.do(run_pipeline)

while True:
    schedule.run_pending()
    time.sleep(60)

import datetime
import logging
import azure.functions as func

from shared.watermark import get_watermark_utc
from shared.capacity import resume_capacity, pause_capacity
from shared.pipeline import run_pipeline, wait_for_pipeline_success

app = func.FunctionApp()

@app.schedule(
    schedule="0 0 6,18 * * *",  # 06:00 and 18:00 UTC
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True
)
def capacity_scheduler(mytimer: func.TimerRequest) -> None:
    utc_now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    logging.info(f"Timer trigger at {utc_now.isoformat()}")

    watermark = get_watermark_utc()
    logging.info(f"Current watermark: {watermark.isoformat()}")

    if utc_now <= watermark:
        logging.info("Not yet time to run. Exiting.")
        return

    try:
        logging.info("Resuming capacity")
        resume_capacity()
        # If you implement wait_for_capacity_active(), call it here

        logging.info("Running pipeline")
        run_id = run_pipeline()
        status = wait_for_pipeline_success(run_id)
        logging.info(f"Pipeline run completed with status: {status}")

    except Exception as ex:
        logging.error(f"Error during orchestration: {ex}")
        # Rethrow so the function run is marked as failed
        raise

    finally:
        try:
            logging.info("Pausing capacity")
            pause_capacity()
        except Exception as ex:
            logging.error(f"Failed to pause capacity: {ex}")

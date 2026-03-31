import time

from prefect import flow, get_run_logger, task

from export_tools import get_run


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    stream_data = run[stream].read()
    return stream_data


@flow
def data_validation(uid, api_key=None):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)
    logger.info(f"Validating uid {uid}")
    start_time = time.monotonic()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = time.monotonic()
        stream_data = read_stream(run, stream)
        stream_elapsed_time = time.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = time.monotonic() - start_time
    logger.info(f"{elapsed_time = }")

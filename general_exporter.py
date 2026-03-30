# from export_tools import get_proposal_path
from prefect import flow, get_run_logger

from export_tools import get_run
from file_exporter import (
    export_generic_1D,
    export_peak_xps,
    export_resPES,
    export_ses_xps,
    export_xas,
)


def export_switchboard(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)
    if run.stop["exit_status"] != "abort":
        if run.start["autoexport"]:
            if "scantype" in run.start.keys():
                if run.start["scantype"] == "xps":
                    if run.start["analyzer_type"] == "peak":
                        peak_export(uid, api_key=api_key, dry_run=dry_run)
                    elif run.start["analyzer_type"] == "ses":
                        ses_export(uid, api_key=api_key, dry_run=dry_run)
                elif run.start["scantype"] == "xas":
                    xas_export(uid, api_key=api_key, dry_run=dry_run)
                elif run.start["scantype"] == "resPES":
                    resPES_export(uid, api_key=api_key, dry_run=dry_run)
                else:
                    generic_export(uid, api_key=api_key, dry_run=dry_run)
            else:
                generic_export(uid, api_key=api_key, dry_run=dry_run)
    else:
        logger.info("Run was aborted, skipping exports")


@flow
def xas_export(uid, api_key=None, dry_run=False):
    export_xas(uid, api_key=api_key, dry_run=dry_run)


@flow
def peak_export(uid, api_key=None, dry_run=False):
    export_peak_xps(uid, api_key=api_key, dry_run=dry_run)


@flow
def generic_export(uid, api_key=None, dry_run=False):
    export_generic_1D(uid, api_key=api_key, dry_run=dry_run)


@flow
def ses_export(uid, api_key=None, dry_run=False):
    export_ses_xps(uid, api_key=api_key, dry_run=dry_run)


@flow
def resPES_export(uid, api_key=None, dry_run=False):
    export_resPES(uid, api_key=api_key, dry_run=dry_run)

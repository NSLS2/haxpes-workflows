# from export_tools import get_proposal_path
from prefect import flow

from export_tools import initialize_tiled_client
from file_exporter import export_generic_1D, export_peak_xps, export_ses_xps, export_xas


def export_switchboard(uid, beamline_acronym="haxpes"):
    c = initialize_tiled_client(beamline_acronym)
    run = c[uid]
    if "scantype" in run.start.keys():
        if run.stop["exit_status"] != "abort":
            if run.start["autoexport"]:
                if run.start["scantype"] == "xps":
                    if run.start["analyzer_type"] == "peak":
                        peak_export(uid)
                    elif run.start["analyzer_type"] == "ses":
                        ses_export(uid)
                elif run.start["scantype"] == "xas":
                    xas_export(uid)
    else:
        if run.start["autoexport"]:
            generic_export(uid)


@flow
def xas_export(uid, beamline_acronym="haxpes"):
    export_xas(uid, beamline_acronym)


@flow
def peak_export(uid, beamline_acronym="haxpes"):
    export_peak_xps(uid, beamline_acronym)


@flow
def generic_export(uid, beamline_acronym="haxpes"):
    export_generic_1D(uid, beamline_acronym)


@flow
def ses_export(uid, beamline_acronym="haxpes"):
    export_ses_xps(uid, beamline_acronym)

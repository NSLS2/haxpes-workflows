import shutil
from glob import glob
from os import makedirs
from os.path import exists, splitext

import numpy as np

from export_tools import *


def export_peak_xps(uid, beamline_acronym="haxpes"):
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]

    metadata = get_metadata_xps(run)
    header = make_header(metadata, "xps")
    data = get_data_xps(run)
    export_path = get_proposal_path(run) + "XPS_export/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "csv")
    np.savetxt(filename, data, delimiter=",", header=header)


def export_ses_xps(uid, beamline_acronym="haxpes"):
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]

    metadata = get_metadata_xps(run)
    header = make_header(metadata, "xps")
    export_path = get_proposal_path(run) + "XPS_export/"
    ses_path = get_ses_path(run)
    scan_id = run.start["scan_id"]
    if not exists(export_path):
        makedirs(export_path)
    filename = generate_file_name(run, "md")
    out_path = export_path + filename
    write_header_only(out_path, header)
    ses_files = glob(f"{ses_path}*_{scan_id}_*")
    for ses_file in ses_files:
        ext = splitext(ses_file)[1]
        out_path = export_path + generate_file_name(run, ext)
        shutil.copy(ses_file, out_path)


def export_xas(uid, beamline_acronym="haxpes"):
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]

    detlist = run.start["detectors"]
    metadata = get_general_metadata(run)
    header = make_header(metadata, "xas", detlist=detlist)
    data = get_xas_data(run)

    export_path = get_proposal_path(run) + "XAS_export/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "csv")
    np.savetxt(filename, data, delimiter=",", header=header)


def export_generic_1D(uid, beamline_acronym="haxpes"):
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]

    detlist = run.start["detectors"]
    metadata = get_general_metadata(run)
    header = make_header(metadata, "generic", detlist=detlist)
    data = get_generic_1d_data(run)

    export_path = get_proposal_path(run) + "GeneralExport/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "csv")
    np.savetxt(filename, data, delimiter=",", header=header)

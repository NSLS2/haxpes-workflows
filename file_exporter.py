import shutil
from glob import glob
from os import makedirs
from os.path import exists, splitext

import h5py
import numpy as np
from prefect import get_run_logger, task

from export_tools import *


@task
def export_peak_xps(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)

    metadata = get_metadata_xps(run)
    header = make_header(metadata, "xps")
    data = get_data_xps(run)
    export_path = get_proposal_path(run) + "XPS_export/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "csv")
    if dry_run:
        logger.info(f"Dry run: not exporting peak XPS data to {filename}")
    else:
        np.savetxt(filename, data, delimiter=",", header=header)


@task
def export_ses_xps(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)

    metadata = get_metadata_xps(run)
    header = make_header(metadata, "xps")
    export_path = get_proposal_path(run) + "XPS_export/"
    ses_path = get_ses_path(run)
    scan_id = run.start["scan_id"]
    if not exists(export_path):
        makedirs(export_path)
    filename = generate_file_name(run, "md")
    out_path = export_path + filename
    if dry_run:
        logger.info(f"Dry run: not exporting SES XPS data to {export_path}")
    else:
        write_header_only(out_path, header)
        ses_files = glob(f"{ses_path}*_{scan_id}_*")
        for ses_file in ses_files:
            ext = splitext(ses_file)[1]
            out_path = export_path + generate_file_name(run, ext)
            shutil.copy(ses_file, out_path)


@task
def export_xas(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)

    detlist = run.start["detectors"]
    metadata = get_general_metadata(run)
    header = make_header(metadata, "xas", detlist=detlist)
    data = get_xas_data(run)

    export_path = get_proposal_path(run) + "XAS_export/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "csv")
    if dry_run:
        logger.info(f"Dry run: not exporting XAS data to {filename}")
    else:
        np.savetxt(filename, data, delimiter=",", header=header)


@task
def export_generic_1D(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)

    detlist = run.start["detectors"]
    metadata = get_general_metadata(run)
    header = make_header(metadata, "generic", detlist=detlist)
    data = get_generic_1d_data(run)

    export_path = get_proposal_path(run) + "GeneralExport/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "csv")
    if dry_run:
        logger.info(f"Dry run: not exporting generic 1D data to {filename}")
    else:
        np.savetxt(filename, data, delimiter=",", header=header)


def export_resPES(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run = get_run(uid, api_key=api_key)

    data_dictionary = get_resPES_data(run)

    export_path = get_proposal_path(run) + "ResPES/"
    if not exists(export_path):
        makedirs(export_path)
    filename = export_path + generate_file_name(run, "h5")
    if dry_run:
        logger.info(f"Dry run: not exporting ResPES data to {filename}")
    else:
        with h5py.File(filename, "a") as f:

            datagroup = f.create_group("DataSets")
            for key, value in data_dictionary["DataSets"].items():
                ds = datagroup.create_dataset(key, data=value)

            specgroup = f.create_group("Signals")
            for key, value in data_dictionary["Signals"].items():
                s = specgroup.create_dataset(key, data=value)
                s.attrs["X Axis"] = "Photon Energy"

            axisgroup = f.create_group("PlotAxes")
            for key, value in data_dictionary["Axes"].items():
                a = axisgroup.create_dataset(key, data=value)

            metagroup = f.create_group("Meta")
            for key, value in data_dictionary["Metadata"].items():
                metagroup.attrs[key] = value

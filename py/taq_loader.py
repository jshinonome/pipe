import argparse
import logging
import subprocess
from multiprocessing import Pool, current_process
from pathlib import Path

FORMAT = "%(asctime)s %(levelname)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt=DATE_FORMAT)

logger = logging.getLogger()


parser = argparse.ArgumentParser(description="TAQ Loader CLI")
parser.add_argument(
    "-t",
    "--thread",
    type=int,
    help="thread number",
    default=5,
)

parser.add_argument(
    "-d",
    "--dir",
    type=str,
    help="directory that stores taq gz files",
    required=True,
)

parser.add_argument(
    "--hdb",
    type=str,
    help="q hdb directory",
    required=True,
)

args = parser.parse_args()

if not Path(args.dir).exists:
    logger.error("no such gz directory '{}'".format(args.source))

if not Path(args.hdb).exists:
    logger.error("no such hdb directory '{}'".format(args.source))

# use ~/.cache/taq-loader/loaded
CACHE_DIR = Path.joinpath(Path.home(), ".cache", "pipe")

if not CACHE_DIR.exists():
    CACHE_DIR.mkdir(parents=True)

CACHE_FILE = Path.joinpath(CACHE_DIR, "loaded")


def main():
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            loaded_files = set(f.read().splitlines())
    else:
        loaded_files = set([])

    gz_paths = set([str(p) for p in Path(args.dir).glob("*EQY_US_ALL_*.gz")])
    gz_paths = gz_paths.difference(loaded_files)

    if len(gz_paths) == 0:
        logger.info("unprocessed gz file not found, exit ...")
        exit(0)

    gz_files = sorted(gz_paths)

    with Pool(processes=args.thread) as pool:
        processed_gz_files = pool.map(load, gz_files)

    loaded_files.update(processed_gz_files)
    with open(CACHE_FILE, "w") as f:
        for line in sorted(loaded_files):
            f.write(line + "\n")


def load(gz_file: str):
    logger.info("loading " + gz_file)
    process = current_process()
    date = str(gz_file).split("_")[-1].split(".")[0]
    hdb = args.hdb
    cmd = "ktrl --start --process pipe --profile q4 --kargs "
    cmd += "' -gzPath :{} -hdbPath :{} -partition {} -delimiter \"|\" -overwrite 1b -dropStart 1 -dropEnd 1'".format(
        gz_file, hdb, date
    )
    log_file = Path("/tmp/{}-{}.log".format(process.name, date))
    log_file_handle = open(log_file, "a")
    completeProcess = subprocess.run(
        cmd, shell=True, capture_output=False, stdout=log_file_handle, stderr=log_file_handle
    )
    log_file_handle.close()
    if completeProcess.returncode != 0:
        logger.error("failed to process {}".format(gz_file))
        return ""
    else:
        logger.info("loaded " + gz_file)
        return gz_file


if __name__ == "__main__":
    main()

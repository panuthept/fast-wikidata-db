import os
import boto3
import wget
import subprocess
from tqdm import tqdm
from fast_wikidata_db.constants.const import S3_BUCKET, S3_KEYS, DATA_URLS


# NOTE: Code from https://www.scrapingbee.com/blog/python-wget
def runcmd(cmd, verbose = False, *args, **kwargs):
    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    pass


def tqdm_hook(tqdm_progress_bar: tqdm):
    def inner(bytes_amount: int):
        tqdm_progress_bar.update(bytes_amount)
    return inner


def tqdm_wget_hook(tqdm_progress_bar: tqdm):
    def inner(current, total, width=80):
        tqdm_progress_bar.total = total
        tqdm_progress_bar.refresh()
        tqdm_progress_bar.update(current - tqdm_progress_bar.n)
    return inner


def db_download(db_dir, s3_key):
    s3 = boto3.resource("s3")
    if not os.path.exists(f"{db_dir}/{s3_key}"):
        try:
            # Faster than wget but requires aws credentials
            s3_obj = s3.Object(S3_BUCKET, s3_key)
            with tqdm(
                total=s3_obj.content_length,
                unit="B",
                unit_scale=True,
                desc=f"Downloading {s3_key}",
            ) as t:
                s3_obj.download_file(f"{db_dir}/{s3_key}", Callback=tqdm_hook(t))
        except:
            # Works without aws credentials but slower than boto3
            with tqdm(
                unit="B",
                unit_scale=True,
                desc=f"Downloading {s3_key}",
            ) as t:
                wget.download(DATA_URLS[s3_key], out=f"{db_dir}/{s3_key}", bar=tqdm_wget_hook(t))
import boto3
from tqdm import tqdm
from fast_wikidata_db.constants.const import S3_BUCKET, S3_KEYS


def tqdm_hook(tqdm_progress_bar: tqdm):
    def inner(bytes_amount: int):
        tqdm_progress_bar.update(bytes_amount)

    return inner


def db_download(db_dir):
    s3 = boto3.resource("s3")
    for s3_key in S3_KEYS:
        s3_obj = s3.Object(S3_BUCKET, s3_key)
        with tqdm(
            total=s3_obj.content_length,
            unit="B",
            unit_scale=True,
            desc=f"Downloading {s3_key}",
        ) as t:
            s3_obj.download_file(f"{db_dir}/{s3_key}", Callback=tqdm_hook(t))
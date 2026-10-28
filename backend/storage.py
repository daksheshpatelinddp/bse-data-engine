"""
Where the data lives.  R2Store talks to Cloudflare R2 (S3 API); LocalStore uses a folder (for tests / dry runs).

Every key is relative to the bucket, e.g. "bse/raw/2026_H1.parquet".
Chosen automatically: R2 when R2_ENDPOINT_URL is set, otherwise a local folder (LOCAL_R2_DIR, default ./local_r2).
"""
import os
import shutil

BUCKET_NAME = os.getenv("R2_BUCKET", "nse-stock-data")     # your existing bucket (the NSE engine uses the same one)


class LocalStore:
    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _p(self, key):
        return os.path.join(self.root, *key.split("/"))

    def get(self, key):
        try:
            with open(self._p(key), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def put(self, key, body, content_type=None, cache=None):
        p = self._p(key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(body)

    def upload_file(self, path, key):
        p = self._p(key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        shutil.copyfile(path, p)

    def download_file(self, key, path):
        if not os.path.exists(self._p(key)):
            return False
        shutil.copyfile(self._p(key), path)
        return True

    def copy(self, src, dst):
        p = self._p(dst)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        shutil.copyfile(self._p(src), p)

    def list(self, prefix):
        """[(key, size, etag)] of every object under prefix."""
        out = []
        base = self._p(prefix.rstrip("/")) if prefix else self.root
        for d, _, files in os.walk(base):
            for f in files:
                full = os.path.join(d, f)
                key = os.path.relpath(full, self.root).replace(os.sep, "/")
                if key.startswith(prefix):
                    st = os.stat(full)
                    out.append((key, st.st_size, f"{st.st_size}-{int(st.st_mtime)}"))
        return sorted(out)

    def delete(self, keys):
        for k in keys:
            try:
                os.remove(self._p(k))
            except FileNotFoundError:
                pass


class R2Store:
    def __init__(self):
        import boto3
        self.c = boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT_URL"],
                              aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                              aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto")

    def get(self, key):
        try:
            return self.c.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()
        except self.c.exceptions.NoSuchKey:
            return None

    def put(self, key, body, content_type=None, cache=None):
        kw = {}
        if content_type:
            kw["ContentType"] = content_type
        if cache:
            kw["CacheControl"] = cache
        self.c.put_object(Bucket=BUCKET_NAME, Key=key, Body=body, **kw)

    def upload_file(self, path, key):
        """ONE put_object call = ONE Class A operation (boto3's upload_file would split files over 8 MB into many parts, each one an operation)."""
        with open(path, "rb") as f:
            self.c.put_object(Bucket=BUCKET_NAME, Key=key, Body=f)

    def download_file(self, key, path):
        try:
            self.c.download_file(BUCKET_NAME, key, path)
            return True
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e) or "NoSuchKey" in str(e):
                return False
            raise

    def copy(self, src, dst):
        self.c.copy_object(Bucket=BUCKET_NAME, Key=dst, CopySource={"Bucket": BUCKET_NAME, "Key": src})

    def list(self, prefix):
        out, token = [], None
        while True:
            kw = {"Bucket": BUCKET_NAME, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            page = self.c.list_objects_v2(**kw)
            out += [(o["Key"], o["Size"], o["ETag"].strip('"')) for o in page.get("Contents", [])]
            if not page.get("IsTruncated"):
                return sorted(out)
            token = page.get("NextContinuationToken")

    def delete(self, keys):
        keys = list(keys)
        for i in range(0, len(keys), 500):
            self.c.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": [{"Key": k} for k in keys[i:i + 500]]})


def get_store():
    if os.getenv("R2_ENDPOINT_URL"):
        return R2Store()
    return LocalStore(os.getenv("LOCAL_R2_DIR", "local_r2"))

"""S3-compatible object storage client from config (MinIO locally, GCS interoperability in cloud, ADR-0002)."""


def s3_client(cfg: dict):
    import boto3
    from botocore.config import Config

    st = cfg["storage"]
    return boto3.client(
        "s3",
        endpoint_url=st["endpoint"],
        region_name=st.get("region") or None,
        aws_access_key_id=st.get("access_key") or None,
        aws_secret_access_key=st.get("secret_key") or None,
        config=Config(
            s3={"addressing_style": "path" if st.get("path_style", True) else "virtual"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )

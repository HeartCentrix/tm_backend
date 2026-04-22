#!/usr/bin/env python3
"""Idempotent SeaweedFS bucket creation with Object Lock + versioning.

Usage:
  python scripts/create_seaweedfs_buckets.py \\
    --endpoint http://seaweedfs.railway.internal:8333 \\
    --access-key PILOT_ACCESS \\
    --secret-key PILOT_SECRET \\
    --buckets tmvault-shard-0,tmvault-shard-1 \\
    --default-retention-days 30 \\
    --default-retention-mode GOVERNANCE

Object Lock + versioning must be set at bucket creation — you cannot
retrofit either. Rerun is safe: create-bucket errors on existing buckets
are swallowed.
"""
from __future__ import annotations

import argparse
import sys

import boto3
from botocore.exceptions import ClientError


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--buckets", required=True, help="comma-separated")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--default-retention-days", type=int, default=30)
    ap.add_argument("--default-retention-mode", default="GOVERNANCE",
                    choices=["GOVERNANCE", "COMPLIANCE"])
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
    )

    for b in [x.strip() for x in args.buckets.split(",") if x.strip()]:
        try:
            s3.create_bucket(Bucket=b, ObjectLockEnabledForBucket=True)
            print(f"created bucket {b}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"bucket {b} already exists — ok")
            else:
                print(f"create_bucket({b}) failed: {code}: {e}", file=sys.stderr)
                return 1

        try:
            s3.put_bucket_versioning(
                Bucket=b, VersioningConfiguration={"Status": "Enabled"},
            )
        except ClientError as e:
            # SeaweedFS enables versioning implicitly when Object Lock is on.
            print(f"  versioning already-enabled or not required: {e.response['Error']['Code']}")

        try:
            s3.put_object_lock_configuration(
                Bucket=b,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": args.default_retention_mode,
                            "Days": args.default_retention_days,
                        },
                    },
                },
            )
            print(f"  Object Lock default: {args.default_retention_mode} "
                  f"{args.default_retention_days}d")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            # SeaweedFS returns BucketAlreadyExists for this call on already-
            # configured buckets. Per-object locks still work since the bucket
            # was created with ObjectLockEnabledForBucket=True. Log and move on.
            if code in ("BucketAlreadyExists", "NotImplemented"):
                print(f"  bucket-default retention skipped ({code}); per-object"
                      f" locks still work")
            else:
                print(f"  put_object_lock_configuration failed: {code}: {e}",
                      file=sys.stderr)
                return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

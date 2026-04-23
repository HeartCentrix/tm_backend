# SeaweedFS for Railway — thin wrapper over the official image that
# bakes our S3 admin identity. Data persists via a Railway volume
# mounted at /data (attached per-service via the Railway dashboard
# or `railway volume` CLI — not declared here, since Docker VOLUME
# would shadow the attached mount on Railway).
#
# Internal URL once deployed:
#   http://seaweedfs.railway.internal:8333   (S3 API)
#   http://seaweedfs.railway.internal:9333   (master, not needed by clients)
#
# Clients (backup-worker, restore-worker, snapshot-service, …) reach
# it via the ONPREM_S3_ENDPOINT / ONPREM_S3_ACCESS_KEY / ONPREM_S3_SECRET_KEY
# env vars — which must match the credentials baked into s3.json below.
FROM chrislusf/seaweedfs:3.71

COPY deploy/railway/seaweedfs/s3.json /config/s3.json

EXPOSE 8333 9333

CMD ["server", \
     "-dir=/data", \
     "-ip=0.0.0.0", \
     "-s3", \
     "-s3.config=/config/s3.json", \
     "-filer", \
     "-master.volumeSizeLimitMB=256", \
     "-volume.max=0", \
     "-volume.preStopSeconds=1", \
     "-master.volumePreallocate=false"]

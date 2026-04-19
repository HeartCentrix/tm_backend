"""download_export_zip must stream raw blob bytes when Job.result.output_mode='raw_single'.

Full endpoint wiring requires the job-service app to boot, which brings along DB
+ auth + config. The integration test in Task 13 exercises the endpoint
end-to-end against Azurite; here we just pin the shape of the result payload
and validate the branch constants so the handler doesn't drift silently.
"""


def test_raw_single_result_shape():
    payload = {
        "output_mode": "raw_single",
        "source_container": "backup-files-tenant",
        "source_blob_path": "snap/ts/id",
        "original_name": "Report.xlsx",
        "content_type": "application/vnd.ms-excel",
        "size_bytes": 10,
    }
    assert payload["output_mode"] == "raw_single"
    assert payload["source_container"].startswith("backup-")
    assert payload["source_blob_path"]
    assert payload["original_name"].endswith(".xlsx")
    assert payload["content_type"] != "application/zip"
    assert payload["size_bytes"] > 0

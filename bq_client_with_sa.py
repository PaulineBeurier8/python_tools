from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "your_service_account.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

sql = """SELECT * FROM `project.dataset.table` LIMIT 1000"""

print(f"sql used to query:{sql}")
query_job = client.query(sql)


results = query_job.result()

print(results)

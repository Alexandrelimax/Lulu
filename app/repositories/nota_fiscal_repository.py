import io
from google.cloud import storage, bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat

class NotaFiscalRepository:
    def __init__(self, bucket_name: str, project_id: str, table_id: str):
        self.bucket = storage.Client().bucket(bucket_name)
        self.bigquery_client = bigquery.Client(project=project_id)
        self.table_id = table_id

    # Cria um blob no bucket para armazenar o arquivo Parquet
    def save_parquet_to_gcs(self, parquet_buffer: io.BytesIO, gcs_path: str) -> None:
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
        print(f"Uploaded {gcs_path} to gs://{self.bucket.name}/{gcs_path}")

    # Configurando o job de carregamento para ler um arquivo Parquet
    def load_parquet_to_bigquery(self, gcs_parquet_path: str) -> None:
        job_config = LoadJobConfig(source_format=SourceFormat.PARQUET)
        uri = f"gs://{self.bucket.name}/{gcs_parquet_path}"
        load_job = self.bigquery_client.load_table_from_uri(uri, self.table_id, job_config=job_config)
        load_job.result()  # Aguardar a conclusão do job
        print(f"Loaded {gcs_parquet_path} into {self.table_id} in BigQuery")

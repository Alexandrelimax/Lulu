import io
from datetime import datetime
from typing import List, IO

import pandas as pd

from app.models.nota_fiscal_model import NotaFiscal
from app.repositories.nota_fiscal_repository import NotaFiscalRepository


class NotaFiscalService:
    def __init__(self, repository: NotaFiscalRepository):
        self.repository = repository

    # Converte a lista de NotaFiscais em DataFrame
    def process_nota_fiscal(self, notas_fiscais: List[NotaFiscal]) -> None:
        try:
            df = pd.DataFrame([nota.model_dump() for nota in notas_fiscais])

            df['created_at'] = datetime.now()

            # Convert DataFrame em parquet direto na memória
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)  # Reset buffer position

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gcs_parquet_path = f"notas_fiscais/nota_fiscal_{timestamp}.parquet"

            self.repository.save_parquet_to_gcs(parquet_buffer, gcs_parquet_path)
            self.repository.load_parquet_to_bigquery(gcs_parquet_path)

        except Exception as e:
            raise RuntimeError(f"Error processing Nota Fiscal from JSON to Parquet: {str(e)}")

    def process_csv(self, file: IO) -> None:
        try:
            # Converte o CSV em DataFrame
            df = pd.read_csv(file, delimiter='|')

            df['created_at'] = datetime.now()

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)  # Reset buffer position

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gcs_parquet_path = f"notas_fiscais/nota_fiscal_{timestamp}.parquet"

            self.repository.save_parquet_to_gcs(parquet_buffer, gcs_parquet_path)
            self.repository.load_parquet_to_bigquery(gcs_parquet_path)

        except Exception as e:
            raise RuntimeError(f"Error processing CSV to Parquet: {str(e)}")

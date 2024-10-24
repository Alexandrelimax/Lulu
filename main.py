import os
from fastapi import FastAPI
from google.cloud import bigquery, storage
from app.controllers.nota_fiscal_controller import NotaFiscalController
from app.services.nota_fiscal_service import NotaFiscalService
from app.repositories.nota_fiscal_repository import NotaFiscalRepository


bucket_name = "bucket_teste_alexandre"
project_id = ""
table_id = "dataset_teste.nota_fiscal_bronze"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mamae.json'


app = FastAPI()

# Instância do repository
nota_fiscal_repository = NotaFiscalRepository(bucket_name=bucket_name, project_id=project_id, table_id=table_id)

# Instância do service
nota_fiscal_service = NotaFiscalService(repository=nota_fiscal_repository)

# Instância do controller
nota_fiscal_controller = NotaFiscalController(service=nota_fiscal_service)

# Incluindo as rotas do controlador
app.include_router(nota_fiscal_controller.router)

@app.get("/")
def read_root():
    return {"message": "API is running successfully!"}


if __name__ == "__main__":
    import uvicorn
    print("Running at http://localhost:8000")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
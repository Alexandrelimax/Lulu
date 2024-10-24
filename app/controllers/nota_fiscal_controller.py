from typing import List

from fastapi import HTTPException, status, UploadFile, File
from fastapi.responses import JSONResponse

from app.models.nota_fiscal_model import NotaFiscal
from app.services.nota_fiscal_service import NotaFiscalService
from app.controllers.base_controller import BaseController


class NotaFiscalController(BaseController):
    def __init__(self, service: NotaFiscalService):
        self.service = service
        super().__init__(prefix="/nota-fiscal")

    def register_routes(self):
        self.router.add_api_route("/", self.process_nota_fiscal, methods=["POST"])
        self.router.add_api_route("/upload-csv", self.upload_csv, methods=["POST"])


    def process_nota_fiscal(self, notas_fiscais: List[NotaFiscal]) -> JSONResponse:
        try:
            self.service.process_nota_fiscal(notas_fiscais)
            return JSONResponse(content={"message": "Nota Fiscals processed successfully"}, status_code=status.HTTP_201_CREATED)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


    def upload_csv(self, file: UploadFile = File(...)) -> JSONResponse:
        try:
            self.service.process_csv(file.file)
            return JSONResponse(content={"message": "CSV processed successfully"}, status_code=status.HTTP_201_CREATED)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error processing CSV: {str(e)}")

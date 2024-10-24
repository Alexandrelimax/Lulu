from pydantic import BaseModel

class NotaFiscal(BaseModel):
    cod_ctiss: str
    desc_ctiss: str
    sub_item: float
    aliquota: float

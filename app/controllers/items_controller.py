from typing import List
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse, Response
from app.models.item_model import Item
from app.services.item_service import ItemService
from app.controllers.base_controller import BaseController

class ItemsController(BaseController):
    def __init__(self, service: ItemService):
        self.service = service
        super().__init__(prefix="/items")

    def register_routes(self):
        self.router.add_api_route("/", self.get_items, methods=["GET"], response_model=List[Item])
        self.router.add_api_route("/", self.create_item, methods=["POST"], response_model=Item)
        self.router.add_api_route("/{item_id}", self.get_item, methods=["GET"], response_model=Item)
        self.router.add_api_route("/{item_id}", self.update_item, methods=["PUT"], response_model=Item)
        self.router.add_api_route("/{item_id}", self.delete_item, methods=["DELETE"], response_model=Item)

    def get_items(self) -> List[Item]:
        return self.service.get_items()

    def create_item(self, item: Item) -> Item:
        created_item = self.service.create_item(item)
        return JSONResponse(content=created_item.model_dump(), status_code=status.HTTP_201_CREATED)

    def get_item(self, item_id: int) -> Item:
        item = self.service.get_item_by_id(item_id)
        if not item:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item não encontrado")
        return item

    def update_item(self, item_id: int, updated_item: Item) -> Item:
        item = self.service.update_item(item_id, updated_item)
        if not item:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item não encontrado")
        return item

    def delete_item(self, item_id: int) -> Response:
        success = self.service.delete_item(item_id)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item não encontrado")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

from typing import List, Optional
from app.models.item_model import Item
from app.repositories.item_repository import ItemRepository

class ItemService:
    def __init__(self, repository: ItemRepository):
        self.repository = repository

    def get_items(self) -> List[Item]:
        return self.repository.get_all()

    def get_item_by_id(self, item_id: int) -> Optional[Item]:
        return self.repository.get_by_id(item_id)

    def create_item(self, item: Item) -> Item:
        return self.repository.save(item)

    def update_item(self, item_id: int, updated_item: Item) -> Optional[Item]:
        return self.repository.update(item_id, updated_item)

    def delete_item(self, item_id: int) -> bool:
        return self.repository.delete(item_id)

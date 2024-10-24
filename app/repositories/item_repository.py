from typing import List, Optional
from app.models.item_model import Item

class ItemRepository:
    def __init__(self):
        self._items = []  # Simulação de um banco de dados em memória

    def get_all(self) -> List[Item]:
        return self._items

    def get_by_id(self, item_id: int) -> Optional[Item]:
        return next((item for item in self._items if item.id == item_id), None)

    def save(self, item: Item) -> Item:
        self._items.append(item)
        return item

    def update(self, item_id: int, updated_item: Item) -> Optional[Item]:
        item = self.get_by_id(item_id)
        if item:
            item.name = updated_item.name
            item.price = updated_item.price
            item.description = updated_item.description
            return item
        return None

    def delete(self, item_id: int) -> bool:
        item = self.get_by_id(item_id)
        if item:
            self._items.remove(item)
            return True
        return False

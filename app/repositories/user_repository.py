from typing import List, Optional
from app.models.user_model import User

class UserRepository:
    def __init__(self):
        self._users = []  # Simulação de um banco de dados em memória

    def get_all(self) -> List[User]:
        return self._users

    def get_by_id(self, user_id: int) -> Optional[User]:
        return next((user for user in self._users if user.id == user_id), None)

    def save(self, user: User) -> User:
        self._users.append(user)
        return user

    def update(self, user_id: int, updated_user: User) -> Optional[User]:
        user = self.get_by_id(user_id)
        if user:
            user.name = updated_user.name
            user.email = updated_user.email
            return user
        return None

    def delete(self, user_id: int) -> bool:
        user = self.get_by_id(user_id)
        if user:
            self._users.remove(user)
            return True
        return False

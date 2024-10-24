from typing import List, Optional
from app.models.user_model import User
from app.repositories.user_repository import UserRepository

class UserService:
    def __init__(self, repository: UserRepository):
        self.repository = repository

    def get_users(self) -> List[User]:
        return self.repository.get_all()

    def get_user_by_id(self, user_id: int) -> Optional[User]:
        return self.repository.get_by_id(user_id)

    def create_user(self, user: User) -> User:
        return self.repository.save(user)

    def update_user(self, user_id: int, updated_user: User) -> Optional[User]:
        return self.repository.update(user_id, updated_user)

    def delete_user(self, user_id: int) -> bool:
        return self.repository.delete(user_id)

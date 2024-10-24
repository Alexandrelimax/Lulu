from typing import List
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse, Response
from app.models.user_model import User
from app.services.user_service import UserService
from app.controllers.base_controller import BaseController

class UserController(BaseController):
    def __init__(self, service: UserService):
        self.service = service
        super().__init__(prefix="/user")

    def register_routes(self):
        self.router.add_api_route("/", self.get_users, methods=["GET"], response_model=List[User])
        self.router.add_api_route("/", self.create_user, methods=["POST"], response_model=User)
        self.router.add_api_route("/{user_id}", self.get_user, methods=["GET"], response_model=User)
        self.router.add_api_route("/{user_id}", self.update_user, methods=["PUT"], response_model=User)
        self.router.add_api_route("/{user_id}", self.delete_user, methods=["DELETE"], response_model=User)

    def get_users(self) -> List[User]:
        return self.service.get_users()

    def create_user(self, user: User) -> User:
        created_user = self.service.create_user(user)
        return JSONResponse(content=created_user.model_dump(), status_code=status.HTTP_201_CREATED)

    def get_user(self, user_id: int) -> User:
        user = self.service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="user não encontrado")
        return user

    def update_user(self, user_id: int, updated_user: User) -> User:
        user = self.service.update_user(user_id, updated_user)
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="user não encontrado")
        return user

    def delete_user(self, user_id: int) -> Response:
        success = self.service.delete_user(user_id)
        if not success:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="user não encontrado")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

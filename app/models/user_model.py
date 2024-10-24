from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    last_name: str = None
    password: str
    email: str

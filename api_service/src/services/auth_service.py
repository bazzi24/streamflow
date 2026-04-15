from sqlalchemy.orm import Session
from ..core.security import verify_password, get_password_hash, create_access_token
from ..db.user_repo import UserRepository
from ..schemas.auth import UserCreate, Token, TokenWithUser, UserResponse
from ..config import get_settings
from typing import Optional

settings = get_settings()


class AuthService:
    def __init__(self, db: Session):
        self.repo = UserRepository(db)

    def register(self, user_data: UserCreate) -> TokenWithUser:
        existing = self.repo.get_by_email(user_data.email)
        if existing:
            raise ValueError("Email already registered")

        user = self.repo.create(
            email=user_data.email,
            username=user_data.username,
            password_hash=get_password_hash(user_data.password),
        )
        token = create_access_token(
            data={"sub": str(user.id), "email": user.email},
            secret_key=settings.secret_key,
        )
        return TokenWithUser(
            access_token=token,
            user=UserResponse.model_validate(user),
        )

    def login(self, email: str, password: str) -> TokenWithUser:
        user = self.repo.get_by_email(email)
        if not user or not verify_password(password, user.password_hash):
            raise ValueError("Invalid email or password")
        if not user.is_active:
            raise ValueError("Account is inactive")

        token = create_access_token(
            data={"sub": str(user.id), "email": user.email},
            secret_key=settings.secret_key,
        )
        return TokenWithUser(
            access_token=token,
            user=UserResponse.model_validate(user),
        )

from fastapi import Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from ..database import get_db, get_api_db, get_streaming_db
from ..core.security import decode_access_token
from ..db.user_repo import UserRepository
from ..models import User
from ..config import get_settings
from typing import Annotated

security = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)] = None,
    api_db: Session = Depends(get_api_db),
) -> User:
    """JWT-authenticated current user. Raises 401 if missing or invalid."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    settings = get_settings()
    payload = decode_access_token(credentials.credentials, settings.secret_key)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
    repo = UserRepository(api_db)
    user = repo.get_by_id(int(user_id))
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


def get_optional_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)] = None,
    api_db: Session = Depends(get_api_db),
) -> User | None:
    """Optional auth — returns None if no token."""
    if credentials is None:
        return None
    try:
        return get_current_user(credentials, api_db)
    except HTTPException:
        return None

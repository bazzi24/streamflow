from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from sqlalchemy.orm import Session
from ..database import get_api_db
from ...services.auth_service import AuthService
from ...schemas.auth import LoginRequest, UserCreate, TokenWithUser, Token
from ...core.security import create_refresh_token, decode_access_token
from ...config import get_settings

router = APIRouter(prefix="/auth", tags=["auth"])
settings = get_settings()


@router.post("/register", response_model=TokenWithUser, status_code=status.HTTP_201_CREATED)
def register(user_data: UserCreate, response: Response, db: Session = Depends(get_api_db)):
    service = AuthService(db)
    try:
        result = service.register(user_data)
        # Set httpOnly refresh token cookie
        _set_refresh_cookie(response, result.user.id, result.user.email)
        return result
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/login", response_model=TokenWithUser)
def login(req: LoginRequest, response: Response, db: Session = Depends(get_api_db)):
    service = AuthService(db)
    try:
        result = service.login(req.email, req.password)
        # Set httpOnly refresh token cookie
        _set_refresh_cookie(response, result.user.id, result.user.email)
        return result
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )


@router.post("/refresh", response_model=Token)
def refresh(request: Request, db: Session = Depends(get_api_db)):
    """Exchange refresh token cookie for a new access token."""
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    payload = decode_access_token(refresh_token, settings.secret_key)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token")

    user_id = payload.get("sub")
    email = payload.get("email")
    if not user_id or not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token")

    # Verify user still exists and is active
    repo = UserRepository(db)
    user = repo.get_by_email(email)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive")

    # Issue new access token
    access_token = create_access_token(
        data={"sub": str(user.id), "email": user.email},
        secret_key=settings.secret_key,
    )
    return Token(access_token=access_token)


@router.post("/logout")
def logout(response: Response):
    """Clear refresh token cookie."""
    response.delete_cookie(key="refresh_token", path="/auth/refresh")
    return {"message": "Logged out"}


def _set_refresh_cookie(response: Response, user_id: int, email: str) -> None:
    """Set httpOnly refresh token cookie."""
    refresh_token = create_refresh_token(
        data={"sub": str(user_id), "email": email},
        secret_key=settings.secret_key,
    )
    # Determine secure flag: use secure in production (not debug)
    is_secure = not settings.debug
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=is_secure,
        samesite="lax",
        max_age=7 * 24 * 60 * 60,  # 7 days in seconds
        path="/auth/refresh",
    )

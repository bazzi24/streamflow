from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from ..database import get_api_db
from ...services.auth_service import AuthService
from ...schemas.auth import LoginRequest, UserCreate, TokenWithUser

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=TokenWithUser, status_code=status.HTTP_201_CREATED)
def register(user_data: UserCreate, db: Session = Depends(get_api_db)):
    service = AuthService(db)
    try:
        return service.register(user_data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/login", response_model=TokenWithUser)
def login(req: LoginRequest, db: Session = Depends(get_api_db)):
    service = AuthService(db)
    try:
        return service.login(req.email, req.password)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

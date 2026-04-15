from sqlalchemy.orm import Session
from ..models import User, Watchlist


class UserRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_email(self, email: str) -> User | None:
        return self.db.query(User).filter(User.email == email).first()

    def get_by_id(self, user_id: int) -> User | None:
        return self.db.query(User).filter(User.id == user_id).first()

    def create(self, email: str, username: str, password_hash: str) -> User:
        user = User(email=email, username=username, password_hash=password_hash)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user

    def update_watchlist(self, user_id: int, symbols: list[str]) -> list[Watchlist]:
        # Remove existing, then add new
        self.db.query(Watchlist).filter(Watchlist.user_id == user_id).delete()
        self.db.commit()
        results = []
        for pos, sym in enumerate(symbols):
            w = Watchlist(user_id=user_id, symbol=sym, position=pos)
            self.db.add(w)
            results.append(w)
        self.db.commit()
        for w in results:
            self.db.refresh(w)
        return results

    def get_watchlist(self, user_id: int) -> list[Watchlist]:
        return (
            self.db.query(Watchlist)
            .filter(Watchlist.user_id == user_id)
            .order_by(Watchlist.position)
            .all()
        )

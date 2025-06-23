from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List, Dict, Any

from core.database import SessionLocal
from db.models import Base
from db.repositories import get_all, to_dict

router = APIRouter(prefix="/gold", tags=["Gold Layer"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

for table_name, model in Base.classes.items():
    path = f"/{table_name}"

    async def endpoint(
        db: Session = Depends(get_db), _model=model
    ) -> List[Dict[str, Any]]:
        rows = get_all(db, _model)
        return [to_dict(r) for r in rows]

    endpoint.__name__ = f"get_{table_name}_list"
    router.add_api_route(path, endpoint, methods=["GET"], summary=f"Listar {table_name}")

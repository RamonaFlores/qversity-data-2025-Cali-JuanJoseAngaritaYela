from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from core.models import GoldAgeByCountryOperator
from app.schemas.gold import GoldAgeByCountryOperatorSchema
from typing import List

router = APIRouter()

@router.get(
    "/gold/age-by-country-operator",
    response_model=List[GoldAgeByCountryOperatorSchema],
    tags=["Gold"]
)
def get_gold_age_by_country_operator(db: Session = Depends(get_db)):
    results = db.query(GoldAgeByCountryOperator).all()
    return results

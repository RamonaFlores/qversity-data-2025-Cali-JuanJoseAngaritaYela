from sqlalchemy import Column, String, BigInteger
from core.database import Base

class GoldAgeByCountryOperator(Base):
    __tablename__ = "gold_age_by_country_operator"
    __table_args__ = {"schema": "gold"}

    country = Column(String, primary_key=True)
    operator = Column(String, primary_key=True)
    age_group = Column(String, primary_key=True)
    customer_count = Column(BigInteger)
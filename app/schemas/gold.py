from pydantic import BaseModel

class GoldAgeByCountryOperatorSchema(BaseModel):
    country: str
    operator: str
    age_group: str
    customer_count: int

    class Config:
        orm_mode = True
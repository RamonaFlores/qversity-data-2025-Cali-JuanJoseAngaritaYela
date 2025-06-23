from pydantic import BaseModel
from typing import List
class GoldAgeByCountryOperatorSchema(BaseModel):
    country: str
    operator: str
    age_group: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldAgeDistributionByPlanOut(BaseModel):
    plan_type: str
    age_group: str
    customer_count: int

    class Config:
        orm_mode = True
class GoldArpuByPlanTypeOut(BaseModel):
    plan_type: str
    average_revenue_per_user: float

    class Config:
        orm_mode = True

class GoldCreditScorePaymentBehaviorOut(BaseModel):
    credit_score_group: str
    payment_behavior: str
    count: int

    class Config:
        orm_mode = True

class GoldCustomerAcquisitionByOperatorOut(BaseModel):
    operator: str
    acquisition_channel: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldCustomerDistributionByLocationOut(BaseModel):
    country: str
    city: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldCustomerStatusDistributionOut(BaseModel):
    status: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldCustomersWithPendingPaymentsSchema(BaseModel):
    customer_id: str
    full_name: str
    pending_amount_usd: float
    days_overdue: int
    plan_type: str
    operator: str
    country: str

    class Config:
        orm_mode = True

class GoldDeviceBrandByCountryOperatorSchema(BaseModel):
    country: str
    operator: str
    device_brand: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldDeviceBrandByPlanSchema(BaseModel):
    plan_type: str
    device_brand: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldDeviceBrandPopularitySchema(BaseModel):
    device_brand: str
    country: str
    operator: str
    plan_type: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldNewCustomersOverTimeSchema(BaseModel):
    registration_month: str
    country: str
    operator: str
    new_customers: int

    class Config:
        orm_mode = True



class GoldPaymentIssuesStatsSchema(BaseModel):
    issue_type: str
    total_customers: int
    percentage: float

    class Config:
        orm_mode = True

class GoldRevenueByLocationSchema(BaseModel):
    country: str
    city: str
    total_revenue_usd: float

    class Config:
        orm_mode = True

class GoldRevenueBySegmentSchema(BaseModel):
    segment: str
    total_revenue_usd: float

    class Config:
        orm_mode = True

class GoldRevenueByServiceComboSchema(BaseModel):
    service_combo: str
    total_revenue_usd: float

    class Config:
        orm_mode = True

class GoldRevenueStatsByPlanOperatorSchema(BaseModel):
    plan_type: str
    operator: str
    total_revenue_usd: float
    average_revenue_per_user: float

    class Config:
        orm_mode = True

class GoldSegmentByCreditScoreSchema(BaseModel):
    credit_score_group: str
    segment: str
    customer_count: int

    class Config:
        orm_mode = True

class GoldServiceCombinationsSchema(BaseModel):
    services_combination: str
    combination_count: int

    class Config:
        orm_mode = True

class GoldServicePopularitySchema(BaseModel):
    service_type: str
    service_count: int

    class Config:
        orm_mode = True
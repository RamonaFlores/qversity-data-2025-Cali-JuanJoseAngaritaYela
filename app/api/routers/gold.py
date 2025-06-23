from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from typing import List
from app.core.models import GoldAgeByCountryOperator,GoldAgeDistributionByPlan,GoldArpuByPlanType,GoldCreditScorePaymentBehavior,GoldCustomerAcquisitionByOperator,GoldCustomerDistributionByLocation,GoldCustomerStatusDistribution,GoldCustomersWithPendingPayments,GoldDeviceBrandByCountryOperator,GoldDeviceBrandByPlan,GoldDeviceBrandPopularity,GoldNewCustomersOverTime,GoldPaymentIssuesStats,GoldRevenueByLocation,GoldRevenueBySegment,GoldRevenueByServiceCombo,GoldRevenueStatsByPlanOperator,GoldSegmentByCreditScore,GoldServiceCombinations,GoldServicePopularity
from app.schemas.gold import GoldAgeByCountryOperatorSchema,GoldAgeDistributionByPlanOut,GoldArpuByPlanTypeOut,GoldCreditScorePaymentBehaviorOut,GoldCustomerAcquisitionByOperatorOut,GoldCustomerDistributionByLocationOut,GoldCustomerStatusDistributionOut,GoldCustomersWithPendingPaymentsSchema,GoldDeviceBrandByCountryOperatorSchema,GoldDeviceBrandByPlanSchema,GoldDeviceBrandPopularitySchema,GoldNewCustomersOverTimeSchema,GoldPaymentIssuesStatsSchema,GoldRevenueByLocationSchema,GoldRevenueBySegmentSchema,GoldRevenueByServiceComboSchema,GoldRevenueStatsByPlanOperatorSchema,GoldSegmentByCreditScoreSchema,GoldServiceCombinationsSchema,GoldServicePopularitySchema

router = APIRouter()

@router.get(
    "/gold/age-by-country-operator",
    response_model=List[GoldAgeByCountryOperatorSchema],
    tags=["Gold"]
)
def get_gold_age_by_country_operator(db: Session = Depends(get_db)):
    results = db.query(GoldAgeByCountryOperator).all()
    return results

@router.get("/gold/age-distribution-by-plan", response_model=list[GoldAgeDistributionByPlanOut])
async def get_age_distribution_by_plan(db: Session = Depends(get_db)):
    return db.query(GoldAgeDistributionByPlan).all()

@router.get("/gold/arpu-by-plan-type", response_model=list[GoldArpuByPlanTypeOut])
async def get_arpu_by_plan_type(db: Session = Depends(get_db)):
    return db.query(GoldArpuByPlanType).all()

@router.get("/gold/credit-score-payment-behavior", response_model=list[GoldCreditScorePaymentBehaviorOut])
async def get_credit_score_payment_behavior(db: Session = Depends(get_db)):
    return db.query(GoldCreditScorePaymentBehavior).all()


@router.get("/gold/customer-acquisition-by-operator", response_model=list[GoldCustomerAcquisitionByOperatorOut])
async def get_customer_acquisition_by_operator(db: Session = Depends(get_db)):
    return db.query(GoldCustomerAcquisitionByOperator).all()

@router.get("/gold/customer-distribution-by-location", response_model=list[GoldCustomerDistributionByLocationOut])
async def get_customer_distribution_by_location(db: Session = Depends(get_db)):
    return db.query(GoldCustomerDistributionByLocation).all()

@router.get("/gold/customer-status-distribution", response_model=list[GoldCustomerStatusDistributionOut])
async def get_customer_status_distribution(db: Session = Depends(get_db)):
    return db.query(GoldCustomerStatusDistribution).all()

@router.get("/gold/customers-with-pending-payments", response_model=List[GoldCustomersWithPendingPaymentsSchema])
def get_customers_with_pending_payments(db: Session = Depends(get_db)):
    return db.query(GoldCustomersWithPendingPayments).all()

@router.get("/gold/device-brand-by-country-operator", response_model=List[GoldDeviceBrandByCountryOperatorSchema])
def get_device_brand_by_country_operator(db: Session = Depends(get_db)):
    return db.query(GoldDeviceBrandByCountryOperator).all()

@router.get("/gold/device-brand-by-plan", response_model=List[GoldDeviceBrandByPlanSchema])
def get_device_brand_by_plan(db: Session = Depends(get_db)):
    return db.query(GoldDeviceBrandByPlan).all()

@router.get("/gold/device-brand-popularity", response_model=List[GoldDeviceBrandPopularitySchema])
def get_device_brand_popularity(db: Session = Depends(get_db)):
    return db.query(GoldDeviceBrandPopularity).all()

@router.get("/gold/new-customers-over-time", response_model=List[GoldNewCustomersOverTimeSchema])
def get_new_customers_over_time(db: Session = Depends(get_db)):
    return db.query(GoldNewCustomersOverTime).all()

@router.get("/gold/payment-issues-stats", response_model=List[GoldPaymentIssuesStatsSchema])
def get_payment_issues_stats(db: Session = Depends(get_db)):
    return db.query(GoldPaymentIssuesStats).all()

@router.get("/gold/revenue-by-location", response_model=List[GoldRevenueByLocationSchema])
def get_revenue_by_location(db: Session = Depends(get_db)):
    return db.query(GoldRevenueByLocation).all()

@router.get("/gold/revenue-by-segment", response_model=List[GoldRevenueBySegmentSchema])
def get_revenue_by_segment(db: Session = Depends(get_db)):
    return db.query(GoldRevenueBySegment).all()

@router.get("/gold/revenue-by-service-combo", response_model=List[GoldRevenueByServiceComboSchema])
def get_revenue_by_service_combo(db: Session = Depends(get_db)):
    return db.query(GoldRevenueByServiceCombo).all()

@router.get("/gold/revenue-stats-by-plan-operator", response_model=List[GoldRevenueStatsByPlanOperatorSchema])
def get_revenue_stats_by_plan_operator(db: Session = Depends(get_db)):
    return db.query(GoldRevenueStatsByPlanOperator).all()

@router.get("/gold/segment-by-credit-score", response_model=List[GoldSegmentByCreditScoreSchema])
def get_segment_by_credit_score(db: Session = Depends(get_db)):
    return db.query(GoldSegmentByCreditScore).all()

@router.get("/gold/service-combinations", response_model=List[GoldServiceCombinationsSchema])
def get_service_combinations(db: Session = Depends(get_db)):
    return db.query(GoldServiceCombinations).all()

@router.get("/gold/service-popularity", response_model=List[GoldServicePopularitySchema])
def get_service_popularity(db: Session = Depends(get_db)):
    return db.query(GoldServicePopularity).all()
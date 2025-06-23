from sqlalchemy import Column, String, BigInteger,Float, Integer
from app.core.database import Base
from typing import List
from sqlmodel import SQLModel, Field
from typing import Optional
class GoldAgeByCountryOperator(Base):
    __tablename__ = "gold_age_by_country_operator"
    __table_args__ = {"schema": "gold"}

    country = Column(String, primary_key=True)
    operator = Column(String, primary_key=True)
    age_group = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldAgeDistributionByPlan(Base):
    __tablename__ = "gold_age_distribution_by_plan"
    __table_args__ = {"schema": "gold"}

    plan_type = Column(String, primary_key=True)
    age_group = Column(String, primary_key=True)
    customer_count = Column(BigInteger)



class GoldArpuByPlanType(Base):
    __tablename__ = "gold_arpu_by_plan_type"
    __table_args__ = {"schema": "gold"}

    plan_type = Column(String, primary_key=True)
    average_revenue_per_user = Column(Float)

class GoldCreditScorePaymentBehavior(Base):
    __tablename__ = "gold_credit_score_payment_behavior"
    __table_args__ = {"schema": "gold"}

    credit_score_group = Column(String, primary_key=True)
    payment_behavior = Column(String, primary_key=True)
    count = Column(BigInteger)



class GoldCustomerAcquisitionByOperator(Base):
    __tablename__ = "gold_customer_acquisition_by_operator"
    __table_args__ = {"schema": "gold"}

    operator = Column(String, primary_key=True)
    acquisition_channel = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldCustomerDistributionByLocation(Base):
    __tablename__ = "gold_customer_distribution_by_location"
    __table_args__ = {"schema": "gold"}

    country = Column(String, primary_key=True)
    city = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldCustomerStatusDistribution(Base):
    __tablename__ = "gold_customer_status_distribution"
    __table_args__ = {"schema": "gold"}

    status = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldCustomersWithPendingPayments(Base):
    __tablename__ = "gold_customers_with_pending_payments"
    __table_args__ = {"schema": "gold"}

    customer_id = Column(String, primary_key=True)
    full_name = Column(String)
    pending_amount_usd = Column(Float)
    days_overdue = Column(Integer)
    plan_type = Column(String)
    operator = Column(String)
    country = Column(String)

class GoldDeviceBrandByCountryOperator(Base):
    __tablename__ = "gold_device_brand_by_country_operator"
    __table_args__ = {"schema": "gold"}

    country = Column(String, primary_key=True)
    operator = Column(String, primary_key=True)
    device_brand = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldDeviceBrandByPlan(Base):
    __tablename__ = "gold_device_brand_by_plan"
    __table_args__ = {"schema": "gold"}

    plan_type = Column(String, primary_key=True)
    device_brand = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldDeviceBrandPopularity(Base):
    __tablename__ = "gold_device_brand_popularity"
    __table_args__ = {"schema": "gold"}

    device_brand = Column(String, primary_key=True)
    country = Column(String, primary_key=True)
    operator = Column(String, primary_key=True)
    plan_type = Column(String, primary_key=True)
    customer_count = Column(BigInteger)


class GoldNewCustomersOverTime(Base):
    __tablename__ = "gold_new_customers_over_time"
    __table_args__ = {"schema": "gold"}

    registration_month = Column(String, primary_key=True)
    country = Column(String, primary_key=True)
    operator = Column(String, primary_key=True)
    new_customers = Column(BigInteger)

class GoldPaymentIssuesStats(Base):
    __tablename__ = "gold_payment_issues_stats"
    __table_args__ = {"schema": "gold"}

    issue_type = Column(String, primary_key=True)
    total_customers = Column(BigInteger)
    percentage = Column(Float)

class GoldRevenueByLocation(Base):
    __tablename__ = "gold_revenue_by_location"
    __table_args__ = {"schema": "gold"}

    country = Column(String, primary_key=True)
    city = Column(String, primary_key=True)
    total_revenue_usd = Column(Float)

class GoldRevenueBySegment(Base):
    __tablename__ = "gold_revenue_by_segment"
    __table_args__ = {"schema": "gold"}

    segment = Column(String, primary_key=True)
    total_revenue_usd = Column(Float)


class GoldRevenueByServiceCombo(Base):
    __tablename__ = "gold_revenue_by_service_combo"
    __table_args__ = {"schema": "gold"}

    service_combo = Column(String, primary_key=True)
    total_revenue_usd = Column(Float)


class GoldRevenueStatsByPlanOperator(Base):
    __tablename__ = "gold_revenue_stats_by_plan_operator"
    __table_args__ = {"schema": "gold"}

    plan_type = Column(String, primary_key=True)
    operator = Column(String, primary_key=True)
    total_revenue_usd = Column(Float)
    average_revenue_per_user = Column(Float)

class GoldSegmentByCreditScore(Base):
    __tablename__ = "gold_segment_by_credit_score"
    __table_args__ = {"schema": "gold"}

    credit_score_group = Column(String, primary_key=True)
    segment = Column(String, primary_key=True)
    customer_count = Column(BigInteger)

class GoldServiceCombinations(Base):
    __tablename__ = "gold_service_combinations"
    __table_args__ = {"schema": "gold"}

    services_combination = Column(String, primary_key=True)
    combination_count = Column(BigInteger)

class GoldServicePopularity(Base):
    __tablename__ = "gold_service_popularity"
    __table_args__ = {"schema": "gold"}

    service_type = Column(String, primary_key=True)
    service_count = Column(BigInteger)

schema = {
    "type": "object",
    "properties": {
        "customer_id": {"type": "integer"},
        "first_name": {"type": "string"},
        "last_name": {"type": "string"},
        "email": {"type": "string", "format": "email"},
        "phone_number": {"type": "string"},
        "age": {"type": "integer"},
        "country": {"type": "string"},
        "city": {"type": "string"},
        "operator": {"type": "string"},
        "plan_type": {"type": "string", "enum": ["pre_pago", "Pospago"]},
        "monthly_data_gb": {"type": "number"},
        "monthly_bill_usd": {"type": ["number", "null"]},
        "registration_date": {"type": "string", "format": "date"},
        "status": {"type": "string", "enum": ["ACTIVE", "suspended"]},
        "device_brand": {"type": "string"},
        "device_model": {"type": "string"},
        "contracted_services": {
            "type": "array",
            "items": {"type": "string"}
        },
        "record_uuid": {"type": ["string", "null"]},
        "last_payment_date": {"type": "string", "format": "date"},
        "credit_limit": {"type": "number"},
        "data_usage_current_month": {"type": "number"},
        "latitude": {"type": "number"},
        "longitude": {"type": "number"},
        "payment_history": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "format": "date"},
                    "status": {"type": "string", "enum": ["paid", "pending", "late", "failed"]},
                    "amount": {"type": ["number", "string"]}  # Se permite "unknown"
                },
                "required": ["date", "status", "amount"]
            }
        },
        "credit_score": {"type": "integer"}
    },
    "required": [
        "customer_id", "first_name", "last_name", "email",
        "phone_number", "age", "country", "city",
        "plan_type", "monthly_data_gb", "registration_date", "status",
        "contracted_services", "last_payment_date", "credit_limit",
        "data_usage_current_month", "latitude", "longitude", "payment_history"
    ]
}

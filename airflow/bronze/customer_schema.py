schema = {
    "type": "object",
    "properties": {
        "customer_id": {"type": "integer"},  # requerido
        "first_name": {"type": "string"},    # requerido
        "last_name": {"type": "string"},     # requerido
        "email": {
            "type": "string",
            "pattern": ".*@.*\\..*"           # permite emails básicos como "x@y.z"
        },  # requerido
        "phone_number": {
            "type": "string",
            "minLength": 7                   # para evitar vacíos o ruidos cortos
        },  # requerido
        "age": {"type": "integer", "minimum": 0, "maximum": 120},  # requerido
        "country": {"type": "string"},       # requerido
        "city": {"type": "string"},          # requerido
        "operator": {"type": "string"},      # requerido
        "plan_type": {"type": "string"},     # requerido
        "monthly_data_gb": {"type": "number"},  # requerido
        "monthly_bill_usd": {
            "type": ["number", "string", "null"]  # permite "unknown"
        },
        "registration_date": {
            "type": "string",
            "pattern": ".*"  # tolera date o timestamp
        },  # requerido
        "status": {"type": "string"},        # requerido
        "device_brand": {"type": ["string", "null"]},
        "device_model": {"type": ["string", "null"]},
        "contracted_services": {
            "type": "array",
            "items": {"type": "string"}
        },
        "record_uuid": {"type": "string"},  # requerido
        "last_payment_date": {
            "type": ["string", "null"],     # puede ser nulo en algunos casos
            "pattern": ".*"
        },  # requerido
        "credit_limit": {"type": "number"},  # requerido
        "data_usage_current_month": {"type": "number"},  # requerido
        "latitude": {"type": "number"},      # requerido
        "longitude": {"type": "number"},     # requerido
        "payment_history": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {"type": "string"},
                            "status": {"type": "string"},
                            "amount": {"type": ["number", "string"]}
                        },
                        "required": ["date", "status", "amount"]
                    }
                }
            ]
        },
        "credit_score": {
            "type": "integer",
            "minimum": 0,
            "maximum": 1000
        }  # requerido
    },
    "required": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "age",
        "country",
        "city",
        "operator",
        "plan_type",
        "monthly_data_gb",
        "registration_date",
        "status",
        "record_uuid",
        "last_payment_date",
        "credit_limit",
        "data_usage_current_month",
        "latitude",
        "longitude",
        "credit_score"
    ]
}

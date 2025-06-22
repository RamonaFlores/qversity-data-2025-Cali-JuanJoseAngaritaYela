schema = {
    "type": "object",
    "properties": {
        "customer_id": {"type": "integer"},  # required - unique customer identifier
        "first_name": {"type": "string"},    # required - customer's first name
        "last_name": {"type": "string"},     # required - customer's last name
        "email": {
            "type": "string",
            "pattern": ".*@.*\\..*"           # allows basic email format like "x@y.z"
        },  # required - must follow basic email pattern
        "phone_number": {
            "type": "string",
            "minLength": 7                   # avoid empty or very short noise
        },  # required - minimum 7 characters
        "age": {
            "type": "integer",
            "minimum": 0,
            "maximum": 120
        },  # required - must be a valid human age
        "country": {"type": "string"},       # required - country of residence
        "city": {"type": "string"},          # required - city of residence
        "operator": {"type": "string"},      # required - mobile service operator
        "plan_type": {"type": "string"},     # required - plan type (e.g. prepaid/postpaid)
        "monthly_data_gb": {"type": "number"},  # required - monthly data allowance in GB
        "monthly_bill_usd": {
            "type": ["number", "string", "null"]  # can be "unknown", numeric or null
        },
        "registration_date": {
            "type": "string",
            "pattern": ".*"  # allows date or timestamp formats
        },  # required - date of customer registration
        "status": {"type": "string"},        # required - account status (flexible values)
        "device_brand": {"type": ["string", "null"]},  # optional - mobile device brand
        "device_model": {"type": ["string", "null"]},  # optional - mobile device model
        "contracted_services": {
            "type": "array",
            "items": {"type": "string"}
        },  # optional - list of contracted services (e.g. internet, TV)
        "record_uuid": {"type": "string"},  # required - unique identifier for the record
        "last_payment_date": {
            "type": ["string", "null"],     # may be null for inactive or new accounts
            "pattern": ".*"
        },  # required - last payment date
        "credit_limit": {"type": "number"},  # required - assigned credit limit in USD
        "data_usage_current_month": {"type": "number"},  # required - usage in GB
        "latitude": {"type": "number"},      # required - geolocation: latitude
        "longitude": {"type": "number"},     # required - geolocation: longitude
        "payment_history": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {"type": "string"},      # date of payment
                            "status": {"type": "string"},    # e.g. "paid", "missed"
                            "amount": {"type": ["number", "string"]}  # paid amount
                        },
                        "required": ["date", "status", "amount"]
                    }
                }
            ]
        },  # optional - historical list of payments
        "credit_score": {
            "type": "integer",
            "minimum": 0,
            "maximum": 1000
        }  # required - score from credit bureau (0–1000)
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

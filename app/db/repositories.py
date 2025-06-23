from sqlalchemy.orm import Session

def get_all(db: Session, model):
    return db.query(model).all()

def to_dict(obj):
    data = obj.__dict__.copy()
    data.pop("_sa_instance_state", None)
    return data

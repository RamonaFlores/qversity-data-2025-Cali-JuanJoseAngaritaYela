from fastapi import FastAPI
from api.routers.gold import router as gold_router  # 👈 importa el router, no el módulo

app = FastAPI(
    title="Qversity – Gold Layer API",
    version="0.1.0",
    description="Auto generated endpoints of the SQL models on the gold layer.",
)

app.include_router(gold_router)  # 👈 aquí sí va el objeto router

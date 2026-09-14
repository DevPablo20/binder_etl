from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.routes.catalog import router as catalog_router
from src.config import settings
from src.spark_session import get_spark_session


@asynccontextmanager
async def lifespan(app: FastAPI):
    # local[N] pequeno e explícito — não local[*]. Serviço sempre-ligado lendo silver
    # pequeno; local[*] disputava núcleo com pipelines/testes local[*] no mesmo host.
    app.state.spark = get_spark_session(
        app_name="catalog-api", master=settings.catalog_spark_master
    )
    yield
    app.state.spark.stop()


app = FastAPI(
    title="Binder ETL Catalog",
    description="Lake identity catalog for backend Bridge discovery",
    lifespan=lifespan,
)
app.include_router(catalog_router)

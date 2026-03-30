from fastapi import FastAPI

from app.routers.customer import CUSTOMER_ROUTERS
from app.routers.v1 import V1_ROUTERS
from app.routers.web import WEB_ROUTERS

ALL_ROUTERS = (
	*V1_ROUTERS,
	*CUSTOMER_ROUTERS,
	*WEB_ROUTERS,
)


def include_all_routers(app: FastAPI) -> None:
	for router in ALL_ROUTERS:
		app.include_router(router)

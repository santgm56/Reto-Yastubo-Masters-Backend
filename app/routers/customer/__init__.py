from app.routers.customer import payments
from app.routers.customer import portal

CUSTOMER_ROUTERS = (
	payments.router,
	portal.router,
)

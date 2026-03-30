from app.routers.web import backoffice_shell
from app.routers.web import customer_shell

WEB_ROUTERS = (
	customer_shell.router,
	backoffice_shell.router,
)

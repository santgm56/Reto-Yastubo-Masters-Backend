from app.routers.v1 import admin_acl_roles_permissions
from app.routers.v1 import admin_business_units
from app.routers.v1 import admin_companies_capitated_batches
from app.routers.v1 import admin_companies_capitated_contracts
from app.routers.v1 import admin_companies_capitated_monthly_reports
from app.routers.v1 import admin_companies_commission_users_available
from app.routers.v1 import admin_companies_core
from app.routers.v1 import admin_companies_short_code
from app.routers.v1 import admin_companies_status
from app.routers.v1 import admin_companies_users
from app.routers.v1 import admin_config
from app.routers.v1 import admin_countries
from app.routers.v1 import admin_coverages
from app.routers.v1 import admin_plans
from app.routers.v1 import admin_products
from app.routers.v1 import admin_regalias
from app.routers.v1 import admin_templates
from app.routers.v1 import admin_users_search
from app.routers.v1 import admin_zones
from app.routers.v1 import audit
from app.routers.v1 import auth
from app.routers.v1 import cancellations
from app.routers.v1 import frontend_bootstrap
from app.routers.v1 import issuance
from app.routers.v1 import payments
from app.routers.v1 import public_capitated_contracts
from app.routers.v1 import public_files
from app.routers.v1 import seller_dashboard

V1_ROUTERS = (
	issuance.router,
	payments.router,
	cancellations.router,
	auth.router,
	frontend_bootstrap.router,
	public_files.router,
	public_capitated_contracts.router,
	audit.router,
	admin_companies_short_code.router,
	admin_companies_commission_users_available.router,
	admin_companies_users.router,
	admin_companies_status.router,
	admin_companies_capitated_batches.router,
	admin_companies_capitated_contracts.router,
	admin_companies_capitated_monthly_reports.router,
	admin_companies_core.router,
	admin_regalias.router,
	admin_users_search.router,
	admin_products.router,
	admin_plans.router,
	admin_coverages.router,
	admin_countries.router,
	admin_zones.router,
	admin_config.router,
	admin_business_units.router,
	admin_acl_roles_permissions.router,
	admin_templates.router,
	seller_dashboard.router,
)

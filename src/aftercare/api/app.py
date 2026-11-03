from threading import Lock

from fastapi import FastAPI
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from aftercare import __version__
from aftercare.api.fulfillment import router as fulfillment_router
from aftercare.api.read_pages import router as read_pages_router
from aftercare.api.routes import router
from aftercare.api.warehouse import router as warehouse_router
from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.contracts import BusinessError
from aftercare.repositories.database import database_health, session_factory
from aftercare.services.business import BusinessService


class LazyBusiness:
    """Health/import startup must remain usable even with unavailable or denied DB configuration."""

    def __init__(self, settings):
        self.settings = settings
        self.instance = None
        self.lock = Lock()

    def __getattr__(self, name):
        with self.lock:
            if self.instance is None:
                self.instance = BusinessService(session_factory(self.settings))
        return getattr(self.instance, name)


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings if settings is not None else Settings()
    app = FastAPI(title="售后 Agent 工作台", version=__version__)
    app.state.settings = config
    app.state.business = LazyBusiness(config)
    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "[::1]", "testserver"]
    )
    app.include_router(router)
    app.include_router(fulfillment_router)
    app.include_router(read_pages_router)
    app.include_router(warehouse_router)
    app.mount(
        "/static", StaticFiles(directory=PROJECT_ROOT / "src/aftercare/web/static"), name="static"
    )

    @app.get("/")
    def home():
        return FileResponse(PROJECT_ROOT / "src/aftercare/web/templates/index.html")

    @app.get("/fulfillment")
    def fulfillment_home():
        return FileResponse(PROJECT_ROOT / "src/aftercare/web/templates/fulfillment.html")

    @app.exception_handler(RequestValidationError)
    def invalid_request(request, error):
        # Pydantic's default input echo can expose a failed login's password.
        details = [
            {"loc": item["loc"], "type": item["type"], "msg": item["msg"]}
            for item in error.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error_code": "INVALID_INPUT",
                "user_message": "请检查输入内容。",
                "detail": details,
                "retryable": False,
            },
        )

    @app.exception_handler(HTTPException)
    def http_error(request, error):
        code = error.detail if isinstance(error.detail, str) else "HTTP_ERROR"
        return JSONResponse(
            status_code=error.status_code,
            content={
                "ok": False,
                "error_code": code,
                "user_message": code,
                "detail": error.detail,
                "retryable": False,
            },
        )

    @app.exception_handler(BusinessError)
    def business_error(request, error):
        status = 409
        if error.code in {"NOT_FOUND", "STAFF_NOT_FOUND"}:
            status = 404
        elif error.code in {
            "AUTH_FAILED",
            "SESSION_REQUIRED",
            "SESSION_EXPIRED",
            "ACCOUNT_DISABLED",
        }:
            status = 401
        elif error.code in {"FORBIDDEN", "ACTOR_REQUIRED", "ACTOR_INVALID"}:
            status = 403
        elif error.code in {"INVALID_COMMAND_DATA", "STAFF_INPUT_INVALID"}:
            status = 422
        elif error.code in {"INVENTORY_INVARIANT", "OUTBOX_INVARIANT"}:
            status = 503
        return JSONResponse(
            status_code=status,
            content={
                "ok": False,
                "error_code": error.code,
                "user_message": error.message,
                "retryable": error.retryable,
            },
        )

    @app.get("/health/live")
    def live() -> dict:
        return {
            "status": "alive",
            "phase": 4,
            "version": __version__,
            "mode": "offline",
            "demo_faults": config.dev_faults,
        }

    @app.get("/health/ready")
    def ready() -> JSONResponse:
        health = database_health(config)
        return JSONResponse(
            status_code=200 if health.status == "ready" else 503,
            content={**health.model_dump(), "scope": "phase-4-infrastructure"},
        )

    return app

import argparse
import getpass
import importlib.metadata
import json
import os
import platform
import sys
import time
from pathlib import Path

from pydantic import ValidationError

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.offline import install_offline_guard


def local_port(value):
    try:
        port = int(value)
        if 1024 <= port <= 65535:
            return port
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("PORT_MUST_BE_BETWEEN_1024_AND_65535")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="售后 Agent 工作台（默认离线）")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("doctor", "serve"):
        command = commands.add_parser(name)
        command.add_argument("--offline", action="store_true", help="禁止非本机网络（默认启用）")
        if name == "serve":
            command.add_argument("--port", type=int, default=8000)
            command.add_argument("--demo-faults", action="store_true")
            command.add_argument(
                "--demo-login", action="store_true", help="显式开启本机演示客户入口"
            )
            command.add_argument(
                "--simulate-warehouse", action="store_true", help="开启受权限保护的本地模拟出库"
            )
            command.add_argument(
                "--async-warehouse", action="store_true", help="事务Outbox投递到独立本机模拟仓储"
            )
            command.add_argument(
                "--container-network",
                action="store_true",
                help="容器内部监听所有接口；宿主映射仍须绑定 127.0.0.1",
            )
    db = commands.add_parser("db")
    db.add_argument("action", choices=["migrate", "current"])
    staff = commands.add_parser("staff")
    staff_actions = staff.add_subparsers(dest="staff_action", required=True)
    create = staff_actions.add_parser("create")
    create.add_argument("--username", required=True)
    create.add_argument("--role", choices=["support", "warehouse"], required=True)
    create.add_argument(
        "--password-stdin", action="store_true", help="从标准输入读取密码；默认隐藏输入"
    )
    revoke = staff_actions.add_parser("revoke")
    revoke.add_argument("--staff-id", required=True)
    disable = staff_actions.add_parser("disable")
    disable.add_argument("--staff-id", required=True)
    demo = commands.add_parser("demo")
    demos = demo.add_subparsers(dest="demo_action", required=True)
    seed = demos.add_parser("seed")
    seed.add_argument("--database", choices=["demo"], default="demo")
    reset = demos.add_parser("reset")
    reset.add_argument("--confirm", choices=["demo-only"], required=True)
    run = demos.add_parser("run")
    run.add_argument(
        "--case",
        choices=[
            "normal-exchange",
            "lost-submit-response",
            "revise-before-confirm",
            "concurrent-stock",
            "agent-exchange",
            "restart-after-commit",
        ],
        required=True,
    )
    run.add_argument("--provider", choices=["scripted"])
    retrieval = commands.add_parser("retrieval")
    rc = retrieval.add_subparsers(dest="retrieval_action", required=True)
    build = rc.add_parser("build")
    build.add_argument("--profile", default="local-lite")
    inspect = rc.add_parser("inspect")
    inspect.add_argument("--query", required=True)
    inspect.add_argument("--profile", default="bm25")
    evaluate = rc.add_parser("evaluate")
    evaluate.add_argument("--split", choices=["dev"], default="dev")
    evaluate.add_argument("--profiles", default="bm25,dense,hybrid,hybrid-rerank")
    evaluation = commands.add_parser("eval")
    ec = evaluation.add_subparsers(dest="eval_action", required=True)
    for name in ("validate", "run"):
        sub = ec.add_parser(name)
        sub.add_argument("--split", choices=["dev"], default="dev")
        if name == "run":
            sub.add_argument("--provider", choices=["scripted"], default="scripted")
            sub.add_argument(
                "--profile",
                choices=["full", "retrieval", "react-full-policy", "fixed-workflow"],
                default="full",
            )
            sub.add_argument("--limit", type=int)
    ec.add_parser("verify-grader")
    plan = ec.add_parser("plan")
    plan.add_argument("--batch", choices=["compatibility-smoke"], required=True)
    plan.add_argument("--dry-run", action="store_true", required=True)
    model = commands.add_parser("model")
    mc = model.add_subparsers(dest="model_action", required=True)
    smoke = mc.add_parser("smoke")
    smoke.add_argument(
        "--batch",
        choices=[
            "compatibility-smoke",
            "development-pilot",
            "business-loop-regression-v2",
            "quantity-clarification-regression-v1",
            "business-loop-regression-v3",
            "quantity-clarification-regression-v2",
            "business-loop-regression-v4",
            "diverse-behaviors-development-v1",
            "quantity-priority-development-v1",
            "final-development-acceptance-v1",
            "quantity-clarification-regression-v3",
        ],
        required=True,
    )
    smoke.add_argument("--approval", required=True)
    smoke.add_argument("--account-confirmation", required=True)
    smoke.add_argument("--execute", action="store_true", required=True)
    worker = commands.add_parser("worker")
    worker.add_argument("--provider", choices=["scripted"], default="scripted")
    worker.add_argument("--once", action="store_true")
    outbox = commands.add_parser("outbox-worker", help="独立的本机仓储Outbox消费者")
    outbox.add_argument("--once", action="store_true")
    outbox.add_argument("--owner")
    outbox.add_argument("--log-json", action="store_true", help="向stderr输出有界的结构化阶段日志")
    reconciliation = commands.add_parser("reconciliation-worker", help="有界仓储查单与结果合并")
    reconciliation.add_argument("--once", action="store_true")
    reconciliation.add_argument("--owner")
    reconciliation.add_argument(
        "--log-json", action="store_true", help="向stderr输出有界的结构化阶段日志"
    )
    warehouse = commands.add_parser("mock-warehouse", help="独立持久化的本机模拟仓储")
    warehouse.add_argument("--port", type=local_port, default=8140)
    warehouse.add_argument(
        "--state-path", type=Path, default=PROJECT_ROOT / "runtime/mock-warehouse/state.sqlite3"
    )
    warehouse.add_argument("--test-faults", action="store_true")
    callbacks = commands.add_parser("mock-warehouse-callbacks", help="独立仓储持久回调发送器")
    callbacks.add_argument("--once", action="store_true")
    callbacks.add_argument("--effect-key")
    callbacks.add_argument(
        "--state-path", type=Path, default=PROJECT_ROOT / "runtime/mock-warehouse/state.sqlite3"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "model":
        from aftercare.llm.smoke import run_smoke

        try:
            if args.batch in {
                "business-loop-regression-v2",
                "quantity-clarification-regression-v1",
                "business-loop-regression-v3",
                "quantity-clarification-regression-v2",
                "business-loop-regression-v4",
                "diverse-behaviors-development-v1",
                "quantity-priority-development-v1",
                "final-development-acceptance-v1",
                "quantity-clarification-regression-v3",
            }:
                sys.path.insert(0, str(PROJECT_ROOT))
                from evaluation.evaluator.regression_live import run

                approval = json.loads(Path(args.approval).read_text())
                if approval.get("batch_id") != args.batch:
                    raise ValueError("REGRESSION_BATCH_ARGUMENT_MISMATCH")
                output = run(Settings(), args.approval, args.account_confirmation)
            elif args.batch == "development-pilot":
                sys.path.insert(0, str(PROJECT_ROOT))
                from evaluation.evaluator.pilot import run

                output = run(Settings(), args.approval, args.account_confirmation)
            else:
                output = run_smoke(Settings(), args.approval, args.account_confirmation)
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0 if output["status"] == "COMPLETED" else 1
        except Exception as error:
            print(
                json.dumps(
                    {
                        "status": "STOPPED",
                        "error_code": getattr(error, "code", type(error).__name__),
                    }
                )
            )
            return 1
    install_offline_guard()
    if args.command in {"mock-warehouse", "mock-warehouse-callbacks"}:
        import uvicorn

        from aftercare.mock_warehouse.app import create_app as create_warehouse

        try:
            state_path = args.state_path.resolve()
            if not state_path.is_relative_to((PROJECT_ROOT / "runtime").resolve()):
                raise ValueError("WAREHOUSE_STATE_MUST_BE_PROJECT_RUNTIME")
            callback_url = os.environ.get("MOCK_WAREHOUSE_CALLBACK_BASE_URL") or None
            callback_secret = os.environ.get("MOCK_WAREHOUSE_CALLBACK_SECRET") or None
            if args.command == "mock-warehouse-callbacks":
                from aftercare.mock_warehouse.callbacks import CallbackSender

                sender = CallbackSender(state_path, callback_url, callback_secret)
                while True:
                    result = sender.work_once(effect_key=args.effect_key)
                    if args.once or result.get("status") != "IDLE":
                        print(json.dumps(result, ensure_ascii=False), flush=True)
                    if args.once:
                        return 0
                    time.sleep(0.5)
            app = create_warehouse(
                state_path,
                os.environ.get("MOCK_WAREHOUSE_TOKEN", ""),
                test_mode=args.test_faults,
                callback_base_url=callback_url,
                callback_secret=callback_secret,
            )
        except Exception:
            print(json.dumps({"ok": False, "error_code": "MOCK_WAREHOUSE_CONFIG_INVALID"}))
            return 2
        uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)
        return 0
    try:
        overrides = {"offline": True, "dev_faults": getattr(args, "demo_faults", False)}
        if getattr(args, "demo_login", False):
            overrides["demo_login_enabled"] = True
        if getattr(args, "simulate_warehouse", False):
            overrides["warehouse_simulation_enabled"] = True
        if getattr(args, "async_warehouse", False):
            overrides["warehouse_delivery_enabled"] = True
        settings = Settings(**overrides)
    except ValidationError:
        print(json.dumps({"status": "invalid_config", "code": "CONFIG_INVALID"}))
        return 2

    if args.command == "eval":
        # Evaluator is intentionally outside the installable product package/container.
        sys.path.insert(0, str(PROJECT_ROOT))
        from evaluation.evaluator.grader import verify_mutations
        from evaluation.evaluator.plan import compatibility_plan
        from evaluation.evaluator.runner import run_development, validate

        if args.eval_action == "validate":
            output = validate()
        elif args.eval_action == "verify-grader":
            output = verify_mutations()
        elif args.eval_action == "plan":
            output = compatibility_plan()
        else:
            output = run_development(args.profile, args.limit)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0 if output.get("valid", output.get("passed", True)) else 1

    if args.command == "doctor":
        from aftercare.repositories.database import database_health

        health = database_health(settings)
        dependencies = {
            name: importlib.metadata.version(name)
            for name in ("langchain", "langgraph", "psycopg", "fastapi", "alembic")
        }
        print(
            json.dumps(
                {
                    "settings": settings.public_summary(),
                    "python": platform.python_version(),
                    "dependencies": dependencies,
                    "database": health.model_dump(),
                    "next_step": "数据库未就绪时，按 docs/operations.md 启动专用数据库并迁移。",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if health.status == "ready" else 1

    if args.command == "serve":
        import uvicorn

        from aftercare.api.app import create_app

        uvicorn.run(
            create_app(settings),
            host="0.0.0.0" if args.container_network else "127.0.0.1",
            port=args.port,
            access_log=False,
        )
        return 0

    if args.command == "db":
        from alembic import command
        from alembic.config import Config
        from sqlalchemy.engine import make_url

        from aftercare.repositories.database import is_local_database

        url = make_url(settings.database_url.get_secret_value())
        if not is_local_database(url) or url.database not in {"aftercare_demo", "aftercare_test"}:
            print("DEDICATED_LOCAL_DATABASE_REQUIRED")
            return 2
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.attributes["settings"] = settings
        try:
            if args.action == "migrate":
                command.upgrade(config, "head")
                from aftercare.agent.runtime import setup_checkpoints

                setup_checkpoints(settings)
            else:
                command.current(config)
        except Exception:
            print("DATABASE_COMMAND_FAILED: check local database availability and permissions")
            return 1
        return 0
    if args.command == "staff":
        from aftercare.domain.contracts import BusinessError
        from aftercare.repositories.database import session_factory
        from aftercare.services.demo import assert_dedicated
        from aftercare.services.staff_auth import StaffAuthService

        sessions = session_factory(settings)
        try:
            assert_dedicated(sessions)
            auth = StaffAuthService(sessions)
            if args.staff_action == "create":
                password = (
                    sys.stdin.readline().rstrip("\r\n")
                    if args.password_stdin
                    else getpass.getpass("员工密码（至少12字符）: ")
                )
                output = auth.create_account(args.username, password, args.role)
            elif args.staff_action == "revoke":
                output = auth.revoke_all(args.staff_id)
            else:
                output = auth.update_account(args.staff_id, active=False)
            print(json.dumps(output, ensure_ascii=False))
            return 0
        except BusinessError as error:
            print(json.dumps({"ok": False, "error_code": error.code}))
            return 2
        except Exception:
            print(json.dumps({"ok": False, "error_code": "STAFF_COMMAND_FAILED"}))
            return 1
        finally:
            sessions.kw["bind"].dispose()

    if args.command == "worker":
        from aftercare.workers.worker import work

        work(settings, once=args.once)
        return 0
    if args.command in {"outbox-worker", "reconciliation-worker"}:
        from aftercare.domain.contracts import BusinessError
        from aftercare.workers.outbox_worker import work as outbox_work

        try:
            if args.command == "reconciliation-worker":
                from aftercare.workers.reconciliation_worker import work as outbox_work
            worker_options = {
                "once": args.once,
                "owner": args.owner,
                "emit": lambda result: print(json.dumps(result, ensure_ascii=False), flush=True),
            }
            if args.log_json:
                worker_options["log_json"] = True
            outbox_work(settings, **worker_options)
            return 0
        except BusinessError as error:
            print(json.dumps({"ok": False, "error_code": error.code}))
        except Exception:
            code = (
                "RECONCILIATION_WORKER_STOPPED"
                if args.command == "reconciliation-worker"
                else "OUTBOX_WORKER_STOPPED"
            )
            print(json.dumps({"ok": False, "error_code": code}))
        return 1
    if args.command == "retrieval":
        from aftercare.repositories.database import session_factory
        from aftercare.retrieval.engine import PolicySearch
        from aftercare.retrieval.evaluate import evaluate_profiles

        sessions = session_factory(settings)
        engine = PolicySearch(sessions=sessions)
        if args.retrieval_action == "build":
            output = engine.build(args.profile)
        elif args.retrieval_action == "inspect":
            output = engine.search(args.query, profile=args.profile)
        else:
            output = evaluate_profiles(engine, args.profiles.split(","))
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    if args.command == "demo":
        from aftercare.repositories.database import session_factory
        from aftercare.services.business import BusinessService
        from aftercare.services.demo import reset_demo, run_case, seed

        sessions = session_factory(settings)
        if args.demo_action == "run" and args.provider == "scripted":
            from aftercare.agent.demo import run_agent_demo

            print(
                json.dumps(
                    run_agent_demo(settings, BusinessService(sessions), args.case),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        if args.demo_action == "reset":
            print(json.dumps(reset_demo(sessions, args.confirm), ensure_ascii=False))
            sessions.kw["bind"].dispose()
            return 0
        output = (
            seed(sessions)
            if args.demo_action == "seed"
            else run_case(BusinessService(sessions), args.case)
        )
        print(json.dumps(output, ensure_ascii=False, indent=2))
        sessions.kw["bind"].dispose()
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())

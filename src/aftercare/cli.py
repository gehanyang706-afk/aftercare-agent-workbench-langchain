import argparse
import importlib.metadata
import json
import platform
import sys
from pathlib import Path

from pydantic import ValidationError

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.offline import install_offline_guard


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
                "--container-network",
                action="store_true",
                help="容器内部监听所有接口；宿主映射仍须绑定 127.0.0.1",
            )
    db = commands.add_parser("db")
    db.add_argument("action", choices=["migrate", "current"])
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
    try:
        settings = Settings(offline=True, dev_faults=getattr(args, "demo_faults", False))
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
    if args.command == "worker":
        from aftercare.workers.worker import work

        work(settings, once=args.once)
        return 0
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

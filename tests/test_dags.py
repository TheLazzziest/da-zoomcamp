import ast
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def _dag_decorators(tree: ast.Module) -> list[ast.Call]:
    decorators = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if (
                    isinstance(decorator, ast.Call)
                    and getattr(decorator.func, "id", None) == "dag"
                ):
                    decorators.append(decorator)
    return decorators


def test_dag_files_parse_and_declare_ids():
    files = sorted(DAGS_DIR.glob("ingest_*.py"))
    assert files, "no ingestion DAG files found"
    for path in files:
        tree = ast.parse(path.read_text(), filename=str(path))
        decorators = _dag_decorators(tree)
        assert decorators, f"{path.name} has no @dag decorator"
        dag_ids = [
            kw.value.value
            for decorator in decorators
            for kw in decorator.keywords
            if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant)
        ]
        assert dag_ids, f"{path.name} has no dag_id"


def test_common_helpers_exist():
    tree = ast.parse((DAGS_DIR / "common.py").read_text())
    functions = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    assert {"get_destination", "current_period"} <= functions

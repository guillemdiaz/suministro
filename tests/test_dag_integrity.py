import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    """Fixture to load all DAGs once per test session."""
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dag_bag):
    """Verifies that all DAG files parse without import or syntax errors."""
    import_errors = dag_bag.import_errors
    assert len(import_errors) == 0, f"DAG import failures detected: {import_errors}"

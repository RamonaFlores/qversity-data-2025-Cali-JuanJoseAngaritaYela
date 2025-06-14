from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("ingest_customers_data_dag")
    assert dag is not None
    assert len(dag.tasks) == 2
    assert "download_json" in dag.task_ids
    assert "load_to_postgres" in dag.task_ids

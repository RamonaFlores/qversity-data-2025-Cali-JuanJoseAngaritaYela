from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag()

    # Asegúrate de que no haya errores de importación en los DAGs
    assert not dag_bag.import_errors, f"Errores al importar los DAGs: {dag_bag.import_errors}"

    dag = dag_bag.get_dag("ingest_customers_data_dag")
    assert dag is not None, "El DAG 'ingest_customers_data_dag' no se encontró"
    assert len(dag.tasks) == 2, "El DAG no tiene exactamente 2 tareas"
    assert "download_json" in dag.task_ids, "Falta la tarea 'download_json'"
    assert "load_to_postgres" in dag.task_ids, "Falta la tarea 'load_to_postgres'"

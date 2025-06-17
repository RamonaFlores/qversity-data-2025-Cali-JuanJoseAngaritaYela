from sqlalchemy import create_engine, text
from .utils import log


def load_dataframe(df, db_conn: str, schema: str, table: str):
    """
        Append a pandas DataFrame to a relational table, creating the schema
        if it does not yet exist.

        The function leverages SQLAlchemy for connection pooling and transaction
        management.  It first ensures the target schema is present (executed in
        AUTOCOMMIT mode to avoid blocking), then inserts the DataFrame via
        ``df.to_sql`` inside a transactional block—so any failure rolls back
        automatically.  A concise log entry is written on success.

        Parameters
        ----------
        df : pandas.DataFrame
            The data you wish to persist.
        db_conn : str
            SQLAlchemy-compatible connection string
            (e.g., ``"postgresql+psycopg2://user:pass@host/db"``).
        schema : str
            Destination schema; will be created if missing.
        table : str
            Destination table name.

        Returns
        -------
        None
            The function’s sole side effects are writing to the database and
            emitting a log entry.

        Raises
        ------
        sqlalchemy.exc.SQLAlchemyError
            Propagated if the insert or DDL fails for any reason.

        Example
        -------
        >>> load_dataframe(df_orders, os.environ["WAREHOUSE_DSN"], "bronze", "orders")
    """

    #Creates an engine with  qversity's POSTGRESQL 
    #and sends a ping before every checkout to avoid dead end connections
    engine = create_engine(db_conn, pool_pre_ping=True)
    
    #Opens a raw connection and forces the AUTOCOMMIT mode to allow us
    #to execute DDL without explict BEGIN/COMMIT clausules
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:

        #Executes a DDL that creates a schema if it doesnt exist

        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    #Opens another connection in transactional mode , allowing the connection
    #to do an automatic rollback THIS IS HUGE AND REALLY USEFUL   

    with engine.begin() as conn:
        #Sends the dataframe to the db, adding rows without deleting
        #the existing ones and ignores the pandas index.
        df.to_sql(table, conn, schema=schema, if_exists="append", index=False)
    #Logs the ammount of inserted roads
    #[load] makes reference of the ETL phase !!
    #there's no transform in the bronze layer
    log.info(f"[load] Inserted {len(df)} rows into {schema}.{table}")
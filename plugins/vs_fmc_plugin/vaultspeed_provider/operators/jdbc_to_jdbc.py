from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

from vaultspeed_provider.operators.db_table import DBTable


class JdbcToJdbcTransfer(BaseOperator):
    """
    Move data from one JDBC connection to another in batches.

    :param object_mtd: dictionary or list of dictionaries containing the metadata of the source and target tables
        dictionary structure:
        {
            object_name: String,
            src_mtd: {schema_name: String, table_name: String, column_names: List[String]},
            target_mtd: {schema_name: String, table_name: String, column_names: List[String], window_column_name: Optional String}
        }
    :param src_conn_id: reference to the source database connection
    :param target_conn_id: reference to the source database connection
    :param batch_size: maximum number or records loaded at the same time (str containing an int or int)
    :param use_window: whether to use a window to retrieve only the latest data from the source
    :param window_sql: SQL statement which runs on the target and should return a single timestamp,
        all data in the source with a cdc timestamp after this timestamp will get transferred to the target.
        This field can be templated and if it contains a "{object}",
        then that will get replaced by the object_name from the object_mtd
    """
    ui_color = "#6faef2"  # light blue
    template_fields = ("window_sql", "batch_size",)

    def __init__(self, object_mtd, src_conn_id, target_conn_id, batch_size, window_sql=None, *args, **kwargs):
        if not isinstance(object_mtd, list):
            self.object_mtd = [object_mtd]
        else:
            self.object_mtd = object_mtd
        self.src_conn_id = src_conn_id
        self.target_conn_id = target_conn_id
        self.batch_size = batch_size
        self.use_window = window_sql is not None
        self.window_sql = window_sql
        super(JdbcToJdbcTransfer, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:
            from vaultspeed_provider.hooks.jdbc_hook import JdbcHook
            import jaydebeapi as jdbc
            from datetime import datetime

            def get_string(rs, col):
                return rs.getString(col)

            def get_object(rs, col):
                return rs.getObject(col)

            def get_bytes(rs, col):
                b = rs.getObject(col)
                return bytes(b) if b is not None else b

            # Adding converters for OTHER, STRUCT and BINARY and replacing the ones where we want to retain Java types
            CONVERTER_UPDATES = {
                "TIMESTAMP": get_object,
                "TIME": get_object,
                "DATE": get_string,
                "BINARY": get_bytes,
                "OTHER": get_string,
                "STRUCT": get_string
            }
            
            self.log.info("Establishing connections.")
    
            src_hook = JdbcHook(jdbc_conn_id=self.src_conn_id)
            target_hook = JdbcHook(jdbc_conn_id=self.target_conn_id)
            src_conn = src_hook.get_conn()
            target_conn = target_hook.get_conn()
            # update converters to facilitate data transfer between different Database types
            src_conn._converters.update({jdbc._jdbc_name_to_const[k]: v for (k, v) in CONVERTER_UPDATES.items()})
            autocommit_disabled = True
            try:
                target_hook.set_autocommit(target_conn, False)
            except Exception as exc:
                # autocommit control is optional and some JDBC driver might not support it (i.e. simba spark)
                autocommit_disabled = False
                self.log.info(f"set_autocommit(False) failed: {exc}")
        
            self.log.info("Connections established.")
            
            src_cur = src_conn.cursor()
            target_cur = target_conn.cursor()
            
            for object_mtd in self.object_mtd:
                target_table = DBTable(object_mtd["target_mtd"])
                src_table = DBTable(object_mtd["src_mtd"])
                object_name = object_mtd["object_name"]
                self.log.info(f"Transferring {object_name}.")
              
                truncate_query = target_table.truncate_query()
                target_cur.execute(truncate_query)
                if autocommit_disabled:
                    target_conn.commit()
    
                if self.use_window and src_table.window_column_name:
                    target_cur.execute(self.window_sql.format(object=object_name, fmc_window_col=object_mtd["target_mtd"]["fmc_window_col"]))
                    start_date = target_cur.fetchone()[0]
                    self.log.info(
                        f"Start loading data from {src_table.get_table_ref()} to {target_table.get_table_ref()}, with start date: {start_date}.")
                    fetch_query = src_table.windowed_fetch_query()
                    src_cur.execute(fetch_query, [start_date])
                else:
                    self.log.info(f"Start loading data from {src_table.get_table_ref()} to {target_table.get_table_ref()}.")
                    fetch_query = src_table.fetch_query()
                    src_cur.execute(fetch_query)
                
                insert_query = target_table.insert_query()
    
                self.log.info(f"Using batches of size: {self.batch_size}.")
                i = 1
                num_rec = 0
                while True:
                    data = src_cur.fetchmany(int(self.batch_size))
                    if not data:
                        break
                    self.log.info(f"Loading batch {i}.")
                    target_cur.executemany(insert_query, data)
                    num_rec += len(data)
                    i += 1
                  
                self.log.info(f"Total records transferred for {object_name}: {num_rec}.")
                
                if autocommit_disabled:
                    target_conn.commit()
    
            src_conn.close()
            target_conn.close()
    
            self.log.info("Finished loading data.")
            
        except Exception as e:
            self.log.error(e, exc_info=e)
            raise AirflowException(e)

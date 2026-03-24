from vaultspeed_provider.hooks.jdbc_hook import JdbcHook
from contextlib import closing, contextmanager


class SingleStoreHook(JdbcHook):
    """
    A client to interact with SingleStore database.

    This hook requires the singlestore_conn_id connection. The singlestore host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation.
    """

    conn_name_attr = 'singlestore_conn_id'
    default_conn_name = 'singlestore_default'
    conn_type = 'singlestore'
    hook_name = 'SingleStore'

    @contextmanager
    def _create_autocommit_connection(self, autocommit: bool = False):
        """Context manager that closes the connection after use and detects if autocommit is supported."""
        with closing(self.get_conn()) as conn:
            try:
                if self.supports_autocommit:
                    self.set_autocommit(conn, autocommit)
                yield conn
            except Exception as e:
                with closing(conn.cursor()) as cur:
                    # For Singlestore we need to explicitly roll back the transaction after a failure, this does not happen automatically
                    cur.execute("ROLLBACK;")
                    self.log.error(e, exc_info=e)
                    raise e

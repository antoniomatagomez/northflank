#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Any, Optional

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.models.connection import Connection

try:
    # optional import of JDBC
    from jaydebeapi import Connection as JdbcConnection, connect as jdbc_connect
except ImportError:
    class JdbcConnection:
        def __init__(self, *args, **kwargs):
            raise ImportError("jaydebeapi is required to use JdbcHook")
    jdbc_connect = JdbcConnection


class JdbcHook(DbApiHook):
    """
    General hook for jdbc db access.

    JDBC URL, username and password will be taken from the predefined connection.
    Note that the whole JDBC URL must be specified in the "host" field in the DB.
    Raises an airflow error if the given connection id doesn't exist.
    """

    conn_name_attr = 'jdbc_conn_id'
    default_conn_name = 'jdbc_default'
    conn_type = 'jdbc'
    hook_name = 'JDBC Connection'
    supports_autocommit = True

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "driver_path": StringField(lazy_gettext('Driver Path'), widget=BS3TextFieldWidget()),
            "driver_class": StringField(
                lazy_gettext('Driver Class'), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'schema', 'extra'],
            "relabeling": {'host': 'Connection URL'},
        }

    def get_conn(self) -> JdbcConnection:
        conn: Connection = self.connection
        host: str = conn.host
        login: str = conn.login
        psw: str = conn.password
        jdbc_driver_loc: Optional[str] = conn.extra_dejson.get('driver_path')
        jdbc_driver_name: Optional[str] = conn.extra_dejson.get('driver_class')
        driver_args = []
        # For some connections, login and password are optional.
        # In this case the driver arguments should be an empty array instead of an array of None values
        if login: driver_args.append(str(login))
        if psw: driver_args.append(str(psw))
        
        return jdbc_connect(
            jclassname=jdbc_driver_name,
            url=str(host),
            driver_args=driver_args,
            jars=jdbc_driver_loc.split(",") if jdbc_driver_loc else None,
        )

    def set_autocommit(self, conn: JdbcConnection, autocommit: bool) -> None:
        """
        Enable or disable autocommit for the given connection.

        :param conn: The connection.
        :type conn: connection object
        :param autocommit: The connection's autocommit setting.
        :type autocommit: bool
        """
        conn.jconn.setAutoCommit(autocommit)

    def get_autocommit(self, conn: JdbcConnection) -> bool:
        """
        Get autocommit setting for the provided connection.
        Return True if conn.autocommit is set to True.
        Return False if conn.autocommit is not set or set to False

        :param conn: The connection.
        :type conn: connection object
        :return: connection autocommit setting.
        :rtype: bool
        """
        return conn.jconn.getAutoCommit()

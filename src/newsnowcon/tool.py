"""
Snowflake Connector Tool for CrewAI — execute SQL via the Snowflake Python connector.

Uses username/password authentication only. No PAT or Snowflake MCP API.
"""

import os
import json
from typing import Any, Optional, Type

import snowflake.connector
from pydantic import BaseModel, Field

from crewai.tools import BaseTool

# Environment variable names for no-arg / CrewAI Studio usage
_ENV_ACCOUNT = "SNOWFLAKE_ACCOUNT"
_ENV_USER = "SNOWFLAKE_USER"
_ENV_PASSWORD = "SNOWFLAKE_PASSWORD"
_ENV_DATABASE = "SNOWFLAKE_DATABASE"
_ENV_SCHEMA = "SNOWFLAKE_SCHEMA"
_ENV_WAREHOUSE = "SNOWFLAKE_WAREHOUSE"
_ENV_ROLE = "SNOWFLAKE_ROLE"


class SnowflakeConnInput(BaseModel):
    """Input schema for Snowflake Connector Tool."""

    query: str = Field(..., description="The SQL query to execute (e.g. SELECT ...).")


class SnowflakeConn(BaseTool):
    """
    Execute SQL in Snowflake using the Python connector with username/password.
    """

    name: str = "snowflake_conn"
    description: str = (
        "Execute a SQL query in Snowflake and return the results. "
        "Provide the exact SQL string in the 'query' argument. "
        "Returns rows as JSON or an error message."
    )
    args_schema: Type[BaseModel] = SnowflakeConnInput

    account: str = Field(..., description="Snowflake account identifier (e.g. xy12345.us-east-1)")
    user: str = Field(..., description="Snowflake user name")
    password: Optional[str] = Field(None, description="Snowflake password (use with user)")
    database: str = Field(..., description="Database to use")
    schema: str = Field(..., description="Schema to use")
    warehouse: str = Field(..., description="Warehouse to use")
    role: Optional[str] = Field(None, description="Role to use (optional)")
    max_rows: int = Field(1000, description="Maximum rows to return (avoids oversized context).")

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        max_rows: int = 1000,
        **kwargs: Any,
    ):
        # Allow no-arg instantiation (e.g. CrewAI Studio) by reading from environment
        account = account if account is not None else os.environ.get(_ENV_ACCOUNT, "")
        user = user if user is not None else os.environ.get(_ENV_USER, "")
        password = password if password is not None else os.environ.get(_ENV_PASSWORD)
        database = database if database is not None else os.environ.get(_ENV_DATABASE, "")
        schema = schema if schema is not None else os.environ.get(_ENV_SCHEMA, "")
        warehouse = warehouse if warehouse is not None else os.environ.get(_ENV_WAREHOUSE, "")
        role = role if role is not None else os.environ.get(_ENV_ROLE)

        missing = [
            name for name, val in (
                ("account", account), ("user", user),
                ("database", database), ("schema", schema), ("warehouse", warehouse),
            )
            if not val
        ]
        if missing:
            raise ValueError(
                f"SnowflakeConn requires: {', '.join(missing)}. "
                "Pass them explicitly or set the corresponding SNOWFLAKE_* environment variables."
            )

        super().__init__(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
            max_rows=max_rows,
            **kwargs,
        )

    def _run(self, query: str) -> str:
        """Execute the SQL query and return results as a JSON string."""
        if not query.strip():
            return json.dumps({"error": "Empty query"})

        conn = None
        try:
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
            )
            cur = conn.cursor()
            try:
                cur.execute(query)
                rows = cur.fetchmany(self.max_rows)
                columns = [d[0] for d in cur.description] if cur.description else []
            finally:
                cur.close()
            result = [dict(zip(columns, row)) for row in rows]
            out = json.dumps(result, indent=2, default=str)
            if len(rows) == self.max_rows:
                out += f"\n\n[Result truncated at {self.max_rows} rows. Refine the query (e.g. add LIMIT or filters) for full data.]"
            return out
        except snowflake.connector.Error as e:
            return json.dumps({"error": str(e), "errno": getattr(e, "errno", None)}, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

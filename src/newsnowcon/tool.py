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

    Configuration can be provided either:
    1. As constructor parameters when instantiating the tool
    2. Via environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.)

    Environment variables are checked at runtime if parameters are not provided.
    """

    name: str = "snowflake_conn"
    description: str = (
        "Execute a SQL query in Snowflake and return the results. "
        "Provide the exact SQL string in the 'query' argument. "
        "Returns rows as JSON or an error message."
    )
    args_schema: Type[BaseModel] = SnowflakeConnInput

    # Connection parameters - all optional at init time, validated at runtime
    account: Optional[str] = Field(default=None, description="Snowflake account identifier (e.g. xy12345.us-east-1)")
    user: Optional[str] = Field(default=None, description="Snowflake user name")
    password: Optional[str] = Field(default=None, description="Snowflake password")
    database: Optional[str] = Field(default=None, description="Database to use")
    sf_schema: Optional[str] = Field(default=None, description="Schema to use")
    warehouse: Optional[str] = Field(default=None, description="Warehouse to use")
    role: Optional[str] = Field(default=None, description="Role to use (optional)")
    max_rows: int = Field(default=1000, description="Maximum rows to return (avoids oversized context).")

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        sf_schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        max_rows: int = 1000,
        **kwargs: Any,
    ):
        """
        Initialize the Snowflake connector tool.

        All connection parameters are optional at init time. They will be resolved
        from environment variables at runtime if not provided here.
        """
        super().__init__(
            account=account,
            user=user,
            password=password,
            database=database,
            sf_schema=sf_schema,
            warehouse=warehouse,
            role=role,
            max_rows=max_rows,
            **kwargs,
        )

    def _get_config(self) -> dict:
        """
        Resolve configuration from instance attributes or environment variables.
        Returns a dict with all config values and a list of any missing required fields.
        """
        config = {
            "account": self.account or os.environ.get(_ENV_ACCOUNT, ""),
            "user": self.user or os.environ.get(_ENV_USER, ""),
            "password": self.password or os.environ.get(_ENV_PASSWORD, ""),
            "database": self.database or os.environ.get(_ENV_DATABASE, ""),
            "schema": self.sf_schema or os.environ.get(_ENV_SCHEMA, ""),
            "warehouse": self.warehouse or os.environ.get(_ENV_WAREHOUSE, ""),
            "role": self.role or os.environ.get(_ENV_ROLE),
        }

        # Check for missing required fields
        required_fields = {
            "account": _ENV_ACCOUNT,
            "user": _ENV_USER,
            "database": _ENV_DATABASE,
            "schema": _ENV_SCHEMA,
            "warehouse": _ENV_WAREHOUSE,
        }

        missing = []
        for field, env_var in required_fields.items():
            if not config.get(field):
                missing.append(f"{field} (env: {env_var})")

        return config, missing

    def _run(self, query: str) -> str:
        """Execute the SQL query and return results as a JSON string."""
        if not query.strip():
            return json.dumps({"error": "Empty query"})

        # Resolve configuration at runtime
        config, missing = self._get_config()

        if missing:
            return json.dumps({
                "error": f"Missing required Snowflake configuration: {', '.join(missing)}",
                "hint": "Set these as environment variables or configure the tool parameters in CrewAI Studio.",
                "available_env_vars": {
                    "SNOWFLAKE_ACCOUNT": os.environ.get(_ENV_ACCOUNT, "<not set>"),
                    "SNOWFLAKE_USER": os.environ.get(_ENV_USER, "<not set>"),
                    "SNOWFLAKE_PASSWORD": "<hidden>" if os.environ.get(_ENV_PASSWORD) else "<not set>",
                    "SNOWFLAKE_DATABASE": os.environ.get(_ENV_DATABASE, "<not set>"),
                    "SNOWFLAKE_SCHEMA": os.environ.get(_ENV_SCHEMA, "<not set>"),
                    "SNOWFLAKE_WAREHOUSE": os.environ.get(_ENV_WAREHOUSE, "<not set>"),
                    "SNOWFLAKE_ROLE": os.environ.get(_ENV_ROLE, "<not set>"),
                }
            }, indent=2)

        conn = None
        try:
            conn = snowflake.connector.connect(
                account=config["account"],
                user=config["user"],
                password=config["password"],
                database=config["database"],
                schema=config["schema"],
                warehouse=config["warehouse"],
                role=config["role"],
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


# Alias for backward compatibility and CrewAI Studio
Newsnowcon = SnowflakeConn

from copy import deepcopy
from typing import Any

_SCHEMA_OPTIONS: dict[str, Any] = {
    "version": "2026-02-24.v1",
    "dialect": "postgres",
    "table_options": [
        {
            "id": "users_core",
            "description": "Core users table with identity and profile fields.",
            "table": {
                "name": "users",
                "columns": [
                    {
                        "name": "id",
                        "type": "uuid",
                        "nullable": False,
                        "primary_key": True,
                        "unique": True,
                        "default": "gen_random_uuid()",
                    },
                    {
                        "name": "email",
                        "type": "text",
                        "nullable": False,
                        "primary_key": False,
                        "unique": True,
                        "default": None,
                    },
                    {
                        "name": "full_name",
                        "type": "text",
                        "nullable": True,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "created_at",
                        "type": "timestamptz",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": "now()",
                    },
                ],
                "indexes": [
                    {"name": "users_email_idx", "columns": ["email"], "unique": True},
                    {"name": "users_created_at_idx", "columns": ["created_at"], "unique": False},
                ],
                "foreign_keys": [],
            },
            "relationships": [],
        },
        {
            "id": "datasets_core",
            "description": "Data collections owned by users.",
            "table": {
                "name": "datasets",
                "columns": [
                    {
                        "name": "id",
                        "type": "uuid",
                        "nullable": False,
                        "primary_key": True,
                        "unique": True,
                        "default": "gen_random_uuid()",
                    },
                    {
                        "name": "owner_user_id",
                        "type": "uuid",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "name",
                        "type": "text",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "source",
                        "type": "text",
                        "nullable": True,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "created_at",
                        "type": "timestamptz",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": "now()",
                    },
                ],
                "indexes": [
                    {"name": "datasets_owner_idx", "columns": ["owner_user_id"], "unique": False},
                    {"name": "datasets_name_idx", "columns": ["name"], "unique": False},
                ],
                "foreign_keys": [
                    {
                        "column": "owner_user_id",
                        "ref_table": "users",
                        "ref_column": "id",
                        "on_delete": "cascade",
                    }
                ],
            },
            "relationships": [{"from": "datasets.owner_user_id", "to": "users.id"}],
        },
        {
            "id": "analysis_runs_core",
            "description": "Tracks analysis jobs executed against datasets.",
            "table": {
                "name": "analysis_runs",
                "columns": [
                    {
                        "name": "id",
                        "type": "uuid",
                        "nullable": False,
                        "primary_key": True,
                        "unique": True,
                        "default": "gen_random_uuid()",
                    },
                    {
                        "name": "dataset_id",
                        "type": "uuid",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "status",
                        "type": "text",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": "'pending'",
                    },
                    {
                        "name": "input_payload",
                        "type": "jsonb",
                        "nullable": True,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "started_at",
                        "type": "timestamptz",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": "now()",
                    },
                ],
                "indexes": [
                    {"name": "analysis_runs_dataset_idx", "columns": ["dataset_id"], "unique": False},
                    {"name": "analysis_runs_status_idx", "columns": ["status"], "unique": False},
                ],
                "foreign_keys": [
                    {
                        "column": "dataset_id",
                        "ref_table": "datasets",
                        "ref_column": "id",
                        "on_delete": "cascade",
                    }
                ],
            },
            "relationships": [{"from": "analysis_runs.dataset_id", "to": "datasets.id"}],
        },
        {
            "id": "query_logs_core",
            "description": "Audits natural-language requests and generated SQL.",
            "table": {
                "name": "query_logs",
                "columns": [
                    {
                        "name": "id",
                        "type": "bigint",
                        "nullable": False,
                        "primary_key": True,
                        "unique": True,
                        "default": None,
                    },
                    {
                        "name": "user_id",
                        "type": "uuid",
                        "nullable": True,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "prompt_text",
                        "type": "text",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "generated_sql",
                        "type": "text",
                        "nullable": True,
                        "primary_key": False,
                        "unique": False,
                        "default": None,
                    },
                    {
                        "name": "created_at",
                        "type": "timestamptz",
                        "nullable": False,
                        "primary_key": False,
                        "unique": False,
                        "default": "now()",
                    },
                ],
                "indexes": [
                    {"name": "query_logs_created_at_idx", "columns": ["created_at"], "unique": False}
                ],
                "foreign_keys": [
                    {
                        "column": "user_id",
                        "ref_table": "users",
                        "ref_column": "id",
                        "on_delete": "set null",
                    }
                ],
            },
            "relationships": [{"from": "query_logs.user_id", "to": "users.id"}],
        },
    ],
}


def getSchemaOptions() -> dict[str, Any]:
    return deepcopy(_SCHEMA_OPTIONS)

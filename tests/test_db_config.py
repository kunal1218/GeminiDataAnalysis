import os
import unittest
from unittest.mock import patch

from app.db import _build_connect_args, _select_database_url, validate_database_config


class DatabaseConfigTests(unittest.TestCase):
    def test_prefers_database_public_url_over_database_url(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_PUBLIC_URL": (
                    "postgresql://postgres:public_pw@metro.proxy.rlwy.net:13993/railway"
                ),
                "DATABASE_URL": (
                    "postgresql://postgres:internal_pw@postgres.railway.internal:5432/railway"
                ),
            },
            clear=True,
        ):
            selected_key, selected_url = _select_database_url()

        self.assertEqual(selected_key, "DATABASE_PUBLIC_URL")
        self.assertIn("metro.proxy.rlwy.net", selected_url)

    def test_rejects_railway_internal_when_public_missing(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": (
                    "postgresql://postgres:internal_pw@postgres.railway.internal:5432/railway"
                )
            },
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as context:
                _select_database_url()

        message = str(context.exception)
        self.assertIn("DATABASE_PUBLIC_URL", message)
        self.assertIn("Railway", message)

    def test_pghost_does_not_override_priority(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_PUBLIC_URL": (
                    "postgresql://postgres:public_pw@metro.proxy.rlwy.net:13993/railway"
                ),
                "PGHOST": "postgres.railway.internal",
            },
            clear=True,
        ):
            selected_key, selected_url = _select_database_url()

        self.assertEqual(selected_key, "DATABASE_PUBLIC_URL")
        self.assertIn("metro.proxy.rlwy.net", selected_url)

    def test_logs_hostnames_without_leaking_secrets(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_PUBLIC_URL": (
                    "postgresql://postgres:supersecret@metro.proxy.rlwy.net:13993/railway"
                ),
                "DATABASE_URL": (
                    "postgresql://postgres:hidden1@postgres.railway.internal:5432/railway"
                ),
                "DATABASE_PRIVATE_URL": (
                    "postgresql://postgres:hidden2@private.proxy.rlwy.net:5432/railway"
                ),
                "PGHOST": "manual.db.host",
                "POSTGRES_HOST": "pg.host.example",
                "POSTGRES_URL": "postgresql://foo:hidden3@postgres.example.com:5432/railway",
                "POSTGRES_PRIVATE_URL": (
                    "postgresql://bar:hidden4@postgres.private.example:5432/railway"
                ),
            },
            clear=True,
        ):
            with self.assertLogs("app.db", level="INFO") as captured:
                validate_database_config()

        joined = "\n".join(captured.output)
        self.assertIn("DATABASE_PUBLIC_URL=metro.proxy.rlwy.net", joined)
        self.assertIn("DATABASE_URL=postgres.railway.internal", joined)
        self.assertIn("DATABASE_PRIVATE_URL=private.proxy.rlwy.net", joined)
        self.assertIn("PGHOST=manual.db.host", joined)
        self.assertIn("POSTGRES_HOST=pg.host.example", joined)
        self.assertIn("POSTGRES_URL=postgres.example.com", joined)
        self.assertIn("POSTGRES_PRIVATE_URL=postgres.private.example", joined)
        self.assertIn(
            "Database URL selected from DATABASE_PUBLIC_URL host=metro.proxy.rlwy.net port=13993",
            joined,
        )
        self.assertNotIn("supersecret", joined)
        self.assertNotIn("hidden1", joined)
        self.assertNotIn("hidden2", joined)
        self.assertNotIn("hidden3", joined)
        self.assertNotIn("hidden4", joined)
        self.assertNotIn("postgresql://", joined)

    def test_ssl_connect_args_applied_for_railway_public_url(self):
        with patch.dict(os.environ, {"DATABASE_SSL": "false"}, clear=True):
            connect_args = _build_connect_args(
                "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway"
            )

        self.assertEqual(connect_args, {"sslmode": "require"})


if __name__ == "__main__":
    unittest.main()

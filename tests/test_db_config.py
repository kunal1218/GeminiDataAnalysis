import os
import unittest
from unittest.mock import patch

from app.db import _build_connect_args, _select_database_url, validate_database_config


class DatabaseConfigTests(unittest.TestCase):
    def test_prefers_public_url_outside_railway(self):
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

    def test_accepts_database_url_public_alias(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL_PUBLIC": (
                    "postgresql://postgres:public_pw@metro.proxy.rlwy.net:13993/railway"
                ),
                "DATABASE_URL": (
                    "postgresql://postgres:internal_pw@postgres.railway.internal:5432/railway"
                ),
            },
            clear=True,
        ):
            selected_key, selected_url = _select_database_url()

        self.assertEqual(selected_key, "DATABASE_URL_PUBLIC")
        self.assertIn("metro.proxy.rlwy.net", selected_url)

    def test_prefers_database_url_on_railway(self):
        with patch.dict(
            os.environ,
            {
                "RAILWAY_PROJECT_ID": "proj_123",
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

        self.assertEqual(selected_key, "DATABASE_URL")
        self.assertIn("postgres.railway.internal", selected_url)

    def test_vercel_takes_public_url_even_with_railway_markers(self):
        with patch.dict(
            os.environ,
            {
                "VERCEL": "1",
                "RAILWAY_PROJECT_ID": "proj_123",
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

    def test_rejects_internal_host_when_not_on_railway_and_public_missing(self):
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
        self.assertIn("outside Railway runtime", message)
        self.assertIn("DATABASE_PUBLIC_URL", message)

    def test_logs_redacted_host_without_leaking_passwords(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_PUBLIC_URL": (
                    "postgresql://postgres:supersecret@metro.proxy.rlwy.net:13993/railway"
                ),
            },
            clear=True,
        ):
            with self.assertLogs("app.db", level="INFO") as captured:
                validate_database_config()

        joined = "\n".join(captured.output)
        self.assertIn("source=DATABASE_PUBLIC_URL", joined)
        self.assertIn("runtime=external", joined)
        self.assertIn("host=***.rlwy.net", joined)
        self.assertIn("port=13993", joined)
        self.assertNotIn("supersecret", joined)
        self.assertNotIn("postgresql://", joined)
        self.assertNotIn("metro.proxy.rlwy.net", joined)

    def test_ssl_connect_args_enforced_for_public_railway_url(self):
        with patch.dict(os.environ, {"DATABASE_SSL": "false"}, clear=True):
            connect_args = _build_connect_args(
                "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway"
            )
        self.assertEqual(connect_args, {"sslmode": "require"})

    def test_rejects_malformed_public_url(self):
        with patch.dict(
            os.environ,
            {"DATABASE_PUBLIC_URL": "postgresql://postgres:pw@:5432/railway"},
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as context:
                _select_database_url()
        self.assertIn("missing a hostname", str(context.exception))


if __name__ == "__main__":
    unittest.main()

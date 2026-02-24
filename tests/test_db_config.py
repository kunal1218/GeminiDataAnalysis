import os
import unittest
from unittest.mock import patch

from app.db import _build_connect_args, _get_database_url


class DatabaseConfigTests(unittest.TestCase):
    def test_prefers_public_url_when_database_url_is_internal(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": (
                    "postgresql://postgres:password@postgres.railway.internal:5432/railway"
                ),
                "DATABASE_URL_PUBLIC": (
                    "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway"
                ),
            },
            clear=True,
        ):
            resolved = _get_database_url()
        self.assertEqual(
            resolved,
            "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway",
        )

    def test_allows_database_public_url_alias_when_database_url_missing(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_PUBLIC_URL": (
                    "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway"
                )
            },
            clear=True,
        ):
            resolved = _get_database_url()
        self.assertEqual(
            resolved,
            "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway",
        )

    def test_raises_if_no_database_url_values_are_set(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as context:
                _get_database_url()
        self.assertIn("DATABASE_URL", str(context.exception))
        self.assertIn("DATABASE_URL_PUBLIC", str(context.exception))

    def test_ssl_defaults_to_require_for_public_railway_hosts(self):
        with patch.dict(os.environ, {}, clear=True):
            connect_args = _build_connect_args(
                "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway"
            )
        self.assertEqual(connect_args, {"sslmode": "require"})

    def test_ssl_can_be_disabled_explicitly(self):
        with patch.dict(os.environ, {"DATABASE_SSL": "false"}, clear=True):
            connect_args = _build_connect_args(
                "postgresql://postgres:password@metro.proxy.rlwy.net:13993/railway"
            )
        self.assertEqual(connect_args, {})

    def test_existing_sslmode_in_url_is_not_overridden(self):
        with patch.dict(os.environ, {"DATABASE_SSL": "true"}, clear=True):
            connect_args = _build_connect_args(
                "postgresql://postgres:password@host:5432/db?sslmode=disable"
            )
        self.assertEqual(connect_args, {})


if __name__ == "__main__":
    unittest.main()

"""
    python -m app.cli init-db

Creates tables for local development. Production uses Alembic (see the
inventory service for a fully worked migration setup).
"""

from __future__ import annotations

import sys

from . import create_app
from .extensions import db


def init_db() -> None:
    app = create_app()
    with app.app_context():
        from . import models  # noqa: F401 -- register models on the metadata

        db.create_all()
    print("tables created")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "init-db":
        init_db()
    else:
        print(__doc__)
        sys.exit(1)

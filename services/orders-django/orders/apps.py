from django.apps import AppConfig


class OrdersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "orders"

    def ready(self) -> None:
        """
        `ready()` runs once per process, after the app registry is populated.
        It is the correct place to connect signal handlers.

        It is the WRONG place for database queries. `ready()` runs during
        `manage.py migrate` too, before the tables it would query exist, and in
        every management command. A query here means `migrate` on an empty
        database fails with ProgrammingError — a bootstrap deadlock that is
        confusing the first time you meet it.
        """
        return None

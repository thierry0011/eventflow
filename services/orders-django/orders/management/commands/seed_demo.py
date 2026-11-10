"""Create the demo user used by scripts/smoke_test.sh."""

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Create a demo user (idempotent)"

    def handle(self, *args, **options):
        User = get_user_model()
        user, created = User.objects.get_or_create(
            username="demo", defaults={"email": "demo@example.com"},
        )
        if created:
            user.set_password("demo12345")
            # set_password HASHES. Assigning to `user.password` directly stores
            # the plaintext and every login then fails — a rite of passage bug.
            user.save()
        self.stdout.write(self.style.SUCCESS(
            f"demo user {'created' if created else 'already present'}"))

        admin, created = User.objects.get_or_create(
            username="admin", defaults={"email": "admin@example.com",
                                        "is_staff": True, "is_superuser": True},
        )
        if created:
            admin.set_password("admin12345")
            admin.save()
        self.stdout.write(self.style.SUCCESS("admin user ready (admin/admin12345)"))

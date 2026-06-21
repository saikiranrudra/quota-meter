import sys

from models import Quota, db  # Import your models directly from models

from app import app


def seed_database():
    with app.app_context():
        print("Truncating quotas table...")
        db.session.query(Quota).delete()
        db.session.commit()

        print("Seeding database...")
        msc_quota = [
            Quota(org_id="MSC", feature="container-tracking", default_limit=50),
            Quota(org_id="MSC", feature="sailing-schedule", default_limit=50),
        ]

        maersk_quota = [
            Quota(org_id="MAERSK", feature="container-tracking", default_limit=150),
            Quota(org_id="MAERSK", feature="sailing-schedule", default_limit=150),
        ]

        cosco_quota = [
            Quota(org_id="COSC", feature="container-tracking", default_limit=130),
            Quota(org_id="COSC", feature="sailing-schedule", default_limit=150),
        ]

        one_quota = [
            Quota(org_id="ONE", feature="container-tracking", default_limit=120),
            Quota(org_id="ONE", feature="sailing-schedule", default_limit=130),
        ]

        db.session.add_all(msc_quota)
        db.session.add_all(maersk_quota)
        db.session.add_all(cosco_quota)
        db.session.add_all(one_quota)

        db.session.commit()
        print("✅ Seed script executed successfully!")


if __name__ == "__main__":
    try:
        seed_database()
    except Exception as e:
        print(f"❌ Seed failed: {e}", file=sys.stderr)
        sys.exit(1)

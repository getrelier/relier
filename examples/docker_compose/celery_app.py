# Entry point for Celery:  celery -A celery_app worker / beat
#
# Importing tasks registers them with the Celery app and configures
# the beat_schedule — both worker and beat must import this module.
import tasks  # noqa: F401

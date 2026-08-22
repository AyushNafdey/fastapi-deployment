import os

DEFAULT_SCHEDULE_MINUTES = 1


def get_schedule_minutes() -> int:
    value = os.getenv("SCHEDULE_MINUTES", str(DEFAULT_SCHEDULE_MINUTES)).strip()
    try:
        return max(1, int(value))
        # return 1
    except ValueError:
        return DEFAULT_SCHEDULE_MINUTES

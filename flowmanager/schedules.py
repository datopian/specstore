import datetime


def parse_schedule(spec):
    if spec is None:
        return None, []
    schedule = spec.get('schedule')
    if schedule is None:
        return None, []
    if not isinstance(schedule, str):
        return None, ["Schedule should be a string"]
    schedule = schedule.strip()
    prefix = 'every '
    if not schedule.startswith(prefix):
        return None, ["Schedule should start with 'every'"]
    schedule = schedule[len(prefix):]
    multiplier = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400,
        'w': 7*86400
    }.get(schedule[-1])
    if multiplier is None:
        return None, ["Bad time unit for schedule, only s/m/h/d/w are allowed"]
    try:
        schedule = schedule[:-1]
        amount = int(schedule)
        amount *= multiplier
        if amount < 60:
            return None,["Can't schedule tasks for less than one minute"]
        return amount, []
    except ValueError:
        return None, ["Failed to parse time number"]


def calculate_new_schedule(scheduled_for, period_in_seconds, now):
    if period_in_seconds is None:
        return None
    else:
        if scheduled_for is None:
            return now + datetime.timedelta(seconds=period_in_seconds)
        else:
            if scheduled_for < now:
                diff = (now - scheduled_for).seconds
                diff = (diff // period_in_seconds) * period_in_seconds
                scheduled_for += datetime.timedelta(seconds=diff)
                while scheduled_for < now:
                    scheduled_for += datetime.timedelta(seconds=period_in_seconds)
            return scheduled_for

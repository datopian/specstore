import datetime

import pytest

from flowmanager.schedules import parse_schedule, calculate_new_schedule


@pytest.mark.parametrize(
    'schedule,period,success', [
        ('every 1s', None, False),
        ('every 60s', 60, True),
        ('every 3h', 3*3600, True),
        ('  every    4d   ', 4*86400, True),
        (' every 5w\n', 5*7*86400, True),
        (' every 6z', None, False),
        ('every 4.2w', None, False),
        ('fsdfds', None, False),
        (45454, None, False),
        (None, None, True)
    ]
)
def test_schedule_parse(schedule, period, success):
    spec = {'schedule': schedule}
    ret, errors = parse_schedule(spec)
    assert success == (len(errors) == 0)
    assert ret == period

@pytest.mark.parametrize(
    'spec', [{}, {'schedule': None}]
)
def test_schedule_bad_spec(spec):
    assert parse_schedule(spec) == (None, [])


@pytest.mark.parametrize(
    'current,period,expected', [
        (None, None, None),
        (100, None, None),
        (None, 60, 1060),
        (1001, 60, 1001),
        (999, 60, 1059),
        (10, 60, 1030),
    ]
)
def test_schedule_calculate(current, period, expected):
    now = datetime.datetime.fromtimestamp(1000)
    if current is not None:
        current = datetime.datetime.fromtimestamp(current)
    update = calculate_new_schedule(current, period, now)
    if update is None:
        assert expected is None
    else:
        assert expected == int(update.timestamp())

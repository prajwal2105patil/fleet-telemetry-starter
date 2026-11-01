"""The overspeed rule as a PURE function: no database, no Kafka.

Rows are (timestamp, speed_kmph) of ONE vehicle, newest ping first. The rule: strictly above the limit for at least
`min_pings` consecutive pings ending at the newest ping -> returns the timestamp of the first ping of that run.
"""
import datetime as dt

import pytest

from app.alerts import overspeed_run_start

T0 = dt.datetime(2026, 10, 28, 9, 0, 0, tzinfo=dt.timezone.utc)
LIMIT, MIN_PINGS = 100, 3


def pings(*speeds_oldest_first):
    """Builds newest-first rows from speeds listed in the order they happened (5 s apart)."""
    rows = [(T0 + dt.timedelta(seconds=5 * i), float(s)) for i, s in enumerate(speeds_oldest_first)]
    return list(reversed(rows)), [ts for ts, _ in rows]


def rule(*speeds):
    rows, times = pings(*speeds)
    return overspeed_run_start(rows, LIMIT, MIN_PINGS), times


def test_three_pings_above_the_limit_start_a_run_at_the_first_of_them():
    start, t = rule(60, 105, 112, 118)
    assert start == t[1]


def test_exactly_the_limit_is_not_speeding():
    start, _ = rule(100, 100, 100, 100)
    assert start is None


def test_a_hair_above_the_limit_is_speeding():
    start, t = rule(100.01, 100.01, 100.01)
    assert start == t[0]


def test_run_of_two_is_not_enough():
    start, _ = rule(60, 105, 112)
    assert start is None


def test_run_of_exactly_three_is_enough():
    start, t = rule(105, 112, 118)
    assert start == t[0]


def test_run_broken_by_a_slow_ping_starts_over():
    # 105, 112 | 90 | 120, 125: two pings on each side of the slow one -> neither side reaches three
    start, _ = rule(105, 112, 90, 120, 125)
    assert start is None


def test_a_ping_at_exactly_the_limit_also_breaks_the_run():
    start, _ = rule(105, 112, 100, 120, 125)
    assert start is None


def test_run_after_a_break_is_measured_from_after_the_break():
    start, t = rule(105, 112, 118, 90, 120, 125, 130)
    assert start == t[4]                                # the second run, not the first


def test_run_that_keeps_growing_keeps_the_same_start():
    """THE property that makes the alert unique: the 3rd, 4th, 5th ... ping all report the same start."""
    speeds = [60, 105, 112, 118, 121, 125]
    starts = {rule(*speeds[:n])[0] for n in range(4, len(speeds) + 1)}       # evaluate after each new ping
    assert len(starts) == 1 and None not in starts
    assert starts == {pings(*speeds)[1][1]}


def test_run_that_ended_before_the_newest_ping_does_not_count():
    start, _ = rule(105, 112, 118, 121, 60)             # speeding, then slowed down: nothing active right now
    assert start is None


def test_the_simulators_overspeed_scenario_gives_one_start():
    """simulate.py --mode overspeed sends 60, 105, 112, 118, 121, 125, 80."""
    speeds = [60, 105, 112, 118, 121, 125, 80]
    results = [rule(*speeds[:n])[0] for n in range(1, len(speeds) + 1)]
    starts = {r for r in results if r is not None}
    assert starts == {pings(*speeds)[1][1]}             # one distinct start = one alert (the 105 km/h ping)
    assert results[-1] is None                          # the final 80 km/h ping ends the run


def test_empty_and_single_inputs():
    assert overspeed_run_start([], LIMIT, MIN_PINGS) is None
    assert rule(150)[0] is None


@pytest.mark.parametrize("min_pings,expected_alert", [(1, True), (2, True), (3, True), (4, False)])
def test_min_pings_is_configurable(min_pings, expected_alert):
    rows, _ = pings(110, 120, 130)
    assert (overspeed_run_start(rows, LIMIT, min_pings) is not None) is expected_alert


def test_limit_is_configurable():
    rows, _ = pings(70, 75, 80)
    assert overspeed_run_start(rows, 60, 3) is not None
    assert overspeed_run_start(rows, 80, 3) is None

"""Unit tests for the open-issue bucket replay in gh-activity-report.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s .github/scripts -p 'test_*.py'
"""
import importlib.machinery
import importlib.util
import os
import sys
import unittest
from datetime import date, datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader("gh_activity_report", os.path.join(HERE, "gh-activity-report"))
_spec = importlib.util.spec_from_loader("gh_activity_report", _loader)
report = importlib.util.module_from_spec(_spec)
_loader.exec_module(report)

# Day boundaries are local midnights; pin the zone to UTC so the fixtures'
# timestamps mean what they say.
report.LOCAL_TZ = timezone.utc


def labeled(ts, name):
    return {"__typename": "LabeledEvent", "createdAt": ts, "label": {"name": name}}


def unlabeled(ts, name):
    return {"__typename": "UnlabeledEvent", "createdAt": ts, "label": {"name": name}}


def closed(ts):
    return {"__typename": "ClosedEvent", "createdAt": ts}


def reopened(ts):
    return {"__typename": "ReopenedEvent", "createdAt": ts}


def issue(created, events=(), closed_at=None, labels=()):
    return {"number": 1, "createdAt": created, "closedAt": closed_at, "labels": list(labels), "events": list(events)}


def counts(issues, *days):
    out = report.open_issue_buckets(issues, [date.fromisoformat(d) for d in days])
    return [tuple(out[date.fromisoformat(d)][b] for b in report.BUCKETS) for d in days]


class BucketFor(unittest.TestCase):
    def test_next_release_wins_over_roadmap_labels(self):
        self.assertEqual(report.bucket_for({"next release", "research", "low priority"}), "next_release")

    def test_low_priority_or_research_is_roadmap(self):
        self.assertEqual(report.bucket_for({"low priority"}), "roadmap")
        self.assertEqual(report.bucket_for({"research", "bug"}), "roadmap")

    def test_anything_else_is_other(self):
        self.assertEqual(report.bucket_for(set()), "other")
        self.assertEqual(report.bucket_for({"requires design decision"}), "other")


class OpenIssueBuckets(unittest.TestCase):
    def test_issue_counts_from_the_day_after_it_is_opened(self):
        i = issue("2026-10-01T09:00:00Z")
        self.assertEqual(counts([i], "2026-10-01", "2026-10-02"), [(0, 0, 0), (0, 0, 1)])

    def test_bucket_is_the_one_in_force_at_the_start_of_the_day(self):
        i = issue("2026-10-01T09:00:00Z", [
            labeled("2026-10-02T10:00:00Z", "Next Release"),
            unlabeled("2026-10-04T10:00:00Z", "Next Release"),
        ])
        self.assertEqual(
            counts([i], "2026-10-02", "2026-10-03", "2026-10-04", "2026-10-05"),
            [(0, 0, 1), (1, 0, 0), (1, 0, 0), (0, 0, 1)],
        )

    def test_label_added_at_creation_counts_from_the_first_snapshot(self):
        i = issue("2026-10-01T09:00:00Z", [labeled("2026-10-01T09:00:01Z", "Research")])
        self.assertEqual(counts([i], "2026-10-02"), [(0, 1, 0)])

    def test_next_release_added_on_top_of_roadmap_label_moves_it_back(self):
        i = issue("2026-10-01T00:00:00Z", [
            labeled("2026-10-01T00:00:00Z", "low priority"),
            labeled("2026-10-03T00:00:00Z", "Next Release"),
            unlabeled("2026-10-05T00:00:00Z", "Next Release"),
        ])
        self.assertEqual(
            counts([i], "2026-10-02", "2026-10-03", "2026-10-05"),
            [(0, 1, 0), (1, 0, 0), (0, 1, 0)],
        )

    def test_closed_issue_is_not_counted_and_a_reopened_one_is_again(self):
        i = issue("2026-10-01T00:00:00Z", [
            closed("2026-10-02T12:00:00Z"),
            reopened("2026-10-04T12:00:00Z"),
        ], labels=["Next Release"])
        self.assertEqual(
            counts([i], "2026-10-02", "2026-10-03", "2026-10-04", "2026-10-05"),
            [(0, 0, 1), (0, 0, 0), (0, 0, 0), (0, 0, 1)],
        )

    def test_labels_changed_while_closed_apply_on_reopen(self):
        i = issue("2026-10-01T00:00:00Z", [
            closed("2026-10-02T00:00:00Z"),
            labeled("2026-10-03T00:00:00Z", "Next Release"),
            reopened("2026-10-04T00:00:00Z"),
        ])
        self.assertEqual(counts([i], "2026-10-03", "2026-10-05"), [(0, 0, 0), (1, 0, 0)])

    def test_labels_match_case_insensitively_and_a_deleted_label_is_ignored(self):
        i = issue("2026-10-01T00:00:00Z", [
            labeled("2026-10-01T00:00:00Z", "NEXT RELEASE"),
            {"__typename": "LabeledEvent", "createdAt": "2026-10-01T00:00:01Z", "label": None},
        ])
        self.assertEqual(counts([i], "2026-10-02"), [(1, 0, 0)])

    def test_events_out_of_order_are_replayed_chronologically(self):
        i = issue("2026-10-01T00:00:00Z", [
            unlabeled("2026-10-03T12:00:00Z", "research"),
            labeled("2026-10-02T12:00:00Z", "research"),
        ])
        self.assertEqual(counts([i], "2026-10-03", "2026-10-04"), [(0, 1, 0), (0, 0, 1)])

    def test_an_event_exactly_at_midnight_belongs_to_the_new_day(self):
        i = issue("2026-10-01T00:00:00Z", [labeled("2026-10-03T00:00:00Z", "Next Release")])
        self.assertEqual(counts([i], "2026-10-02", "2026-10-03"), [(0, 0, 1), (1, 0, 0)])


class Aggregate(unittest.TestCase):
    def test_total_is_the_sum_of_the_buckets_and_the_series_reach_today(self):
        issues = [
            issue("2026-10-01T09:00:00Z", [labeled("2026-10-01T09:00:00Z", "Next Release")]),
            issue("2026-10-01T10:00:00Z", [labeled("2026-10-01T10:00:00Z", "Research")]),
            issue("2026-10-01T11:00:00Z"),
        ]
        days = report.aggregate(issues, [])
        by_date = {d["date"]: d for d in days}
        d = by_date["2026-10-02"]
        self.assertEqual((d["open_next_release"], d["open_roadmap"], d["open_other"], d["open_issues_total"]), (1, 1, 1, 3))
        self.assertEqual(days[-1]["date"], datetime.now(timezone.utc).date().isoformat())


class ReplayMismatches(unittest.TestCase):
    def test_flags_issues_whose_replay_disagrees_with_current_state(self):
        ok = issue("2026-10-01T00:00:00Z", [labeled("2026-10-01T00:00:00Z", "Research")], labels=["Research"])
        stale = issue("2026-10-01T00:00:00Z", [], labels=["Research"])
        stale["number"] = 2
        self.assertEqual(report.replay_mismatches([ok, stale]), [2])

    def test_closed_issue_matches_when_replay_ends_closed(self):
        i = issue("2026-10-01T00:00:00Z", [closed("2026-10-02T00:00:00Z")], closed_at="2026-10-02T00:00:00Z")
        self.assertEqual(report.replay_mismatches([i]), [])


if __name__ == "__main__":
    unittest.main()

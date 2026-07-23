import re

import pandas as pd
from unittest.mock import patch

from tfl import load as load_module


class FakeClickHouseClient:
    """
    In-memory stand-in for clickhouse_connect's client.

    Simulates DROP PARTITION as immediate (it's metadata-only in real
    ClickHouse, unlike ALTER ... DELETE which is an async mutation) — this
    checks that load_dataframe's code path is correct (drop-before-insert,
    correct partition id), not ClickHouse's own guarantees, which can only
    be observed against a real server.
    """

    def __init__(self):
        self.rows: list[dict] = []
        self.command_calls: list[str] = []
        self.insert_calls: list[tuple[str, int]] = []
        self.closed = False

    def command(self, sql, settings=None):
        self.command_calls.append(sql)
        match = re.search(r"DROP PARTITION '(\d+)'", sql)
        if match:
            partition_id = match.group(1)
            self.rows = [
                r for r in self.rows
                if str(r.get("extracted_date", "")).replace("-", "") != partition_id
            ]

    def insert_df(self, table, df):
        self.insert_calls.append((table, len(df)))
        self.rows.extend(df.to_dict(orient="records"))

    def close(self):
        self.closed = True


def make_bike_points_df(n: int, date: str = "2026-07-20") -> pd.DataFrame:
    return pd.DataFrame(
        [{"extracted_date": date, "station_id": i, "nb_bikes": 5} for i in range(n)]
    )


def test_load_dataframe_drops_partition_before_insert():
    """Guards against the async-mutation race: dropping the partition must
    happen (and complete) before insert_df runs, or a fresh insert can be
    wiped by a late-finishing mutation. DROP PARTITION is metadata-only and
    synchronous, unlike ALTER ... DELETE."""
    fake = FakeClickHouseClient()

    with patch.object(load_module, "get_client", return_value=fake):
        load_module.load_dataframe(
            make_bike_points_df(3), "bronze_bike_points", "2026-07-20"
        )

    assert len(fake.command_calls) == 1
    assert "DROP PARTITION '20260720'" in fake.command_calls[0]
    assert "bronze_bike_points" in fake.command_calls[0]
    assert len(fake.insert_calls) == 1
    assert len(fake.rows) == 3


def test_load_dataframe_idempotent_on_retry():
    """Simulates a backfill/retry: load the same date twice, confirm no
    duplication and no data loss — the failure mode the original bug produced."""
    fake = FakeClickHouseClient()
    df = make_bike_points_df(798, date="2026-07-20")

    with patch.object(load_module, "get_client", return_value=fake):
        load_module.load_dataframe(df, "bronze_bike_points", "2026-07-20")
        assert len(fake.rows) == 798

        load_module.load_dataframe(df, "bronze_bike_points", "2026-07-20")

    assert len(fake.rows) == 798


def test_load_dataframe_only_touches_matching_date():
    fake = FakeClickHouseClient()
    fake.rows = [{"extracted_date": "2026-07-19", "station_id": 1, "nb_bikes": 1}]

    with patch.object(load_module, "get_client", return_value=fake):
        load_module.load_dataframe(
            make_bike_points_df(2, date="2026-07-20"), "bronze_bike_points", "2026-07-20"
        )

    dates = {r["extracted_date"] for r in fake.rows}
    assert dates == {"2026-07-19", "2026-07-20"}
    assert len(fake.rows) == 3

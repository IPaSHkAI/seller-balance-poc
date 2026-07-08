"""
Синтетический генератор событий для Seller Balance Pipeline.

Генерирует события четырёх типов (order, payout, refund, fee) для заданного
числа продавцов за заданный период. Воспроизводим благодаря фиксированному seed.

Использование:
    python data_generator/generate_events.py --sellers 25 --days 30 --seed 42 \
        --out data/events.csv

    # с намеренным "грязным" довеском для проверки DQ (дубликаты, null, outliers)
    python data_generator/generate_events.py --sellers 25 --days 30 --seed 42 \
        --dirty-fraction 0.01 --out data/events.csv
"""
from __future__ import annotations

import argparse
import csv
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

EVENT_TYPES = ["order", "payout", "refund", "fee"]
CURRENCIES = ["USD"]
SOURCE_SYSTEMS = ["checkout", "payouts-service", "support-refunds", "billing"]

# Знак и типичный диапазон суммы по типу события
EVENT_PROFILE = {
    "order": {"direction": "in", "amount_range": (5.0, 500.0)},
    "payout": {"direction": "out", "amount_range": (50.0, 2000.0)},
    "refund": {"direction": "out", "amount_range": (5.0, 500.0)},
    "fee": {"direction": "out", "amount_range": (0.5, 25.0)},
}


@dataclass
class GenConfig:
    sellers: int
    days: int
    seed: int
    start_date: datetime
    events_per_seller_per_day: tuple[int, int]
    dirty_fraction: float


def _make_event(rng: random.Random, seller_id: str, ts: datetime) -> dict:
    event_type = rng.choice(EVENT_TYPES)
    profile = EVENT_PROFILE[event_type]
    amount = round(rng.uniform(*profile["amount_range"]), 2)
    direction = profile["direction"]
    signed_amount = amount if direction == "in" else -amount

    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "seller_id": seller_id,
        "event_type": event_type,
        "currency": rng.choice(CURRENCIES),
        "amount": f"{amount:.2f}",
        "signed_amount": f"{signed_amount:.2f}",
        "direction": direction,
        "source_system": rng.choice(SOURCE_SYSTEMS),
    }


def generate(cfg: GenConfig) -> list[dict]:
    rng = random.Random(cfg.seed)
    rows: list[dict] = []
    seller_ids = [f"S{100 + i}" for i in range(cfg.sellers)]

    for day_offset in range(cfg.days):
        day = cfg.start_date + timedelta(days=day_offset)
        for seller_id in seller_ids:
            n_events = rng.randint(*cfg.events_per_seller_per_day)
            for _ in range(n_events):
                ts = day + timedelta(
                    seconds=rng.randint(0, 24 * 3600 - 1)
                )
                rows.append(_make_event(rng, seller_id, ts))

    if cfg.dirty_fraction > 0:
        rows = _inject_dirty_rows(rng, rows, cfg.dirty_fraction)

    rows.sort(key=lambda r: (r["event_ts"], r["seller_id"]))
    return rows


def _inject_dirty_rows(rng: random.Random, rows: list[dict], fraction: float) -> list[dict]:
    """Намеренно портит часть строк, чтобы было на чём проверять DQ:
    дубликаты event_id, null в ключевых полях, аномально большие суммы."""
    n_dirty = max(1, int(len(rows) * fraction))
    dirty_rows = []

    for _ in range(n_dirty):
        kind = rng.choice(["duplicate", "null_seller", "huge_amount"])
        base = dict(rng.choice(rows))

        if kind == "duplicate":
            # Точная копия существующего event_id -> должно ловиться DQ-проверкой дублей
            dirty_rows.append(base)
        elif kind == "null_seller":
            base["event_id"] = str(uuid.uuid4())
            base["seller_id"] = ""
            dirty_rows.append(base)
        else:  # huge_amount
            base["event_id"] = str(uuid.uuid4())
            base["amount"] = "999999.99"
            base["signed_amount"] = "999999.99"
            dirty_rows.append(base)

    return rows + dirty_rows


def write_csv(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "event_id", "event_ts", "seller_id", "event_type",
        "currency", "amount", "signed_amount", "direction", "source_system",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sellers", type=int, default=25)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-date", type=str, default="2026-06-01")
    p.add_argument("--min-events-per-day", type=int, default=1)
    p.add_argument("--max-events-per-day", type=int, default=8)
    p.add_argument("--dirty-fraction", type=float, default=0.0,
                    help="Доля намеренно 'грязных' строк (0.01 = 1%%), по умолчанию 0 — чистые данные")
    p.add_argument("--out", type=str, default="data/events.csv")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = GenConfig(
        sellers=args.sellers,
        days=args.days,
        seed=args.seed,
        start_date=datetime.strptime(args.start_date, "%Y-%m-%d"),
        events_per_seller_per_day=(args.min_events_per_day, args.max_events_per_day),
        dirty_fraction=args.dirty_fraction,
    )
    rows = generate(cfg)
    out_path = Path(args.out)
    write_csv(rows, out_path)
    print(f"Сгенерировано {len(rows)} событий для {cfg.sellers} продавцов "
          f"за {cfg.days} дней -> {out_path}")


if __name__ == "__main__":
    main()

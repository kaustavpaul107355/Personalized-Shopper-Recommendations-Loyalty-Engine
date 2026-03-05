#!/usr/bin/env python3
"""
Generate mock retail data for the Hyper-Personalized Loyalty App.

Produces:
  - products_catalog.csv: Apparel product catalog
  - customer_profiles.csv: Customer profiles with loyalty tiers
  - clickstream_events.csv: 5,000 browsing events across apparel categories
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

# Output directory (relative to project root)
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
SEED = 42
random.seed(SEED)

# --- Apparel categories and sample data ---
CATEGORIES = [
    ("denim", ["Skinny Jeans", "Straight Leg", "High Rise", "Mom Jeans", "Wide Leg", "Boyfriend"]),
    ("tops", ["T-Shirt", "Blouse", "Henley", "Polo", "Crop Top", "Tank", "Button-Down"]),
    ("dresses", ["Maxi", "Midi", "Mini", "Wrap", "Shift", "Bodycon", "A-Line"]),
    ("outerwear", ["Jacket", "Coat", "Blazer", "Vest", "Trench", "Puffer"]),
    ("activewear", ["Leggings", "Sports Bra", "Running Shorts", "Hoodie", "Tank"]),
    ("accessories", ["Belt", "Scarf", "Hat", "Sunglasses", "Bag", "Jewelry"]),
    ("footwear", ["Sneakers", "Boots", "Sandals", "Heels", "Loafers", "Slides"]),
    ("intimates", ["Bra", "Underwear", "Loungewear", "Shapewear"]),
    ("swimwear", ["Bikini", "One-Piece", "Cover-Up", "Board Shorts"]),
]

BRANDS = ["Nordstrom Edit", "Zara Style", "H&M Fashion", "Levi's", "Nike", "Adidas", "Everlane", "Madewell", "Reformation"]

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "William", "Mia", "James", "Charlotte", "Benjamin", "Amelia",
    "Lucas", "Harper", "Henry", "Evelyn", "Alexander",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas",
]

LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
EVENT_TYPES = ["view", "click", "add_to_cart", "wishlist", "search"]


def generate_products_catalog(n_products: int = 200) -> List[dict]:
    """Generate product catalog with apparel categories."""
    products = []
    seen_names = set()
    pid = 1

    for category, subcategories in CATEGORIES:
        n_in_cat = max(5, n_products // len(CATEGORIES))
        for _ in range(n_in_cat):
            sub = random.choice(subcategories)
            brand = random.choice(BRANDS)
            base_name = f"{brand} {sub}"
            name = base_name
            c = 0
            while name in seen_names:
                c += 1
                name = f"{base_name} #{c}"
            seen_names.add(name)

            price = round(random.uniform(19.99, 299.99), 2)
            products.append({
                "product_id": f"p_{pid:05d}",
                "product_name": name,
                "category": category,
                "subcategory": sub,
                "brand": brand,
                "price": price,
            })
            pid += 1

    return products


def generate_customer_profiles(n_customers: int = 150) -> List[dict]:
    """Generate customer profiles with loyalty tiers and signup dates."""
    customers = []
    base_date = datetime(2022, 1, 1)

    for i in range(1, n_customers + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        signup = base_date + timedelta(days=random.randint(0, 700))
        tier = random.choices(
            LOYALTY_TIERS,
            weights=[0.35, 0.35, 0.2, 0.1]
        )[0]
        total_purchases = random.randint(0, 50)
        ltv = round(total_purchases * random.uniform(25, 150), 2)
        last_purchase = signup + timedelta(days=random.randint(0, 400)) if total_purchases > 0 else None

        customers.append({
            "customer_id": f"c_{i:05d}",
            "email": email,
            "first_name": first,
            "last_name": last,
            "signup_date": signup.strftime("%Y-%m-%d"),
            "loyalty_tier": tier,
            "total_purchases": total_purchases,
            "ltv": ltv,
            "last_purchase_date": last_purchase.strftime("%Y-%m-%d") if last_purchase else "",
        })

    return customers


def generate_clickstream(
    products: List[dict],
    customers: List[dict],
    n_events: int = 5000,
) -> List[dict]:
    """Generate 5,000 clickstream events with browsing patterns across apparel categories."""
    events = []
    customer_ids = [c["customer_id"] for c in customers]
    product_by_category = {}
    for p in products:
        product_by_category.setdefault(p["category"], []).append(p)

    customer_session_counters = {cid: 0 for cid in customer_ids}
    event_id = 1
    end_date = datetime.now()
    start_date = end_date - timedelta(days=14)

    category_weights = [1.2, 1.2, 1.0, 0.8, 0.9, 0.7, 0.8, 0.5, 0.6]
    assert len(category_weights) == len(CATEGORIES), (
        f"category_weights length ({len(category_weights)}) != CATEGORIES length ({len(CATEGORIES)})"
    )

    for _ in range(n_events):
        customer_id = random.choice(customer_ids)
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        event_type = random.choices(
            EVENT_TYPES,
            weights=[0.5, 0.25, 0.1, 0.1, 0.05]
        )[0]

        # Pick category (favor denim, tops, dresses for demo variety)
        category = random.choices(
            [c[0] for c in CATEGORIES],
            weights=category_weights,
        )[0]

        product = random.choice(product_by_category[category])

        # Session: scoped per customer so sessions don't span across customers
        session_id = f"s_{customer_id}_{customer_session_counters[customer_id]:04d}"
        if random.random() < 0.15:
            customer_session_counters[customer_id] += 1

        duration = random.randint(3, 120) if event_type == "view" else 0

        events.append({
            "event_id": f"e_{event_id:06d}",
            "customer_id": customer_id,
            "session_id": session_id,
            "event_type": event_type,
            "product_id": product["product_id"],
            "category": category,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration,
        })
        event_id += 1

    return events


def write_csv(path: Path, rows: List[dict], fieldnames: Optional[List[str]] = None):
    """Write rows to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    keys = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: List[dict]):
    """Write rows to JSON (one JSON object per line for easy streaming)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating products catalog...")
    products = generate_products_catalog()
    write_csv(OUTPUT_DIR / "products_catalog.csv", products)
    print(f"  -> {len(products)} products written to data/raw/products_catalog.csv")

    print("Generating customer profiles...")
    customers = generate_customer_profiles()
    write_csv(OUTPUT_DIR / "customer_profiles.csv", customers)
    print(f"  -> {len(customers)} customers written to data/raw/customer_profiles.csv")

    print("Generating clickstream (5,000 events)...")
    events = generate_clickstream(products, customers, n_events=5000)
    write_csv(OUTPUT_DIR / "clickstream_events.csv", events)
    write_json(OUTPUT_DIR / "clickstream_events.jsonl", events)
    print(f"  -> {len(events)} events written to data/raw/clickstream_events.csv and .jsonl")

    print("\nDone. Outputs:")
    for f in sorted(OUTPUT_DIR.glob("*")):
        size = f.stat().st_size / 1024
        print(f"  - {f.relative_to(OUTPUT_DIR.parent.parent)} ({size:.1f} KB)")


if __name__ == "__main__":
    main()

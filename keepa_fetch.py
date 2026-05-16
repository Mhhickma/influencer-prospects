import csv
import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import keepa
import numpy as np


def env_int(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return int(value)


def env_float(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return float(value)


def env_int_list(name, default=None):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default or []

    cleaned = []
    for item in str(value).replace(";", ",").split(","):
        item = item.strip()
        if item:
            cleaned.append(int(item))
    return cleaned


def env_str_list(name, default=None):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default or []

    cleaned = []
    for item in str(value).replace(";", ",").split(","):
        item = item.strip()
        if item:
            cleaned.append(item)
    return cleaned


DOMAIN = "US"
MAX_ASINS = env_int("MAX_ASINS", 100)
MIN_PRICE = env_float("MIN_PRICE", 50)
MAX_PRICE = env_float("MAX_PRICE", 100)
MIN_MONTHLY_REVENUE = env_float("MIN_MONTHLY_REVENUE", 10000)
MAX_TOTAL_VIDEOS = env_int("MAX_TOTAL_VIDEOS", 5)
MAX_INFLUENCER_VIDEOS = env_int("MAX_INFLUENCER_VIDEOS", 0)
LOOSE_MIN_PRICE = env_float("LOOSE_MIN_PRICE", 40)
LOOSE_MAX_PRICE = env_float("LOOSE_MAX_PRICE", 125)
LOOSE_MIN_MONTHLY_REVENUE = env_float("LOOSE_MIN_MONTHLY_REVENUE", 5000)
LOOSE_MAX_TOTAL_VIDEOS = env_int("LOOSE_MAX_TOTAL_VIDEOS", 10)
LOOSE_MAX_INFLUENCER_VIDEOS = env_int("LOOSE_MAX_INFLUENCER_VIDEOS", 2)
NEW_PRODUCT_DAYS = env_int("NEW_PRODUCT_DAYS", 90)
CREATOR_CONNECTIONS_DIR = os.getenv("CREATOR_CONNECTIONS_DIR", "creator-connections")
CAMPAIGN_ASINS_PER_ROW = env_int("CAMPAIGN_ASINS_PER_ROW", 1)
SCAN_HISTORY_FILE = "scan_history.json"
MAX_SCAN_HISTORY = env_int("MAX_SCAN_HISTORY", 20)
ASIN_RE = re.compile(r"\b[A-Z0-9]{10}\b")

# Amazon US root category IDs used by Keepa Product Finder.
DEFAULT_INCLUDED_CATEGORY_IDS = [
    228013,      # Tools & Home Improvement
    3375301,     # Sports & Outdoors
    261953301,   # Pet Supplies
    2972638011,  # Patio, Lawn & Garden
    1064954,     # Office Products
    16310091,    # Industrial & Scientific
    1055398,     # Home & Kitchen
    172282,      # Electronics
    2335752011,  # Cell Phones & Accessories
    2619525011,  # Appliances
    2102313011,  # Amazon Devices & Accessories
]

DEFAULT_EXCLUDED_CATEGORY_IDS = [
    283155,      # Books
    599858,      # Magazine Subscriptions
    7141123011,  # Clothing, Shoes & Jewelry
    16310101,    # Grocery & Gourmet Food
]

DEFAULT_EXCLUDED_BRANDS = [
    "Beats",
    "Apple",
    "Apple Store",
]

INCLUDED_CATEGORY_IDS = env_int_list("INCLUDED_CATEGORY_IDS", DEFAULT_INCLUDED_CATEGORY_IDS)
EXCLUDED_CATEGORY_IDS = env_int_list("EXCLUDED_CATEGORY_IDS", DEFAULT_EXCLUDED_CATEGORY_IDS)
EXCLUDED_BRANDS = env_str_list("EXCLUDED_BRANDS", DEFAULT_EXCLUDED_BRANDS)

KEEPA_API_KEY = os.environ["KEEPA_API_KEY"]

KEEPA_EPOCH = datetime(2011, 1, 1, tzinfo=timezone.utc)


def keepa_minutes_to_datetime(value):
    """Convert Keepa minutes since 2011-01-01 to a UTC datetime."""
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None

    if value <= 0:
        return None

    return KEEPA_EPOCH + timedelta(minutes=value)


def cents_to_dollars(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None

    if np.isnan(value) or value <= 0:
        return None

    return value / 100


def price_from_stats(product):
    """
    Prefer Keepa stats.current prices. Common Keepa indexes:
    AMAZON=0, NEW=1, BUY_BOX_SHIPPING=18.
    """
    current = (product.get("stats") or {}).get("current") or []

    for index, label in [(18, "BUY_BOX_SHIPPING"), (1, "NEW"), (0, "AMAZON")]:
        if len(current) <= index:
            continue

        price = cents_to_dollars(current[index])
        if price:
            print(f"  {product.get('asin', '?')} price from stats.current {label}: ${price:.2f}")
            return price

    return None


def price_from_data(product_data, price_keys=("BUY_BOX_SHIPPING", "NEW", "AMAZON")):
    """Fallback to Keepa data arrays if stats.current is unavailable."""
    for key in price_keys:
        arr = product_data.get(key)
        if arr is None or not hasattr(arr, "__len__") or len(arr) == 0:
            continue

        valid_prices = []
        for value in arr:
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue

            # Keepa arrays can include timestamps. Keep only realistic cent prices.
            if not np.isnan(value) and 0 < value < 1_000_000:
                valid_prices.append(value)

        if valid_prices:
            return valid_prices[-1] / 100

    return None


def get_current_price(product):
    return price_from_stats(product) or price_from_data(product.get("data", {}) or {})


def latest_positive_value(product_data, key, divisor=1, decimals=None):
    """Return latest positive numeric value from a Keepa data array."""
    arr = product_data.get(key)
    if arr is None or not hasattr(arr, "__len__") or len(arr) == 0:
        return None

    valid_values = []
    for value in arr:
        try:
            value = float(value)
        except (TypeError, ValueError):
            continue

        if not np.isnan(value) and value > 0:
            valid_values.append(value)

    if not valid_values:
        return None

    result = valid_values[-1] / divisor
    return round(result, decimals) if decimals is not None else result


def classify_videos(videos):
    """
    Keepa video entries use the 'creator' key.
    Common values include 'main' and 'influencer'.
    """
    main_count = 0
    influencer_count = 0
    other_count = 0

    for video in videos or []:
        if not isinstance(video, dict):
            continue

        creator = str(video.get("creator", "")).strip().lower()

        if creator == "influencer":
            influencer_count += 1
        elif creator == "main":
            main_count += 1
        else:
            other_count += 1

    return main_count, influencer_count, other_count


def get_official_video_count(product, fallback_count):
    """
    Prefer Keepa's official product-level videoCount field.
    This matches the Keepa page's 'Videos - Video Count' better than counting the returned videos list.
    """
    for key in ("videoCount", "videosCount"):
        value = product.get(key)
        if value is None:
            continue
        try:
            value = int(value)
        except (TypeError, ValueError):
            continue
        if value >= 0:
            return value
    return fallback_count


def get_sales_trend(product):
    trend_pct = product.get("deltaPercent90_monthlySold", 0) or 0
    if trend_pct > 10:
        trend = "Growing"
    elif trend_pct < -10:
        trend = "Declining"
    else:
        trend = "Stable"
    return trend, trend_pct


def get_image_url(product):
    images = product.get("imagesCSV") or product.get("images") or ""

    if isinstance(images, str):
        first_image = next((item.strip() for item in images.split(",") if item.strip()), "")
    elif isinstance(images, list):
        first_image = next((str(item).strip() for item in images if str(item).strip()), "")
    else:
        first_image = ""

    if not first_image:
        return ""

    if first_image.startswith("http://") or first_image.startswith("https://"):
        return first_image

    return f"https://m.media-amazon.com/images/I/{first_image}"


def normalize_filter_value(value):
    return str(value or "").strip().lower()


def is_excluded_category(product):
    root_category = product.get("rootCategory")
    categories = product.get("categories") or []
    category_ids = {root_category, *categories}
    return any(category_id in EXCLUDED_CATEGORY_IDS for category_id in category_ids)


def is_excluded_brand(product):
    ignored = {normalize_filter_value(brand) for brand in EXCLUDED_BRANDS}
    brand_values = [
        product.get("brand", ""),
        product.get("brandStoreUrlName", ""),
        product.get("manufacturer", ""),
    ]
    return any(normalize_filter_value(value) in ignored for value in brand_values)


def get_ideal_misses(product_summary):
    misses = []

    if not product_summary["creator_connection"]:
        misses.append("no Creator Campaign")
    if product_summary["main_video_count"] < 1:
        misses.append("no brand video")
    if product_summary["influencer_count"] > MAX_INFLUENCER_VIDEOS:
        misses.append("has influencer videos")
    if product_summary["video_count"] > MAX_TOTAL_VIDEOS:
        misses.append("over 5 videos")
    if product_summary["buybox_price"] <= MIN_PRICE or product_summary["buybox_price"] > MAX_PRICE:
        misses.append("outside $50-$100")
    if product_summary["monthly_revenue"] < MIN_MONTHLY_REVENUE:
        misses.append("under $10k/mo")

    return misses


def record_skip(skip_counts, reason):
    skip_counts[reason] += 1


def score_prospect(prospect):
    score = 0

    if prospect.get("creator_connection"):
        score += 25
    if prospect.get("main_video_count", 0) >= 1:
        score += 20
    if prospect.get("influencer_count", 0) == 0:
        score += 15
    elif prospect.get("influencer_count", 0) <= 2:
        score += 7
    if prospect.get("video_count", 0) <= MAX_TOTAL_VIDEOS:
        score += 10

    revenue = prospect.get("monthly_revenue", 0) or 0
    score += min(15, int((revenue / max(MIN_MONTHLY_REVENUE, 1)) * 10))

    price = prospect.get("buybox_price", 0) or 0
    if MIN_PRICE < price <= MAX_PRICE:
        score += 10
    elif LOOSE_MIN_PRICE < price <= LOOSE_MAX_PRICE:
        score += 4

    trend = prospect.get("sales_trend")
    if trend == "Growing":
        score += 5
    elif trend == "Stable":
        score += 3

    return min(score, 100)


def update_scan_history(output, tokens_used):
    entry = {
        "last_updated": output["last_updated"],
        "total": output["total"],
        "total_ideal": output.get("total_ideal", 0),
        "total_loose": output.get("total_loose", 0),
        "creator_connection_matches": output.get("creator_connection_matches", 0),
        "creator_connection_rows_scanned": output.get("creator_connection_rows_scanned", 0),
        "skip_counts": output.get("skip_counts", {}),
        "tokens_used": tokens_used,
    }

    try:
        with open(SCAN_HISTORY_FILE, "r", encoding="utf-8") as handle:
            history = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    history = [entry] + [item for item in history if item.get("last_updated") != entry["last_updated"]]
    history = history[:MAX_SCAN_HISTORY]

    with open(SCAN_HISTORY_FILE, "w", encoding="utf-8") as handle:
        json.dump(history, handle, indent=2)


def parse_campaign_date(value):
    try:
        return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def campaign_from_row(row, now_utc):
    start_date = parse_campaign_date(row.get("Campaign Start Date", ""))
    end_date = parse_campaign_date(row.get("Campaign End Date", ""))
    today = now_utc.date()
    is_active = (start_date is None or start_date <= today) and (end_date is None or today <= end_date)

    return {
        "campaign_id": row.get("Campaign Id", ""),
        "campaign_name": row.get("Campaign Name", ""),
        "campaign_brand": row.get("Brand Name", ""),
        "commission_rate": row.get("Commission Rate", ""),
        "campaign_start_date": row.get("Campaign Start Date", ""),
        "campaign_end_date": row.get("Campaign End Date", ""),
        "recommended": str(row.get("Recommended", "")).lower() == "true",
        "active": is_active,
    }


def select_creator_campaign_asins(max_asins, now_utc):
    folder = Path(CREATOR_CONNECTIONS_DIR)
    if not folder.exists():
        print(f"Creator Connections folder not found: {folder}")
        return [], {}, 0, 0

    selected_asins = []
    campaign_by_asin = {}
    rows_scanned = 0
    files_scanned = 0

    passes = [
        ("recommended active", lambda campaign: campaign["recommended"] and campaign["active"]),
        ("active", lambda campaign: campaign["active"]),
        ("any", lambda campaign: True),
    ]

    for pass_label, campaign_filter in passes:
        for csv_file in sorted(folder.glob("*.csv")):
            files_scanned += 1
            with csv_file.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    rows_scanned += 1
                    campaign = campaign_from_row(row, now_utc)
                    if not campaign_filter(campaign):
                        continue

                    row_added = 0
                    for asin in ASIN_RE.findall(row.get("ASIN List", "")):
                        if asin in campaign_by_asin:
                            continue

                        campaign_by_asin[asin] = campaign
                        selected_asins.append(asin)
                        row_added += 1

                        if len(selected_asins) >= max_asins:
                            print(
                                f"Creator Connections: selected {len(selected_asins)} {pass_label} ASINs "
                                f"after scanning {files_scanned} file passes / {rows_scanned} rows"
                            )
                            return selected_asins, campaign_by_asin, files_scanned, rows_scanned

                        if row_added >= CAMPAIGN_ASINS_PER_ROW:
                            break

    print(
        f"Creator Connections: selected {len(selected_asins)} ASINs "
        f"after scanning {files_scanned} file passes / {rows_scanned} rows"
    )
    return selected_asins, campaign_by_asin, files_scanned, rows_scanned


def get_creator_connections_last_updated():
    folder = Path(CREATOR_CONNECTIONS_DIR)
    if not folder.exists():
        return ""

    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        return ""

    newest_file = max(csv_files, key=lambda path: path.stat().st_mtime)
    newest_dt = datetime.fromtimestamp(newest_file.stat().st_mtime, tz=timezone.utc)
    return newest_dt.strftime("%Y-%m-%d %H:%M UTC")


def main():
    api = keepa.Keepa(KEEPA_API_KEY)

    api.update_status()
    starting_tokens = api.tokens_left
    print(f"Available tokens: {starting_tokens}")
    print(f"MAX_ASINS: {MAX_ASINS}")
    print(f"Price range: over ${MIN_PRICE:.2f} and up to ${MAX_PRICE:.2f}")
    print(f"Max total videos: {MAX_TOTAL_VIDEOS}")
    print(f"Max influencer videos: {MAX_INFLUENCER_VIDEOS}")
    print(f"Minimum monthly revenue: ${MIN_MONTHLY_REVENUE:,.2f}")
    print(
        "Loose section: "
        f"${LOOSE_MIN_PRICE:.2f}-${LOOSE_MAX_PRICE:.2f}, "
        f"${LOOSE_MIN_MONTHLY_REVENUE:,.2f}+ revenue, "
        f"{LOOSE_MAX_TOTAL_VIDEOS} videos max, "
        f"{LOOSE_MAX_INFLUENCER_VIDEOS} influencer videos max"
    )
    print(f"New product window: {NEW_PRODUCT_DAYS} days")
    print(f"Included category IDs: {INCLUDED_CATEGORY_IDS}")
    print(f"Excluded category IDs: {EXCLUDED_CATEGORY_IDS}")
    print(f"Excluded brands: {EXCLUDED_BRANDS}")
    print(f"Creator Connections folder: {CREATOR_CONNECTIONS_DIR}")
    print("Brand video required: True")

    min_price_cents = int(MIN_PRICE * 100) + 1
    max_price_cents = int(MAX_PRICE * 100)
    now_utc = datetime.now(timezone.utc)
    skip_counts = Counter()

    print("Reading Creator Connections campaign ASINs...")
    creator_connections_last_updated = get_creator_connections_last_updated()
    asins, creator_connection_matches, cc_files_scanned, cc_rows_scanned = select_creator_campaign_asins(
        MAX_ASINS,
        now_utc,
    )
    print(f"Selected {len(asins)} Creator Connections ASINs for Keepa")

    if not asins:
        output = {
            "last_updated": now_utc.strftime("%Y-%m-%d %H:%M UTC"),
            "total": 0,
            "total_ideal": 0,
            "total_loose": 0,
            "scan_source": "creator_connections",
            "included_category_ids": INCLUDED_CATEGORY_IDS,
            "excluded_category_ids": EXCLUDED_CATEGORY_IDS,
            "excluded_brands": EXCLUDED_BRANDS,
            "brand_video_required": True,
            "creator_connections_last_updated": creator_connections_last_updated,
            "creator_connection_files_scanned": cc_files_scanned,
            "creator_connection_rows_scanned": cc_rows_scanned,
            "creator_connection_matches": 0,
            "skip_counts": {},
            "loose_criteria": {
                "min_price": LOOSE_MIN_PRICE,
                "max_price": LOOSE_MAX_PRICE,
                "min_monthly_revenue": LOOSE_MIN_MONTHLY_REVENUE,
                "max_total_videos": LOOSE_MAX_TOTAL_VIDEOS,
                "max_influencer_videos": LOOSE_MAX_INFLUENCER_VIDEOS,
            },
            "prospects": [],
        }
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        update_scan_history(output, 0)
        print("No ASINs found. Saved empty data.json.")
        return

    print("Querying full Keepa product data...")
    products = api.query(asins, history=True, videos=True, aplus=True, stats=90, domain=DOMAIN) or []

    keepa_data = {}

    for product in products:
        asin = product.get("asin", "?")

        try:
            videos = product.get("videos") or []
            main_count, influencer_count, other_video_count = classify_videos(videos)
            counted_video_total = main_count + influencer_count + other_video_count
            official_video_count = get_official_video_count(product, counted_video_total)

            if official_video_count > LOOSE_MAX_TOTAL_VIDEOS:
                record_skip(skip_counts, "too_many_videos")
                print(f"Skipping {asin} - official videoCount is {official_video_count}")
                continue

            if main_count < 1:
                record_skip(skip_counts, "missing_brand_video")
                print(f"Skipping {asin} - no creator: Main video found")
                continue

            if influencer_count > LOOSE_MAX_INFLUENCER_VIDEOS:
                record_skip(skip_counts, "too_many_influencer_videos")
                print(f"Skipping {asin} - {influencer_count} influencer videos")
                continue

            product_data = product.get("data", {}) or {}
            buybox_price = get_current_price(product)

            if buybox_price is None:
                record_skip(skip_counts, "missing_price")
                print(f"Skipping {asin} - price missing")
                print(f"  stats.current sample: {(product.get('stats') or {}).get('current', [])[:20]}")
                print(f"  product videoCount field: {product.get('videoCount')}")
                print(f"  counted video total: {counted_video_total}")
                continue

            if buybox_price <= LOOSE_MIN_PRICE or buybox_price > LOOSE_MAX_PRICE:
                record_skip(skip_counts, "price_outside_loose_range")
                print(f"Skipping {asin} - price ${buybox_price:.2f} outside loose range")
                continue

            monthly_units = product.get("monthlySold", 0) or 0
            monthly_revenue = buybox_price * monthly_units

            if monthly_revenue < LOOSE_MIN_MONTHLY_REVENUE:
                record_skip(skip_counts, "monthly_revenue_too_low")
                print(f"Skipping {asin} - monthly revenue ${monthly_revenue:,.2f}")
                continue

            sales_trend, trend_pct = get_sales_trend(product)

            drops_90 = product.get("salesRankDrops90", 0) or 0
            drops_30 = product.get("salesRankDrops30", 0) or 0
            accelerating = bool(drops_30 > (drops_90 * 0.4)) if drops_90 > 0 else False

            rating = latest_positive_value(product_data, "RATING", divisor=10, decimals=1)
            review_count = latest_positive_value(product_data, "COUNT_REVIEWS")

            listed_since_raw = product.get("listedSince", None)
            listed_since_dt = keepa_minutes_to_datetime(listed_since_raw)
            listed_since_iso = listed_since_dt.strftime("%Y-%m-%d") if listed_since_dt else ""
            age_days = (now_utc - listed_since_dt).days if listed_since_dt else None
            is_new_90 = age_days is not None and age_days <= NEW_PRODUCT_DAYS

            brand = product.get("brand", "") or ""
            brand_store_name = product.get("brandStoreUrlName", "") or ""
            root_category = product.get("rootCategory")
            categories = product.get("categories") or []

            if is_excluded_category(product):
                record_skip(skip_counts, "excluded_category")
                print(f"Skipping {asin} - excluded category {root_category}")
                continue

            if is_excluded_brand(product):
                record_skip(skip_counts, "excluded_brand")
                print(f"Skipping {asin} - excluded brand {brand or brand_store_name}")
                continue

            creator_campaign = creator_connection_matches.get(asin)
            product_summary = {
                "creator_connection": creator_campaign is not None,
                "buybox_price": buybox_price,
                "monthly_revenue": monthly_revenue,
                "video_count": official_video_count,
                "main_video_count": main_count,
                "influencer_count": influencer_count,
            }
            missed_ideal_reasons = get_ideal_misses(product_summary)
            opportunity_tier = "ideal" if not missed_ideal_reasons else "loose"

            prospect = {
                "asin": asin,
                "title": product.get("title", "") or "",
                "brand": brand,
                "brand_store_url": f"https://www.amazon.com/stores/{brand_store_name}" if brand_store_name else "",
                "amazon_url": f"https://www.amazon.com/dp/{asin}",
                "image_url": get_image_url(product),
                "buybox_price": round(buybox_price, 2),
                "monthly_units": monthly_units,
                "monthly_revenue": round(monthly_revenue, 2),
                "rating": rating,
                "review_count": int(review_count) if review_count else None,
                "video_count": official_video_count,
                "counted_video_total": counted_video_total,
                "main_video_count": main_count,
                "influencer_count": influencer_count,
                "other_video_count": other_video_count,
                "sales_trend": sales_trend,
                "sales_trend_pct": trend_pct,
                "sales_rank_drops_90": drops_90,
                "sales_rank_drops_30": drops_30,
                "daily_sales": round(drops_90 / 90) if drops_90 else 0,
                "accelerating": accelerating,
                "has_aplus": product.get("hasAPlus", False),
                "has_aplus_from_manufacturer": product.get("hasAPlusFromManufacturer", False),
                "listed_since": listed_since_raw,
                "listed_since_iso": listed_since_iso,
                "age_days": age_days,
                "is_new_90": is_new_90,
                "root_category": root_category,
                "categories": categories,
                "creator_connection": creator_campaign is not None,
                "creator_connection_campaign": creator_campaign,
                "opportunity_tier": opportunity_tier,
                "is_ideal": opportunity_tier == "ideal",
                "missed_ideal_reasons": missed_ideal_reasons,
            }
            prospect["opportunity_score"] = score_prospect(prospect)
            keepa_data[asin] = prospect

        except Exception as exc:
            record_skip(skip_counts, "error")
            print(f"Skipping {asin}: {exc}")
            continue

    print(f"\nKeepa filtered to {len(keepa_data)} prospects")

    results = list(keepa_data.values())
    sorted_results = sorted(
        results,
        key=lambda x: (
            0 if x.get("is_ideal") else 1,
            0 if x.get("creator_connection") else 1,
            0 if x.get("is_new_90") else 1,
            x.get("age_days") if x.get("age_days") is not None else 999999,
            -x.get("opportunity_score", 0),
            -x.get("monthly_revenue", 0),
        ),
    )
    tokens_used = starting_tokens - api.tokens_left
    output = {
        "last_updated": now_utc.strftime("%Y-%m-%d %H:%M UTC"),
        "total": len(results),
        "total_ideal": sum(1 for item in results if item.get("is_ideal")),
        "total_loose": sum(1 for item in results if not item.get("is_ideal")),
        "scan_source": "creator_connections",
        "included_category_ids": INCLUDED_CATEGORY_IDS,
        "excluded_category_ids": EXCLUDED_CATEGORY_IDS,
        "excluded_brands": EXCLUDED_BRANDS,
        "brand_video_required": True,
        "creator_connections_last_updated": creator_connections_last_updated,
        "creator_connection_files_scanned": cc_files_scanned,
        "creator_connection_rows_scanned": cc_rows_scanned,
        "creator_connection_matches": len(creator_connection_matches),
        "keepa_products_returned": len(products),
        "skip_counts": dict(skip_counts),
        "loose_criteria": {
            "min_price": LOOSE_MIN_PRICE,
            "max_price": LOOSE_MAX_PRICE,
            "min_monthly_revenue": LOOSE_MIN_MONTHLY_REVENUE,
            "max_total_videos": LOOSE_MAX_TOTAL_VIDEOS,
            "max_influencer_videos": LOOSE_MAX_INFLUENCER_VIDEOS,
        },
        "prospects": sorted_results,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    update_scan_history(output, tokens_used)

    print(f"\nSaved {len(results)} prospects to data.json")
    print(f"Tokens used: {tokens_used} | Remaining: {api.tokens_left}")


if __name__ == "__main__":
    main()

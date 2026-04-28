import json
import os
from datetime import datetime, timezone

import keepa
import numpy as np
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource


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


DOMAIN = "US"
MAX_ASINS = env_int("MAX_ASINS", 8)  # Use 8 for testing. Set repo variable MAX_ASINS=40 for production.
MIN_PRICE = env_float("MIN_PRICE", 25)
MAX_PRICE = env_float("MAX_PRICE", 100)
MIN_MONTHLY_REVENUE = env_float("MIN_MONTHLY_REVENUE", 5000)
MAX_INFLUENCER_VIDEOS = env_int("MAX_INFLUENCER_VIDEOS", 5)

KEEPA_API_KEY = os.environ["KEEPA_API_KEY"]
CREDENTIAL_ID = os.environ["CREATORS_CREDENTIAL_ID"]
CREDENTIAL_SECRET = os.environ["CREATORS_CREDENTIAL_SECRET"]
PARTNER_TAG = os.getenv("AFFILIATE_TAG") or "influencer-20"


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


def get_sales_trend(product):
    trend_pct = product.get("deltaPercent90_monthlySold", 0) or 0
    if trend_pct > 10:
        trend = "Growing"
    elif trend_pct < -10:
        trend = "Declining"
    else:
        trend = "Stable"
    return trend, trend_pct


def fetch_amazon_images(keepa_data):
    """Use Amazon Creators API images so the dashboard avoids blocked direct CDN URLs."""
    if not keepa_data:
        return

    print("Fetching images from Amazon Creators API...")

    try:
        amazon = AmazonCreatorsApi(
            credential_id=CREDENTIAL_ID,
            credential_secret=CREDENTIAL_SECRET,
            version="3.1",
            tag=PARTNER_TAG,
            country=Country.US,
        )

        resources = [
            GetItemsResource.IMAGES_DOT_PRIMARY_DOT_LARGE,
            GetItemsResource.ITEM_INFO_DOT_TITLE,
        ]

        asin_list = list(keepa_data.keys())

        for i in range(0, len(asin_list), 10):
            batch = asin_list[i:i + 10]
            print(f"  Batch {i // 10 + 1}: {len(batch)} ASINs...")

            try:
                items = amazon.get_items(batch, resources=resources)

                for item in items:
                    asin = getattr(item, "asin", None)
                    if asin not in keepa_data:
                        continue

                    try:
                        image_url = item.images.primary.large.url
                    except Exception:
                        image_url = ""

                    if image_url:
                        keepa_data[asin]["image_url"] = image_url
                        print(f"    Got image for {asin}")

            except Exception as exc:
                print(f"  Batch failed: {exc}")

    except Exception as exc:
        print(f"Amazon Creators API error: {exc}")


def main():
    api = keepa.Keepa(KEEPA_API_KEY)

    api.update_status()
    starting_tokens = api.tokens_left
    print(f"Available tokens: {starting_tokens}")
    print(f"MAX_ASINS: {MAX_ASINS}")
    print(f"Price range: over ${MIN_PRICE:.2f} and up to ${MAX_PRICE:.2f}")

    min_price_cents = int(MIN_PRICE * 100) + 1
    max_price_cents = int(MAX_PRICE * 100)

    # Keepa Product Finder does the broad video/product screen.
    # The Python post-filter below separates Main videos from Influencer videos.
    product_params = {
        "hasMainVideo": True,
        "videoCount_gte": 1,
        "videoCount_lte": 5,
        "current_RATING_gte": 40,
        "monthlySold_gte": 10,
        "current_BUY_BOX_SHIPPING_gte": min_price_cents,
        "current_BUY_BOX_SHIPPING_lte": max_price_cents,
        "sort": [["monthlySold", "desc"]],
    }

    print("Querying Keepa product finder...")
    asins = api.product_finder(product_params, n_products=MAX_ASINS, domain=DOMAIN) or []
    asins = asins[:MAX_ASINS]
    print(f"Found {len(asins)} ASINs")

    if not asins:
        output = {
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "total": 0,
            "prospects": [],
        }
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print("No ASINs found. Saved empty data.json.")
        return

    print("Querying full Keepa product data...")
    products = api.query(asins, history=True, videos=True, stats=90, domain=DOMAIN) or []

    keepa_data = {}

    for product in products:
        asin = product.get("asin", "?")

        try:
            videos = product.get("videos") or []
            main_count, influencer_count, other_video_count = classify_videos(videos)

            if main_count < 1:
                print(f"Skipping {asin} - no creator: Main video found")
                continue

            if influencer_count > MAX_INFLUENCER_VIDEOS:
                print(f"Skipping {asin} - {influencer_count} influencer videos")
                continue

            product_data = product.get("data", {}) or {}
            buybox_price = get_current_price(product)

            if buybox_price is None:
                print(f"Skipping {asin} - price missing")
                print(f"  stats.current sample: {(product.get('stats') or {}).get('current', [])[:20]}")
                print(f"  data keys sample: {list(product_data.keys())[:20]}")
                continue

            if buybox_price <= MIN_PRICE or buybox_price > MAX_PRICE:
                print(f"Skipping {asin} - price ${buybox_price:.2f} outside range")
                continue

            monthly_units = product.get("monthlySold", 0) or 0
            monthly_revenue = buybox_price * monthly_units

            if monthly_revenue < MIN_MONTHLY_REVENUE:
                print(f"Skipping {asin} - monthly revenue ${monthly_revenue:,.2f}")
                continue

            sales_trend, trend_pct = get_sales_trend(product)

            drops_90 = product.get("salesRankDrops90", 0) or 0
            drops_30 = product.get("salesRankDrops30", 0) or 0
            accelerating = bool(drops_30 > (drops_90 * 0.4)) if drops_90 > 0 else False

            rating = latest_positive_value(product_data, "RATING", divisor=10, decimals=1)
            review_count = latest_positive_value(product_data, "COUNT_REVIEWS")

            brand = product.get("brand", "") or ""
            brand_store_name = product.get("brandStoreUrlName", "") or ""

            keepa_data[asin] = {
                "asin": asin,
                "title": product.get("title", "") or "",
                "brand": brand,
                "brand_store_url": f"https://www.amazon.com/stores/{brand_store_name}" if brand_store_name else "",
                "amazon_url": f"https://www.amazon.com/dp/{asin}",
                "image_url": "",
                "buybox_price": round(buybox_price, 2),
                "monthly_units": monthly_units,
                "monthly_revenue": round(monthly_revenue, 2),
                "rating": rating,
                "review_count": int(review_count) if review_count else None,
                "video_count": main_count + influencer_count + other_video_count,
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
                "listed_since": product.get("listedSince", None),
            }

        except Exception as exc:
            print(f"Skipping {asin}: {exc}")
            continue

    print(f"\nKeepa filtered to {len(keepa_data)} prospects")

    fetch_amazon_images(keepa_data)

    results = list(keepa_data.values())
    output = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "total": len(results),
        "prospects": sorted(results, key=lambda x: x["monthly_revenue"], reverse=True),
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    tokens_used = starting_tokens - api.tokens_left
    print(f"\nSaved {len(results)} prospects to data.json")
    print(f"Tokens used: {tokens_used} | Remaining: {api.tokens_left}")


if __name__ == "__main__":
    main()

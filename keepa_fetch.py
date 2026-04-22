import keepa
import json
import os
import numpy as np
from datetime import datetime
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource

api = keepa.Keepa(os.environ["KEEPA_API_KEY"])

CREDENTIAL_ID     = os.environ["CREATORS_CREDENTIAL_ID"]
CREDENTIAL_SECRET = os.environ["CREATORS_CREDENTIAL_SECRET"]
PARTNER_TAG       = os.getenv("AFFILIATE_TAG", "influencer-20")

api.update_status()
available_tokens = api.tokens_left
print(f"Available tokens: {available_tokens}")

MAX_ASINS = 8

product_parms = {
        "hasMainVideo": True,
        "current_RATING_gte": 40,
        "monthlySold_gte": 10,
        "current_BUY_BOX_SHIPPING_gte": 2000,
        "current_BUY_BOX_SHIPPING_lte": 6000,
        "sort": [["monthlySold", "desc"]],
}

print("Querying Keepa product finder...")
asins = api.product_finder(product_parms, n_products=MAX_ASINS)
print(f"Found {len(asins)} ASINs")
asins = asins[:MAX_ASINS]

products = api.query(asins, history=True, videos=True, stats=90)

keepa_data = {}
for p in products:
        try:
                    videos = p.get("videos") or []

            # Debug: print video counts
                    influencer_count = sum(1 for v in videos if isinstance(v, dict) and str(v.get("creator", "")).lower() == "influencer")
                    main_count = sum(1 for v in videos if isinstance(v, dict) and str(v.get("creator", "")).lower() == "main")
                    print(f"ASIN {p.get('asin')}: main={main_count} influencer={influencer_count} total={len(videos)}")

            if main_count == 0:
                            print(f"  -> Skip: no main video")
                            continue

        if influencer_count > 5:
                        print(f"  -> Skip: {influencer_count} influencer videos")
                        continue

        data = p.get("data", {})
        bb_price = None
        for key in ["BUY_BOX_SHIPPING", "NEW", "AMAZON"]:
                        arr = data.get(key)
                        if arr is not None and hasattr(arr, "__len__") and len(arr) > 0:
                                            valid = [float(x) for x in arr if x is not None and not np.isnan(float(x)) and float(x) > 0]
                                            if valid:
                                                                    bb_price = valid[-1] / 100
                                                                    break

                                    if bb_price is None or bb_price < 5:
                                                    print(f"  -> Skip: no valid price (bb_price={bb_price})")
                                                    continue

        monthly_units = p.get("monthlySold", 0) or 0
        monthly_revenue = bb_price * monthly_units
        print(f"  -> Price=${bb_price} units={monthly_units} revenue=${monthly_revenue}")

        if monthly_revenue < 5000:
                        print(f"  -> Skip: revenue too low")
            continue

        trend_pct = p.get("deltaPercent90_monthlySold", 0) or 0
        sales_trend = "Growing" if trend_pct > 10 else "Declining" if trend_pct < -10 else "Stable"

        drops_90 = p.get("salesRankDrops90", 0) or 0
        drops_30 = p.get("salesRankDrops30", 0) or 0
        accelerating = bool(drops_30 > (drops_90 * 0.4)) if drops_90 > 0 else False

        rating = None
        rating_arr = data.get("RATING")
        if rating_arr is not None and len(rating_arr) > 0:
                        valid_r = [float(x) for x in rating_arr if x is not None and not np.isnan(float(x)) and float(x) > 0]
            if valid_r:
                                rating = round(valid_r[-1] / 10, 1)

        review_count = None
        review_arr = data.get("COUNT_REVIEWS")
        if review_arr is not None and len(review_arr) > 0:
                        valid_rv = [int(x) for x in review_arr if x is not None and not np.isnan(float(x)) and float(x) > 0]
            if valid_rv:
                                review_count = valid_rv[-1]

        print(f"  -> ADDED to prospects!")
        keepa_data[p["asin"]] = {
                        "asin": p["asin"],
                        "title": p.get("title", ""),
                        "brand": p.get("brand", ""),
                        "brand_store_url": f"https://www.amazon.com/stores/{p.get('brandStoreUrlName', '')}",
                        "amazon_url": f"https://www.amazon.com/dp/{p['asin']}",
                        "image_url": "",
                        "buybox_price": round(bb_price, 2),
                        "monthly_units": monthly_units,
                        "monthly_revenue": round(monthly_revenue, 2),
                        "rating": rating,
                        "review_count": review_count,
                        "video_count": main_count,
                        "influencer_count": influencer_count,
                        "sales_trend": sales_trend,
                        "sales_trend_pct": trend_pct,
                        "sales_rank_drops_90": drops_90,
                        "sales_rank_drops_30": drops_30,
                        "daily_sales": round(drops_90 / 90) if drops_90 else 0,
                        "accelerating": accelerating,
                        "has_aplus": p.get("hasAPlus", False),
                        "listed_since": p.get("listedSince", None),
        }

except Exception as e:
        print(f"Skipping {p.get('asin', '?')}: {e}")
        continue

print(f"\nKeepa filtered to {len(keepa_data)} prospects")

if keepa_data:
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
                        batch = asin_list[i:i+10]
            print(f"  Batch {i//10+1}: {len(batch)} ASINs...")
            try:
                                items = amazon.get_items(batch, resources=resources)
                                for item in items:
                                                        asin = item.asin
                                                        if asin in keepa_data:
                                                                                    try:
                                                                                                                    img = item.images.primary.large.url
                                                                                                                    if img:
                                                                                                                                                        keepa_data[asin]["image_url"] = img
                                                                                                                                                        print(f"    Got image for {asin}")
                                                                                                                                                except:
                                                                                                                    pass
            except Exception as e:
                print(f"  Batch failed: {e}")
except Exception as e:
        print(f"Amazon Creators API error: {e}")

results = list(keepa_data.values())

output = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "total": len(results),
        "prospects": sorted(results, key=lambda x: x["monthly_revenue"], reverse=True)
}

with open("data.json", "w") as f:
        json.dump(output, f, indent=2)

tokens_used = available_tokens - api.tokens_left
print(f"\nSaved {len(results)} prospects to data.json")
print(f"Tokens used: {tokens_used} | Remaining: {api.tokens_left}")

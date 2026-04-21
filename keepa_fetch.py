import keepa
import json
import os
import numpy as np
from datetime import datetime

api = keepa.Keepa(os.environ["KEEPA_API_KEY"])

product_parms = {
    "hasMainVideo": True,
    "videoCount_lte": 1,
    "current_RATING_gte": 40,
    "monthlySold_gte": 10,
    "sort": [["monthlySold", "desc"]],
}

print("Querying Keepa product finder...")
asins = api.product_finder(product_parms, n_products=500)
print(f"Found {len(asins)} ASINs")

products = api.query(asins, history=True, videos=True, stats=90)

results = []
for p in products:
    try:
        data = p.get("data", {})

        bb_price = None
        for key in ["BUY_BOX_SHIPPING", "NEW", "AMAZON"]:
            arr = data.get(key)
            if arr is not None and hasattr(arr, "__len__") and len(arr) > 0:
                valid = [float(x) for x in arr if x is not None and not np.isnan(float(x)) and float(x) > 0]
                if valid:
                    bb_price = valid[-1] / 100
                    break

        if bb_price is None:
            continue

        monthly_units = p.get("monthlySold", 0) or 0
        monthly_revenue = bb_price * monthly_units

        if monthly_revenue < 1:
            continue

        images = p.get("imagesCSV", "")
        first_image = images.split(",")[0] if images else ""
        image_url = f"https://images-na.ssl-images-amazon.com/images/I/{first_image}" if first_image else ""

        trend_pct = p.get("deltaPercent90_monthlySold", 0) or 0
        if trend_pct > 10:
            sales_trend = "Growing"
        elif trend_pct < -10:
            sales_trend = "Declining"
        else:
            sales_trend = "Stable"

        drops_90 = p.get("salesRankDrops90", 0) or 0
        drops_30 = p.get("salesRankDrops30", 0) or 0
        accelerating = bool(drops_30 > (drops_90 * 0.4)) if drops_90 > 0 else False
        daily_sales = round(drops_90 / 90) if drops_90 else 0

        rating = None
        rating_arr = data.get("RATING")
        if rating_arr is not None and len(rating_arr) > 0:
            valid_r = [float(x) for x in rating_arr if x is not None and not np.isnan(float(x)) and float(x) > 0]
            if valid_r:
                rating = valid_r[-1] / 10

        review_count = None
        review_arr = data.get("COUNT_REVIEWS")
        if review_arr is not None and len(review_arr) > 0:
            valid_rv = [int(x) for x in review_arr if x is not None and not np.isnan(float(x)) and float(x) > 0]
            if valid_rv:
                review_count = valid_rv[-1]

        results.append({
            "asin": p["asin"],
            "title": p.get("title", ""),
            "brand": p.get("brand", ""),
            "brand_store_url": f"https://www.amazon.com/stores/{p.get('brandStoreUrlName', '')}",
            "amazon_url": f"https://www.amazon.com/dp/{p['asin']}",
            "image_url": image_url,
            "buybox_price": round(bb_price, 2),
            "monthly_units": monthly_units,
            "monthly_revenue": round(monthly_revenue, 2),
            "rating": rating,
            "review_count": review_count,
            "video_count": p.get("videoCount", 0),
            "sales_trend": sales_trend,
            "sales_trend_pct": trend_pct,
            "sales_rank_drops_90": drops_90,
            "sales_rank_drops_30": drops_30,
            "daily_sales": daily_sales,
            "accelerating": accelerating,
            "has_aplus": p.get("hasAPlus", False),
            "listed_since": p.get("listedSince", None),
        })

    except Exception as e:
        print(f"Skipping {p.get('asin', '?')}: {e}")
        continue

output = {
    "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    "total": len(results),
    "prospects": sorted(results, key=lambda x: x["monthly_revenue"], reverse=True)
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"Saved {len(results)} prospects to data.json")

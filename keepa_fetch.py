import keepa
import json
import os
from datetime import datetime

api = keepa.Keepa(os.environ["KEEPA_API_KEY"])

product_parms = {
      "hasMainVideo": True,
      "videoCount_lte": 1,
      "current_RATING_gte": 40,
      "monthlySold_gte": 10,
      "sort": ["monthlySold", "desc"],
}

print("Querying Keepa product finder...")
asins = api.product_finder(product_parms, n_products=500)
print(f"Found {len(asins)} ASINs")

products = api.query(asins, history=False, videos=True, stats=90)

results = []
for p in products:
      try:
                bb_price = p["data"]["BUY_BOX_SHIPPING"][-1] / 100
                monthly_units = p.get("monthlySold", 0)
                monthly_revenue = bb_price * monthly_units

          if monthly_revenue < 5000:
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
        accelerating = drops_30 > (drops_90 * 0.4) if drops_90 > 0 else False
        daily_sales = round(drops_90 / 90) if drops_90 else 0

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
                      "rating": p["data"]["RATING"][-1] / 10 if p["data"].get("RATING") else None,
                      "review_count": int(p["data"]["COUNT_REVIEWS"][-1]) if p["data"].get("COUNT_REVIEWS") else None,
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

except (KeyError, IndexError, TypeError) as e:
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

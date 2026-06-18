import os

os.environ.setdefault("RELIER_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("RELIER_SECRET_KEY", "dev-secret-change-in-production")

from celery import chord, group
from tasks import aggregate_results, process_image

IMAGE_IDS = [f"img_{i:03d}" for i in range(8)]

if __name__ == "__main__":
    # --- fan-out: process all images in parallel ---
    job = group(
        process_image.s(image_id=img_id, operation="resize") for img_id in IMAGE_IDS
    )
    result = job.delay()
    results = result.get(timeout=60)
    print(f"processed {len(results)} images")
    for r in results:
        print(f"  {r}")

    print()

    # --- chord: fan-out + collect into a single aggregation step ---
    pipeline = chord(
        group(
            process_image.s(image_id=img_id, operation=op)
            for img_id in IMAGE_IDS[:4]
            for op in ("resize", "watermark")
        ),
        aggregate_results.s(),
    )
    summary = pipeline.delay().get(timeout=60)
    print(f"summary: {summary}")

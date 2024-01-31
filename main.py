import base64
import functions_framework
import pandas as pd
from google_play_scraper import Sort, reviews
from google.cloud import storage
from datetime import datetime

storage_client = storage.Client(project='pdm-class')

def scrap_data(apps_ids):
    app_reviews = []

    # Removed tqdm for clarity, assuming direct iteration
    for ap in apps_ids:
        for score in range(1, 6):
            rvs, _ = reviews(
                ap,
                lang='pt',
                country='br',
                sort=Sort.NEWEST,
                filter_score_with=score
            )
            for r in rvs:
                r['sortOrder'] = 'newest'
                r['appId'] = ap
            app_reviews.extend(rvs)

    return app_reviews

@functions_framework.cloud_event
def daily_scrapper(cloud_event):
    # Assuming cloud_event.data is correctly structured and encoded
    print(base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8'))

    apps_ids = ['com.shopee.br', 'com.mercadolibre', 'com.luizalabs.mlapp', 'com.b2w.americanas', 'com.amazon.mShop.android.shopping']
    app_reviews = scrap_data(apps_ids)

    app_reviews_df = pd.DataFrame(app_reviews)
    app_reviews_df['sentiment'] = app_reviews_df['score'].apply(lambda rating: ['Muito Negativo', 'Negativo', 'Neutro', 'Positivo', 'Muito Positivo'][rating - 1])

    storage_bucket = storage_client.get_bucket('bronze-review')
    data = datetime.now().strftime("%d-%m-%Y")
    csv_content = app_reviews_df.to_csv(index=False)
    storage_bucket.blob(f'{data}_reviews.csv').upload_from_string(csv_content, content_type='text/csv')
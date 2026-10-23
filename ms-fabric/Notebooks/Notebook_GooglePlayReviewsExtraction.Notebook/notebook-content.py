# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ff3c4e99-0ed0-44d7-a225-e3d08e1d73b6",
# META       "default_lakehouse_name": "Lakehouse_CustomerSatisfaction",
# META       "default_lakehouse_workspace_id": "45871885-07df-4095-b5ae-108f2abcd427",
# META       "known_lakehouses": [
# META         {
# META           "id": "ff3c4e99-0ed0-44d7-a225-e3d08e1d73b6"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "15a629a0-f6d9-884a-494f-98286c97eb0f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Google Play Reviews Extraction

# MARKDOWN ********************

# ## 00 Importing packages

# CELL ********************

from google_play_scraper import reviews, Sort
from datetime import datetime, timedelta
from delta.tables import *
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 01 Defining global variables and functions

# MARKDOWN ********************

# ### 01-1 Global variables

# CELL ********************

APP_IDS = ["com.tiket.gits", "com.traveloka.android", "com.agoda.mobile.consumer"]
CUTOFF = datetime.now() - timedelta(days=365)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 01-2 Functions

# MARKDOWN ********************

# #### > get_reviews

# CELL ********************

def get_reviews(app_ids, cutoff):
    """Getting reviews from various apps in Google Play"""
    
    results = []
    for app_id in app_ids:

        print(f"=== Scraping [{app_id}] before {CUTOFF} ===")
        token = None
        complete = False
        index = 0
        review_count = 0

        while not complete:
            batch, token = reviews(
                app_id=app_id,
                lang="id",
                country="id",
                sort=Sort.NEWEST,
                count=1000,
                continuation_token=token
            )

            token_value = True if getattr(token, "token", None) is not None else False
            print(f"    ==> [{app_id}] Token exists for batch {index}: {token_value}")
            print(f"    ==> [{app_id}] Reviews in batch {index}: {len(batch)}")

            if not batch:
                break

            for review in batch:
                if review["at"] < CUTOFF:
                    complete = True
                    break
                review.setdefault("appId", app_id)
                results.append(review)
                review_count += 1

            if not token_value:
                break

            index += 1
            time.sleep(3)
    
        print(f"==> [{app_id}] Total reviews: {review_count}\n")

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### > load_into_table

# CELL ********************

def load_into_table(df_source, target_table, candidate_key):
    try:
        deltaTable = DeltaTable.forName(spark, target_table)
    except Exception:
        try:
            df_source.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(f"{target_table}")
        except Exception as e:
            print(f":Load for table {target_table} failed with error: {str(e)}")
            raise
        return
    
    try:
        match_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in candidate_key])

        target_columns = deltaTable.toDF().columns
        new_columns = [col for col in df_source.columns if col not in candidate_key and col not in target_columns]

        if new_columns:
            backfill_expressions = {col: f"source.`{col}`" for col in new_columns}
            deltaTable.alias("target").merge(
                source=df_source.alias("source"),
                condition=match_condition
            ).whenMatchedUpdate(
                set=backfill_expressions
            ).withSchemaEvolution().execute()

            deltaTable = DeltaTable.forName(spark, target_table)

        change_detection_columns = [col for col in df_source.columns if col not in candidate_key]
        update_condition = " OR ".join([f"target.`{col}` != source.`{col}`" for col in change_detection_columns])
        update_expressions = {col: f"source.`{col}`" for col in df_source.columns}

        deltaTable.alias("target").merge(
            source=df_source.alias("source"),
            condition=match_condition
        ).whenMatchedUpdate(
            condition=update_condition,
            set=update_expressions
        ).whenNotMatchedInsertAll().withSchemaEvolution().execute()
        
    except Exception as e:
        print(f"Insert operation for table {target_table} failed with error: {str(e)}")
    return

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 02 Getting reviews from Google Play

# CELL ********************

results = get_reviews(app_ids=APP_IDS, cutoff=CUTOFF)

print(f"Total final reviews: {len(results)}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 03 Converting list data type to spark DataFrame

# CELL ********************

df = spark.createDataFrame(results)

display(df.limit(5))
display(df.printSchema())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 04 Load data into Lakehouse_CustomerSatisfaction.bronze.google_play_reviews table

# CELL ********************

load_into_table(
    df_source=df,
    target_table="Lakehouse_CustomerSatisfaction.bronze.google_play_reviews",
    candidate_key=["reviewId"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Environment variables
s3_cleansed = os.environ["s3_cleansed_layer"]
write_mode = os.environ["write_data_operation"]


def lambda_handler(event, context):
    print("✅ Lambda triggered")

    try:
        # Get bucket and key from event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        print(f"📦 Bucket: {bucket}")
        print(f"📄 Key: {key}")

        # Read JSON file from S3
        s3_path = f"s3://{bucket}/{key}"
        df = wr.s3.read_json(path=s3_path)
        print("🟢 Successfully read JSON from S3")
        print(df.head())

        # Write DataFrame to Parquet
        output_path = f"{s3_cleansed}"
        print(f"💾 Writing to {output_path}")

        result = wr.s3.to_parquet(
            df=df,
            path=output_path,
            dataset=True,
            mode=write_mode
        )

        print("✅ Parquet written and Glue table updated")
        return {"status": "success", "details": result}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise


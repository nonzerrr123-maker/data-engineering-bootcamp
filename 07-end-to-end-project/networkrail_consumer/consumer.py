import configparser
import json
import os
from datetime import datetime
from pathlib import Path
from time import sleep

from google.cloud import storage
from google.oauth2 import service_account
from kafka import KafkaConsumer


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
DATA_DIR = HERE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

parser = configparser.ConfigParser()
config_path = HERE / "confluent.conf"
if not config_path.exists():
    raise FileNotFoundError(f"Kafka config not found: {config_path}")

parser.read(config_path)

confluent_bootstrap_servers = parser.get("config", "confluent_bootstrap_servers")
confluent_key = parser.get("config", "confluent_key")
confluent_secret = parser.get("config", "confluent_secret")

GCP_PROJECT_ID = os.getenv(
    "GCP_PROJECT_ID",
    "project-b9bafacf-46f9-43ef-bcc",
)
BUCKET_NAME = os.getenv(
    "GCS_BUCKET",
    "deb-bootcamp-005-non",
)
BUSINESS_DOMAIN = "networkrail"
TOPIC = "networkrail-train-movements"
CONSUMER_GROUP = os.getenv(
    "NETWORKRAIL_CONSUMER_GROUP",
    "deb-bootcamp-005-non",
)


def resolve_keyfile_path() -> Path:
    """Resolve the local service-account JSON without hardcoding one location."""
    configured = os.getenv("KEYFILE_PATH")
    if configured:
        path = Path(configured).expanduser()
        if path.is_file():
            return path
        raise FileNotFoundError(
            f"KEYFILE_PATH points to a file that does not exist: {path}"
        )

    preferred_names = [
        "project-b9bafacf-46f9-43ef-bcc-b4cd6476cc84.json",
        "project-b9bafacf-46f9-43ef-bcc-6ca8073c5513.json",
    ]
    preferred_dirs = [
        HERE,
        REPO_ROOT / "00-bootcamp-project" / "pyspark",
        REPO_ROOT / "07-end-to-end-project" / "pyspark",
    ]

    for directory in preferred_dirs:
        for name in preferred_names:
            candidate = directory / name
            if candidate.is_file():
                return candidate

    # Final fallback: find any service-account JSON for this GCP project.
    matches = sorted(
        REPO_ROOT.rglob("project-b9bafacf-46f9-43ef-bcc-*.json")
    )
    if matches:
        return matches[0]

    raise FileNotFoundError(
        "Google Cloud service-account JSON was not found. "
        "Put the JSON file in networkrail_consumer/, "
        "00-bootcamp-project/pyspark/, or 07-end-to-end-project/pyspark/, "
        "or set KEYFILE_PATH to its full path."
    )


KEYFILE_PATH = resolve_keyfile_path()
print(f"Using GCP keyfile: {KEYFILE_PATH}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=confluent_bootstrap_servers,
    sasl_mechanism="PLAIN",
    security_protocol="SASL_SSL",
    sasl_plain_username=confluent_key,
    sasl_plain_password=confluent_secret,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
)


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    with KEYFILE_PATH.open(encoding="utf-8") as keyfile:
        service_account_info = json.load(keyfile)

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


try:
    for message in consumer:
        try:
            data = json.loads(message.value.decode("utf-8"))
            print(data)

            train_id = data["train_id"]
            now = datetime.now()
            file_name = f"{train_id}-{int(now.timestamp())}.json"
            source_file_name = DATA_DIR / file_name

            with source_file_name.open("w", encoding="utf-8") as f:
                json.dump(data, f)

            # Store raw Network Rail data by ingestion date, as required by Week 7.
            ingestion_date = now.strftime("%Y-%m-%d")
            destination_folder = f"{BUSINESS_DOMAIN}/raw/{ingestion_date}"

            upload_to_gcs(
                bucket_name=BUCKET_NAME,
                source_file_name=str(source_file_name),
                destination_blob_name=f"{destination_folder}/{file_name}",
            )

            print(
                f"Uploaded gs://{BUCKET_NAME}/{destination_folder}/{file_name}"
            )
            sleep(3)

        except json.decoder.JSONDecodeError:
            pass

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

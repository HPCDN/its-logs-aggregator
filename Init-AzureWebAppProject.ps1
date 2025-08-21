$projectName = "azure-web-app-aggregator"
$root = Join-Path -Path (Get-Location) -ChildPath $projectName

# Create folder structure
New-Item -ItemType Directory -Path $root -Force | Out-Null
New-Item -ItemType Directory -Path "$root\.github\workflows" -Force | Out-Null

# app.py
@'
from flask import Flask, request, jsonify
import os
from azure.storage.blob import BlobServiceClient

app = Flask(__name__)
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

@app.route("/")
def index():
    return "Azure Web App is running."

@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

@app.route("/aggregate", methods=["POST"])
def aggregate():
    data = request.get_json()
    source_container = data["SourceContainer"]
    target_container = data["TragetContainer"]
    source_files = data["SourceFiles"]
    msg_to_aggregate = set(data["MsgToAggregate"])

    processed = created = updated = 0
    created_paths = []
    updated_paths = []

    for file_path in source_files:
        file_name = file_path.split("/")[-1]
        if file_name not in msg_to_aggregate:
            continue

        processed += 1
        date_path = "/".join(file_path.split("/")[1:4])
        target_blob_path = f"{date_path}/{file_name}"

        source_blob = blob_service_client.get_blob_client(source_container, file_path)
        target_blob = blob_service_client.get_blob_client(target_container, target_blob_path)

        new_data = source_blob.download_blob().readall()

        try:
            existing_data = target_blob.download_blob().readall()
            combined = existing_data + b"\\n" + new_data
            target_blob.upload_blob(combined, overwrite=True)
            updated += 1
            updated_paths.append(target_blob_path)
        except:
            target_blob.upload_blob(new_data)
            created += 1
            created_paths.append(target_blob_path)

    return jsonify({
        "files_processed": processed,
        "files_created": created,
        "files_updated": updated,
        "created_paths": created_paths,
        "updated_paths": updated_paths
    })

if __name__ == "__main__":
    app.run(debug=True)
'@ | Set-Content "$root\app.py"

# requirements.txt
@"
flask
azure-storage-blob
"@ | Set-Content "$root\requirements.txt"

# .env
@"
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=...
"@ | Set-Content "$root\.env"

# startup.txt
@"
gunicorn --bind=0.0.0.0 --timeout 600 app:app
"@ | Set-Content "$root\startup.txt"

# Dockerfile
@'
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["gunicorn", "--bind", "0.0.0.0:80", "app:app"]
'@ | Set-Content "$root\Dockerfile"

# README.md
@"
# Azure Web App Aggregator

A lightweight Flask web app to aggregate JSON files in Azure Blob Storage using Logic App-triggered POST requests.
"@ | Set-Content "$root\README.md"

# GitHub Actions workflow
@'
name: Deploy Azure Web App

on:
  push:
    branches:
      - main

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Deploy to Azure Web App
        uses: azure/webapps-deploy@v2
        with:
          app-name: ${{ secrets.AZURE_WEBAPP_NAME }}
          slot-name: production
          publish-profile: ${{ secrets.AZURE_WEBAPP_PUBLISH_PROFILE }}
          package: .
'@ | Set-Content "$root\\.github\\workflows\\deploy.yml"

Write-Host "✅ Project '$projectName' initialized at $root"

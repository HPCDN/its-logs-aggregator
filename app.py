from flask import Flask, request, jsonify
import os, json
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
    updated_files = []

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
            target_array = json.loads(existing_data.decode("utf-8"))
            source_array = json.loads(new_data.decode("utf-8"))

            before_count = len(target_array)
            after_count = before_count + len(source_array)

            combined_array = target_array + source_array
            target_blob.upload_blob(json.dumps(combined_array), overwrite=True)

            updated += 1
            updated_files.append({
                "path": target_blob_path,
                "before": before_count,
                "after": after_count
            })
        except:
            target_blob.upload_blob(new_data)
            created += 1
            created_paths.append(target_blob_path)

    return jsonify({
        "files_processed": processed,
        "files_created": created,
        "files_updated": updated,
        "created_paths": created_paths,
        "updated_files": updated_files
    })

if __name__ == "__main__":
    app.run(debug=True)

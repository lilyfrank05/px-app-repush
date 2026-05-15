"""A Flask web app to generate and upload CSV files for app provisioning via SFTP."""

import csv
import datetime
import io
import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from threading import Lock

import paramiko
import pytz
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, url_for

load_dotenv()

app = Flask(__name__)

# Set up logging to file with 30-day retention
handler = TimedRotatingFileHandler(
    "/app/logs/app.log", when="midnight", interval=1, backupCount=30
)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)
app.secret_key = os.getenv("SECRET_KEY")

# Load apps data
with open("apps.json", encoding="utf-8") as f:
    apps_data = json.load(f)

# SFTP config
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT"))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH")
SFTP_REMOTE_PATH = os.getenv("SFTP_REMOTE_PATH")

filename_lock = Lock()
sydney_tz = pytz.timezone("Australia/Sydney")


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Log client info
        # In Docker, request.remote_addr may be the Docker gateway IP
        # Check X-Real-IP header if behind a proxy
        client_ip = request.headers.get("X-Real-IP", request.remote_addr)
        app.logger.info("Form submitted from IP: %s", client_ip)

        # Get form data
        push_time_str = request.form.get("push_time")
        tids = request.form.getlist("tid[]")
        app_entries = []
        for raw_tid in tids:
            packages = request.form.getlist(f"packages_{raw_tid}[]")
            versions = request.form.getlist(f"versions_{raw_tid}[]")
            forces = request.form.getlist(f"force_{raw_tid}[]")
            individual_tids = [t.strip() for t in raw_tid.split(",") if t.strip()]
            for package, version, force in zip(packages, versions, forces):
                if package and version:  # skip empty
                    for tid in individual_tids:
                        app_entries.append((tid, package, version, force == "True"))

        # Validate and generate CSV
        try:
            # Parse as Sydney time YYYY-MM-DD HH:MM
            push_time = datetime.datetime.strptime(push_time_str, "%Y-%m-%d %H:%M")
            push_time = sydney_tz.localize(push_time)

            # Generate filename
            timestamp = push_time.strftime("%Y%m%d%H%M")
            filename = f"push_app-prov-{timestamp}.csv"
            csv_path = f"/app/csv_output/{filename}"

            # Check for uniqueness (simple check, in production use better method)
            with filename_lock:
                counter = 1
                while os.path.exists(csv_path):
                    filename = f"push_app-prov-{timestamp}-{counter}.csv"
                    csv_path = f"/app/csv_output/{filename}"
                    counter += 1

            # Generate CSV content
            csv_content = generate_csv(app_entries)

            # Save locally first
            with open(csv_path, "w", encoding="utf-8", newline="") as file:
                file.write(csv_content)

            # Upload via SFTP
            upload_to_sftp(csv_path)

            # Remove local file after successful upload
            os.remove(csv_path)

            app.logger.info("CSV file generated and uploaded: %s", filename)
            flash(f"CSV generated and uploaded: {filename}")
            return redirect(url_for("index"))
        except Exception as e:
            flash(f"Error: {str(e)}")
            return redirect(url_for("index"))

    now_sydney = datetime.datetime.now(sydney_tz)
    default_time_str = now_sydney.strftime("%Y-%m-%d %H:%M")
    return render_template(
        "index.html", apps_data=apps_data, default_time_str=default_time_str
    )


@app.route("/health")
def health():
    return "OK", 200


def generate_csv(app_entries):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["TID", "APP", "Version", "ForceUpdate"])

    for tid, package, version, force_update in app_entries:
        writer.writerow([tid, package, version, str(force_update)])

    return output.getvalue()


def upload_to_sftp(filename):
    transport = None
    sftp = None
    try:
        key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, pkey=key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(filename, os.path.join(SFTP_REMOTE_PATH, os.path.basename(filename)))
    except Exception as e:
        app.logger.error("SFTP upload failed: %s", str(e))
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


if __name__ == "__main__":
    port = int(os.getenv("FLASK_PORT"))
    app.run(debug=False, port=port)

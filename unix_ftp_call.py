import os
import paramiko
from ftplib import FTP
from ftplib import FTP_TLS
from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime
from datetime import datetime, timezone

# Load environment variables from .env
load_dotenv()

# Generate timestamp for versioned upload
#timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
# ENV variables
FTP_HOST = os.environ['FTP_HOST']
FTP_USER = os.environ['FTP_USER']
FTP_PASS = os.environ['FTP_PASS']
FTP_FILE_PATH = os.environ['FTP_FILE_PATH']
GCS_BUCKET = os.environ['GCS_BUCKET']
GCS_DEST = os.environ.get('GCS_DEST', f'ftp-data/employees-{timestamp}.csv')


def ftp_to_gcs(request=None):
    """Triggered by HTTP or CLI. Connects to FTP/SFTP and uploads file to GCS."""
    mode = os.environ.get('FTP_MODE', 'SFTP')  # Default is 'SFTP'
    print(f"🚀 Transfer mode: {mode}")

    if mode.upper() == 'SFTP':
        return handle_sftp()
    else:
        return handle_ftp()


def handle_sftp():
    try:
        print("🔐 Connecting to SFTP...")
        transport = paramiko.Transport((FTP_HOST, 21))
        transport.connect(username=FTP_USER, password=FTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)

        local_file = '/tmp/tempfile.csv'
        sftp.get(FTP_FILE_PATH, local_file)
        print("✅ File fetched from SFTP:", local_file)
        print("📁 File exists locally:", os.path.exists(local_file))

        sftp.close()
        transport.close()

        upload_to_gcs(local_file)
        return "✅ SFTP file uploaded to GCS successfully."

    except Exception as e:
        return f"❌ SFTP Error: {str(e)}"


def handle_ftp():
    try:
        print("🔐 Connecting to FTP...")
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        local_file = '/tmp/tempfile.csv'

        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {FTP_FILE_PATH}", f.write)

        print("✅ File fetched from FTP:", local_file)
        print("📁 File exists locally:", os.path.exists(local_file))

        ftp.quit()

        upload_to_gcs(local_file)
        return "✅ FTP file uploaded to GCS successfully."

    except Exception as e:
        return f"❌ FTP Error: {str(e)}"


def upload_to_gcs(local_file_path):
    print("⬆️ Uploading to GCS:", GCS_BUCKET, "/", GCS_DEST)
    storage_client = storage.Client(project='neural-guard-454017-q6')
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_DEST)
    blob.upload_from_filename(local_file_path)
    print("✅ Upload complete.")


# Allow running from CLI
if __name__ == "__main__":
    result = ftp_to_gcs()
    print("📝 Result:", result)
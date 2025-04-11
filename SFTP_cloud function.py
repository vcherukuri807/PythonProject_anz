import os
import paramiko
from ftplib import FTP
from google.cloud import storage

# ENV variables (set in Cloud Function config)
#FTP_PORT = 21
#FTP_HOST = os.environ['127.0.0.1']

# Load environment variables from .env
load_dotenv()

FTP_HOST = os.environ['FTP_HOST']
FTP_USER = os.environ['FTP_USER']
FTP_PASS = os.environ['FTP_PASS']
FTP_FILE_PATH = os.environ['FTP_FILE_PATH']
GCS_BUCKET = os.environ['GCS_BUCKET']

def ftp_to_gcs(request):
    """Triggered by HTTP. Connects to FTP/SFTP and uploads file to GCS."""
    mode = os.environ.get('FTP_MODE', 'SFTP')  # 'SFTP' or 'FTP'

    if mode.upper() == 'SFTP':
        return handle_sftp()
    else:
        return handle_ftp()

def handle_sftp():
    try:
        transport = paramiko.Transport((FTP_HOST, 22))
        transport.connect(username=FTP_USER, password=FTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)

        local_file = '/tmp/tempfile.csv'
        sftp.get(FTP_FILE_PATH, local_file)
        sftp.close()
        transport.close()

        upload_to_gcs(local_file)
        return "✅ SFTP file uploaded to GCS successfully."

    except Exception as e:
        return f"❌ SFTP Error: {str(e)}"

def handle_ftp():
    try:
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        local_file = '/tmp/tempfile.csv'

        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {FTP_FILE_PATH}", f.write)

        ftp.quit()

        upload_to_gcs(local_file)
        return "✅ FTP file uploaded to GCS successfully."

    except Exception as e:
        return f"❌ FTP Error: {str(e)}"

def upload_to_gcs(local_file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(GCS_DEST)
    blob.upload_from_filename(local_file_path)
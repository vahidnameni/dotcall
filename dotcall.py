import os
import tarfile
import csv
import re
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import logging
from datetime import datetime

# Configuration
ROOT_DIR = "/home/sftpbackup/crm"
EXTRACT_DIR = "/home/sftpbackup/crm_extracted"
LOG_FILE = "/home/sftpbackup/logs/files_logs.log"
STATE_FILE = "/home/sftpbackup/logs/extracted_tars.log"
UPLOAD_STATE_FILE = "/home/sftpbackup/logs/uploaded_wavs.log"

# S3 Bucket configuration for .wav files
S3_CREDENTIALS = {
    "access_key_id": "2HU117KTVJ2QSD0MS538",
    "secret_access_key": "1Nb6NBnX3xreQuqX9eDAnh57OWK5ngrY47PQmbed"
}
BUCKET_WAV_CONFIG = {
    "name": "dotcall-prod-ftn0nz5",
    "endpoint": "https://hel1.your-objectstorage.com"
}

# Initialize logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()
stderr_handler = logging.StreamHandler()
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(logging.Formatter('[%(asctime)s] ERROR: %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(stderr_handler)

def initialize_s3_client(endpoint):
    """Initialize and return an S3 client for the given endpoint."""
    logger.info(f"Initializing S3 client for endpoint: {endpoint}")
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=S3_CREDENTIALS["access_key_id"],
            aws_secret_access_key=S3_CREDENTIALS["secret_access_key"],
            config=Config(signature_version='s3v4', retries={'max_attempts': 3, 'mode': 'standard'})
        )
        logger.info(f"Successfully initialized S3 client for {endpoint}")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client for {endpoint}: {e}")
        raise

def file_exists_in_s3(s3_client, bucket, key):
    """Check if a file exists in S3."""
    logger.debug(f"Checking if s3://{bucket}/{key} exists")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.debug(f"File s3://{bucket}/{key} exists")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.debug(f"File s3://{bucket}/{key} does not exist")
            return False
        logger.error(f"Failed to check S3 key {key} in bucket {bucket}: {e}")
        raise

def load_uploaded_wavs(state_file=UPLOAD_STATE_FILE):
    """Load uploaded and failed .wav files from state file, keeping only the latest entry per file."""
    uploaded = {}
    if os.path.exists(state_file) and os.path.getsize(state_file) > 0:
        with open(state_file, 'r') as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) >= 2:
                    wav_path, status = parts[:2]
                    error_message = parts[2] if len(parts) > 2 else ""
                    uploaded[wav_path] = {"status": status, "error_message": error_message}
        logger.info(f"Loaded {len(uploaded)} upload records from {state_file}")
    
    uploaded_wavs = {k for k, v in uploaded.items() if v["status"] == "SUCCESS"}
    failed_wavs = [k for k, v in uploaded.items() if v["status"] == "FAILED"]
    logger.info(f"Found {len(uploaded_wavs)} successful and {len(failed_wavs)} failed uploads")
    return uploaded_wavs, failed_wavs

def update_uploaded_wavs(wav_path, status, error_message=None, state_file=UPLOAD_STATE_FILE):
    """Update the upload state file with the result of an upload attempt, replacing existing entry."""
    try:
        # Load existing entries
        uploaded = {}
        if os.path.exists(state_file) and os.path.getsize(state_file) > 0:
            with open(state_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(" ", 2)
                    if len(parts) >= 2:
                        path, _ = parts[:2]
                        uploaded[path] = line.strip()
        
        # Update or add the entry
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if error_message:
            uploaded[wav_path] = f"{wav_path} {status} {timestamp} {error_message}"
        else:
            uploaded[wav_path] = f"{wav_path} {status} {timestamp}"
        
        # Write back all entries
        with open(state_file, 'w') as f:
            for line in uploaded.values():
                f.write(f"{line}\n")
        
        if status == "SUCCESS" and wav_path in uploaded and uploaded[wav_path].startswith(f"{wav_path} FAILED"):
            logger.info(f"Updated status for {wav_path} from FAILED to SUCCESS")
        else:
            logger.debug(f"Updated upload state for {wav_path}: {status}")
    except OSError as e:
        logger.error(f"Failed to update upload state for {wav_path}: {e}")

def extract_tar(tar_file, extracted_dirs, state_file):
    """Extract a .tar file if not already processed."""
    logger.info(f"Checking extraction status for {tar_file}")
    extracted = set()
    if os.path.exists(state_file) and os.path.getsize(state_file) > 0:
        with open(state_file, 'r') as f:
            extracted = set(f.read().splitlines())
        logger.info(f"Loaded {len(extracted)} previously extracted files from {state_file}")
    
    if tar_file in extracted:
        logger.info(f"Skipping extraction of {tar_file} (already extracted)")
        relative_path = os.path.relpath(tar_file, ROOT_DIR)
        basename = os.path.splitext(os.path.basename(tar_file))[0]
        extracted_dirs.append(os.path.join(relative_path, basename))
        return True
    
    relative_path = os.path.relpath(tar_file, ROOT_DIR)
    basename = os.path.splitext(os.path.basename(tar_file))[0]
    target_dir = os.path.join(EXTRACT_DIR, relative_path, basename)
    os.makedirs(target_dir, exist_ok=True)
    logger.info(f"Extracting {tar_file} to {target_dir}")
    
    try:
        with tarfile.open(tar_file, 'r') as tar:
            tar.extractall(target_dir)
        logger.info(f"Successfully extracted {tar_file} to {target_dir}")
        with open(state_file, 'a') as f:
            f.write(f"{tar_file}\n")
        extracted_dirs.append(os.path.join(relative_path, basename))
        return True
    except tarfile.TarError as e:
        logger.error(f"Failed to extract {tar_file}: {e}")
        return False

def read_csv_metadata(csv_path):
    """Read metadata from a CSV file."""
    logger.info(f"Reading metadata from {csv_path}")
    metadata = {}
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            expected_columns = {'bestandsnaam', 'tijdstip', 'extensie', 'richting', 'afzender', 'bestemming'}
            if not expected_columns.issubset(reader.fieldnames):
                logger.error(f"Missing required columns in {csv_path}. Found: {reader.fieldnames}")
                return {}
            
            for row in reader:
                bestandsnaam = row.get("bestandsnaam", "")
                tijdstip = row.get("tijdstip", "")
                extensie = row.get("extensie", "")
                richting = row.get("richting", "")
                afzender = row.get("afzender", "")
                bestemming = row.get("bestemming", "")
                
                if not (bestandsnaam and tijdstip and extensie and richting and afzender and bestemming):
                    logger.warning(f"Incomplete row in {csv_path}: {row}")
                    continue
                
                if richting.lower() == "incoming":
                    extension = extensie
                    phone = afzender
                elif richting.lower() == "outgoing":
                    extension = extensie
                    phone = bestemming
                else:
                    logger.error(f"Invalid richting in {csv_path}: {richting}")
                    continue
                
                if not (extension.isdigit() and phone.isdigit()):
                    logger.error(f"Invalid extension or phone in {csv_path}: extension={extension}, phone={phone}")
                    continue
                
                metadata[bestandsnaam] = {"tijdstip": tijdstip, "extension": extension, "phone": phone}
                logger.debug(f"Parsed metadata for {bestandsnaam}: extension={extension}, phone={phone}")
        
        logger.info(f"Successfully read {len(metadata)} metadata entries from {csv_path}")
        return metadata
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 decoding failed for {csv_path}, trying latin1")
        try:
            with open(csv_path, newline='', encoding='latin1') as f:
                reader = csv.DictReader(f)
                expected_columns = {'bestandsnaam', 'tijdstip', 'extensie', 'richting', 'afzender', 'bestemming'}
                if not expected_columns.issubset(reader.fieldnames):
                    logger.error(f"Missing required columns in {csv_path}. Found: {reader.fieldnames}")
                    return {}
                
                for row in reader:
                    bestandsnaam = row.get("bestandsnaam", "")
                    tijdstip = row.get("tijdstip", "")
                    extensie = row.get("extensie", "")
                    richting = row.get("richting", "")
                    afzender = row.get("afzender", "")
                    bestemming = row.get("bestemming", "")
                    
                    if not (bestandsnaam and tijdstip and extensie and richting and afzender and bestemming):
                        logger.warning(f"Incomplete row in {csv_path}: {row}")
                        continue
                    
                    if richting.lower() == "incoming":
                        extension = extensie
                        phone = afzender
                    elif richting.lower() == "outgoing":
                        extension = extensie
                        phone = bestemming
                    else:
                        logger.error(f"Invalid richting in {csv_path}: {richting}")
                        continue
                    
                    if not (extension.isdigit() and phone.isdigit()):
                        logger.error(f"Invalid extension or phone in {csv_path}: extension={extension}, phone={phone}")
                        continue
                    
                    metadata[bestandsnaam] = {"tijdstip": tijdstip, "extension": extension, "phone": phone}
                    logger.debug(f"Parsed metadata for {bestandsnaam}: extension={extension}, phone={phone}")
            
            logger.info(f"Successfully read {len(metadata)} metadata entries from {csv_path} using latin1")
            return metadata
        except Exception as e:
            logger.error(f"Failed to read CSV {csv_path}: {e}")
            return {}
    except Exception as e:
        logger.error(f"Failed to read CSV {csv_path}: {e}")
        return {}

def rename_wav_if_needed(wav_file, metadata):
    """Rename .wav file to prefix_extension_phone.wav if needed."""
    logger.info(f"Checking if {wav_file} needs renaming")
    filename = os.path.basename(wav_file)
    
    match = re.match(r'(.+_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})_(\d+)_(\d+)\.wav$', filename)
    if not match:
        logger.error(f"Invalid .wav filename format: {filename}")
        return wav_file
    
    prefix, part1, part2 = match.groups()
    
    if filename not in metadata:
        logger.warning(f"No metadata found for {filename}, skipping rename")
        return wav_file
    
    expected_extension = metadata[filename]["extension"]
    expected_phone = metadata[filename]["phone"]
    
    if part1 == expected_extension and part2 == expected_phone:
        logger.info(f"No renaming needed for {wav_file} (already in correct format)")
        return wav_file
    
    new_filename = f"{prefix}_{expected_extension}_{expected_phone}.wav"
    new_path = os.path.join(os.path.dirname(wav_file), new_filename)
    try:
        os.rename(wav_file, new_path)
        logger.info(f"Renamed {wav_file} to {new_filename}")
        return new_path
    except OSError as e:
        logger.error(f"Failed to rename {wav_file} to {new_filename}: {e}")
        return None

def upload_wav_to_s3(s3_client, wav_file, bucket_config):
    """Upload a .wav file to S3 under calls/year/month/day/ if it doesn't exist."""
    logger.info(f"Attempting to upload {wav_file} to s3://{bucket_config['name']}")
    filename = os.path.basename(wav_file)
    match = re.match(r'.*_(\d{4})-(\d{2})-(\d{2})-\d{2}-\d{2}-\d{2}_.*\.wav$', filename)
    if not match:
        logger.error(f"Invalid filename format for {wav_file}")
        update_uploaded_wavs(wav_file, "FAILED", "Invalid filename format")
        return False
    
    year, month, day = match.groups()
    s3_key = f"calls/{year}/{month}/{day}/{filename}"
    
    if not file_exists_in_s3(s3_client, bucket_config["name"], s3_key):
        try:
            s3_client.upload_file(wav_file, bucket_config["name"], s3_key)
            logger.info(f"Successfully uploaded s3://{bucket_config['name']}/{s3_key}")
            update_uploaded_wavs(wav_file, "SUCCESS")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {s3_key} to {bucket_config['name']}: {e}")
            update_uploaded_wavs(wav_file, "FAILED", str(e))
            return False
    else:
        logger.info(f"Skipped uploading {s3_key} (already exists in {bucket_config['name']})")
        update_uploaded_wavs(wav_file, "SUCCESS")
        return True

def main():
    """Main function to process local .tar files and upload .wav files to S3."""
    logger.info("Starting backup processing script")
    try:
        s3_client = initialize_s3_client(BUCKET_WAV_CONFIG["endpoint"])
    except Exception as e:
        logger.error("Aborting due to S3 client initialization failure")
        return
    
    processed_tars = []
    extracted_dirs = []
    successful_wav_uploads = []
    
    # Load upload state
    uploaded_wavs, failed_wavs = load_uploaded_wavs()
    
    # Step 1: Find .tar files in ROOT_DIR
    logger.info(f"Step 1: Scanning {ROOT_DIR} for .tar files")
    for root, _, files in os.walk(ROOT_DIR):
        for file in files:
            if file.endswith('.tar'):
                tar_file = os.path.join(root, file)
                processed_tars.append(tar_file)
    logger.info(f"Found {len(processed_tars)} .tar files: {processed_tars}")
    
    if not processed_tars and not failed_wavs:
        logger.info("No .tar files or failed .wav files to process")
        return
    
    # Step 2: Extract .tar files and process .wav files
    logger.info("Step 2: Extracting .tar files and processing .wav files")
    for tar_file in processed_tars:
        if tar_file.endswith('.tar'):
            logger.info(f"Processing {tar_file}")
            if extract_tar(tar_file, extracted_dirs, STATE_FILE):
                relative_path = os.path.relpath(tar_file, ROOT_DIR)
                basename = os.path.splitext(os.path.basename(tar_file))[0]
                extract_dir = os.path.join(EXTRACT_DIR, relative_path, basename)
                
                # Find .csv file for metadata
                csv_file = next((os.path.join(extract_dir, f) for f in os.listdir(extract_dir) if f.lower().endswith('.csv')), None)
                if csv_file:
                    logger.info(f"Found CSV file: {csv_file}")
                    metadata = read_csv_metadata(csv_file)
                else:
                    logger.warning(f"No CSV file found in {extract_dir}")
                    metadata = {}
                
                # Process .wav files
                wav_files = [f for f in os.listdir(extract_dir) if f.lower().endswith('.wav')]
                logger.info(f"Found {len(wav_files)} .wav files in {extract_dir}")
                for filename in wav_files:
                    wav_path = os.path.join(extract_dir, filename)
                    if wav_path in uploaded_wavs:
                        logger.info(f"Skipping {wav_path} (already uploaded)")
                        continue
                    if filename not in metadata:
                        logger.warning(f"No metadata for {filename}, uploading without renaming")
                    wav_path = rename_wav_if_needed(wav_path, metadata)
                    if wav_path and upload_wav_to_s3(s3_client, wav_path, BUCKET_WAV_CONFIG):
                        successful_wav_uploads.append(wav_path)
    
    # Step 3: Retry failed .wav files from previous runs
    if failed_wavs:
        logger.info(f"Retrying {len(failed_wavs)} previously failed .wav files")
        for wav_path in failed_wavs:
            if wav_path in uploaded_wavs:
                logger.info(f"Skipping {wav_path} (already marked as SUCCESS)")
                continue
            if not os.path.exists(wav_path):
                logger.warning(f"Failed .wav file {wav_path} no longer exists, skipping")
                continue
            extract_dir = os.path.dirname(wav_path)
            csv_file = next((os.path.join(extract_dir, f) for f in os.listdir(extract_dir) if f.lower().endswith('.csv')), None)
            metadata = read_csv_metadata(csv_file) if csv_file else {}
            wav_path = rename_wav_if_needed(wav_path, metadata)
            if wav_path and upload_wav_to_s3(s3_client, wav_path, BUCKET_WAV_CONFIG):
                successful_wav_uploads.append(wav_path)
    
    # Summary
    logger.info(f"Summary: Processed {len(processed_tars)} .tar files, "
                f"Uploaded {len(successful_wav_uploads)} .wav files")

if __name__ == "__main__":
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        os.makedirs(os.path.dirname(UPLOAD_STATE_FILE), exist_ok=True)
        logger.info("Script initialized, creating necessary directories")
        main()
        logger.info("Process completed successfully")
    except Exception as e:
        logger.error(f"Process failed: {e}")
# dotCall Backup
## Log rotation handling
Potential log loss if `logrotate` misconfigured so use `copytruncate` in `logrotate` config:

    /home/sftpbackup/logs/files-log.log /home/sftpbackup/logs/uploaded_wavs.log {
        daily
        rotate 7
        compress
        missingok
        notifempty
        copytruncate
    }

## SQLite Database for Backup Script

This document describes the SQLite database used by the backup processing script to track extracted `.tar` files and `.wav` file uploads to S3. The database replaces the flat files `extracted_tars.log` and `uploaded_wavs.log` for improved performance, scalability, and concurrency.

## Database Overview

-   **File**: `/home/sftpbackup/logs/state.db`
-   **Purpose**: Stores state information for:
    -   Extracted `.tar` files to prevent reprocessing.
    -   `.wav` file uploads to track status (`SUCCESS` or `FAILED`), timestamps, error messages, and retry attempts.
-   **Tools**:
    -   `sqlite3`: Command-line tool for interacting with the database.
    -   `sqlitebrowser`: GUI tool for inspecting and managing the database.
-   **Access**: The database is accessed by the `sftpbackup` user, with permissions set to `600` (read/write for owner only).

## Database Structure

The database contains two tables:

### Table: `extracted_tars`

-   **Purpose**: Tracks which `.tar` files have been extracted to avoid reprocessing.
-   **Schema**:
    
    ```sql
    CREATE TABLE extracted_tars (
        path TEXT PRIMARY KEY,
        timestamp TEXT
    );
    
    ```
    
-   **Columns**:
    -   `path`: Full path to the `.tar` file (e.g., `/home/sftpbackup/crm/2025/09/01/file1.tar`).
    -   `timestamp`: Extraction timestamp (e.g., `2025-10-23 10:32:00`).

### Table: `uploads`

-   **Purpose**: Tracks the upload status of `.wav` files to S3, including retries and errors.
-   **Schema**:
    
    ```sql
    CREATE TABLE uploads (
        wav_path TEXT PRIMARY KEY,
        status TEXT,
        timestamp TEXT,
        error_message TEXT,
        retry_count INTEGER DEFAULT 0
    );
    
    ```
    
-   **Columns**:
    -   `wav_path`: Full path to the `.wav` file (e.g., `/home/sftpbackup/crm_extracted/2025/09/01/file.wav`).
    -   `status`: Upload status (`SUCCESS` or `FAILED`).
    -   `timestamp`: Last status update (e.g., `2025-10-23 10:32:00`).
    -   `error_message`: Error details for `FAILED` uploads (e.g., `Network error`).
    -   `retry_count`: Number of retry attempts for `FAILED` uploads (default `0`).

## Setup Instructions

The database was created on October 23, 2025, with SQLite version 3.45.1. The following steps were performed:

1.  **Database Creation**:
    
    ```bash
    sqlite3 /home/sftpbackup/logs/state.db
    CREATE TABLE extracted_tars (
        path TEXT PRIMARY KEY,
        timestamp TEXT
    );
    CREATE TABLE uploads (
        wav_path TEXT PRIMARY KEY,
        status TEXT,
        timestamp TEXT,
        error_message TEXT,
        retry_count INTEGER DEFAULT 0
    );
    .exit
    
    ```
    
2.  **Write-Ahead Logging (WAL)**:
    
    -   Enabled WAL for concurrency:
        
        ```bash
        sqlite3 /home/sftpbackup/logs/state.db "PRAGMA journal_mode=WAL;"
        
        ```
        
    -   Output: `wal`
3.  **Permissions**:
    
    -   Set permissions for the `sftpbackup` user:
        
        ```bash
        sudo chown sftpbackup:sftpbackup /home/sftpbackup/logs/state.db
        sudo chmod 600 /home/sftpbackup/logs/state.db
        
        ```
        
4.  **Initial State**:
    
    -   No data was migrated, so both tables are empty.
    -   Verify empty tables:
        
        ```bash
        sqlite3 /home/sftpbackup/logs/state.db "SELECT * FROM extracted_tars; SELECT * FROM uploads;"
        
        ```
        

## Backup Strategy

-   **Daily Backups**:
    -   A cron job backs up `state.db` daily at 2 AM:
        
        ```bash
        0 2 * * * cp /home/sftpbackup/logs/state.db /home/sftpbackup/logs/state.db.backup_$(date +\%Y\%m\%d)
        
        ```
        
    -   Backup files are stored as `/home/sftpbackup/logs/state.db.backup_YYYYMMDD`.
-   **Verification**:
    -   Check backups:
        
        ```bash
        ls -l /home/sftpbackup/logs/state.db.backup_*
        
        ```
        
    -   Restore a backup if needed:
        
        ```bash
        cp /home/sftpbackup/logs/state.db.backup_YYYYMMDD /home/sftpbackup/logs/state.db
        
        ```
        

## Logrotate Configuration

-   The script’s log file (`/home/sftpbackup/logs/files_logs.log`) is managed by `logrotate`:
    
    ```bash
    /home/sftpbackup/logs/files_logs.log {
        daily
        rotate 7
        compress
        missingok
        notifempty
        copytruncate
    }
    
    ```
    
-   The flat files `extracted_tars.log` and `uploaded_wavs.log` are no longer used and have been removed from the logrotate configuration.
-   Verify logrotate:
    
    ```bash
    sudo logrotate -d /etc/logrotate.d/sftpbackup
    
    ```
    
-   Test rotation:
    
    ```bash
    sudo logrotate -f /etc/logrotate.d/sftpbackup
    
    ```
    

## Verification and Troubleshooting

-   **Check Tables**:
    
    ```bash
    sqlite3 /home/sftpbackup/logs/state.db "SELECT name FROM sqlite_master WHERE type='table';"
    
    ```
    
    -   Expected output: `extracted_tars`, `uploads`.
-   **Check Schema**:
    
    ```bash
    sqlite3 /home/sftpbackup/logs/state.db ".schema"
    
    ```
    
-   **Check Data**:
    
    ```bash
    sqlite3 /home/sftpbackup/logs/state.db "SELECT * FROM extracted_tars; SELECT * FROM uploads;"
    
    ```
    
-   **Test Access**:
    
    -   As the `sftpbackup` user, test read/write:
        
        ```bash
        sqlite3 /home/sftpbackup/logs/state.db
        INSERT INTO extracted_tars (path, timestamp) VALUES ('/home/sftpbackup/crm/test.tar', '2025-10-23 10:32:00');
        INSERT INTO uploads (wav_path, status, timestamp, error_message, retry_count) VALUES ('/home/sftpbackup/crm_extracted/test.wav', 'SUCCESS', '2025-10-23 10:32:00', '', 0);
        SELECT * FROM extracted_tars;
        SELECT * FROM uploads;
        DELETE FROM extracted_tars;
        DELETE FROM uploads;
        .exit
        
        ```
        
-   **Use sqlitebrowser**:
    
    ```bash
    sqlitebrowser /home/sftpbackup/logs/state.db
    
    ```
    
-   **Troubleshooting**:
    
    -   If the database is corrupted, restore from a backup.
    -   If permissions issues occur, reapply:
        
        ```bash
        sudo chown sftpbackup:sftpbackup /home/sftpbackup/logs/state.db
        sudo chmod 600 /home/sftpbackup/logs/state.db
        
        ```
        
    -   If concurrency issues arise, ensure WAL is enabled (`PRAGMA journal_mode=WAL;`).

## Next Steps

-   **Script Integration**: Modify the backup script to use SQLite3 instead of `extracted_tars.log` and `uploaded_wavs.log`. This involves:
    -   Importing `sqlite3` and connecting to `/home/sftpbackup/logs/state.db`.
    -   Updating `extract_tar` to query/insert into `extracted_tars`.
    -   Updating `load_uploaded_wavs` and `update_uploaded_wavs` to query/update `uploads`.
    -   Adding retry limit logic using `retry_count`.
-   **Testing**: After modifying the script, test with sample `.tar` and `.wav` files to ensure state tracking works correctly.
-   **Monitoring**: Regularly check `files_logs.log` and `state.db` for errors or unexpected data.
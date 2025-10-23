# dotCall Backup
## Log rotation handling
Potential log loss if `logrotate` misconfigured so use copytruncate in `logrotate` config:

    /home/sftpbackup/logs/files-log.log /home/sftpbackup/logs/uploaded_wavs.log {
        daily
        rotate 7
        compress
        missingok
        notifempty
        copytruncate
    }


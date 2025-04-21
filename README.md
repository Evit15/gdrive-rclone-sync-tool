# gdrive-rclone-sync-tool

# Set up rclone
1. Install rclone: https://rclone.org/install/
2. Config rclone: https://rclone.org/drive/

# Install Python
Follow help at https://www.python.org/downloads/

# Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

# Install dependency
```bash
pip install -r requirements.txt
```

# Create run.bat/run.sh with content
```bat
@echo off
set CURR_DIR=%~dp0

cd /d "%CURR_DIR%"
call venv\Scripts\activate.bat
python daily_sync.py --transfers "[RemoteSource]:[FolderSynchA],[RemoteDestination]:[FolderSynchA]" "[RemoteSource]:[FolderSynchB],[RemoteDestination]:[FolderSynchB]"
```
# Help for run.bat

The `run.bat` script is used to synchronize folders between remote sources and destinations configured in rclone.

## Parameters
- **RemoteSource**: The name of the remote source configured during the "Set up rclone" step.
- **RemoteDestination**: The name of the remote destination configured during the "Set up rclone" step.
- **FolderSynchA**, **FolderSynchB**: The specific folders to synchronize between the source and destination.

## Example Usage
If you have configured a remote source named `MyDrive` and a remote destination named `BackupDrive`, you can synchronize folders as follows:
```bat
python daily_sync.py --transfers "MyDrive:/Documents,BackupDrive:/Documents" "MyDrive:/Pictures,BackupDrive:/Pictures"
```

This example synchronizes the `Documents` and `Pictures` folders between `MyDrive` and `BackupDrive`.
from dagster import RunRequest, SensorResult, sensor, RunConfig, define_asset_job
import os
from typing import Set
from pathlib import Path

ingest_job = define_asset_job(
    name="ingest_raw_fab_data_job",
    selection=["ingest_raw_fab_data"]
)


def get_files_in_directory(directory: str) -> Set[str]:
    """Helper function to get all files in a directory."""
    path = Path(directory)
    if not path.exists():
        return set()
    return {f.name for f in path.glob("*.json")}


@sensor(
    name="raw_data_sensor",
    job=ingest_job,  # Use the job we defined above
    minimum_interval_seconds=30,  # Check every 30 seconds
)
def raw_data_sensor(context) -> SensorResult:
    """
    Sensor that monitors the raw_data directory for new JSON files.
    Triggers the ingest_raw_fab_data job when new files are detected.
    """
    # Get the directory to monitor
    raw_data_dir = "../raw_data"

    # Get current files in directory
    current_files = get_files_in_directory(raw_data_dir)
    print(current_files)
    # Get previously processed files from cursor
    cursor_str = context.cursor or ""
    processed_files = set(cursor_str.split(",")) if cursor_str else set()

    # Find new files
    new_files = current_files - processed_files

    if new_files:
        # Update cursor with all known files
        new_cursor = ",".join(current_files)

        context.log.info(f"Detected new files: {new_files}")

        # Create a run request for each new file
        run_requests = []
        for file in new_files:
            run_key = f"raw_data_sensor_{file}"
            run_config = {
                "ops": {
                    "ingest_raw_fab_data": {
                        "config": {
                            "filename": file
                        }
                    }
                }
            }

            run_requests.append(
                RunRequest(
                    run_key=run_key,
                    run_config=run_config
                )
            )

        return SensorResult(
            run_requests=run_requests,
            cursor=new_cursor
        )

    return SensorResult(skip_reason="No new files detected")

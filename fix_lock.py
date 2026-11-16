
import sqlite3
from datetime import datetime, timezone

db_path = r"C:\data\appliesense.db"

connection = sqlite3.connect(db_path)

cursor = connection.execute(
    """
    UPDATE workflow_runs
    SET status = ?,
        finished_at = ?,
        error = ?
    WHERE id = ? AND status = ?
    """,
    (
        "failed",
        datetime.now(timezone.utc).isoformat(),
        "Recovered stale lock: no active Python process found.",
        5,
        "running",
    ),
)

connection.commit()

print("Rows updated:", cursor.rowcount)

connection.close()

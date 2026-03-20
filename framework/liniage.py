import json
import datetime


class LineageTracker:

    def __init__(self, path="metadata/lineage.json"):
        self.path = path

    def log(self, source, target, operation, record_count):
        """
        Track data lineage for observability
        """
        entry = {
            "timestamp": str(datetime.datetime.utcnow()),
            "source": source,
            "target": target,
            "operation": operation,
            "record_count": record_count
        }

        try:
            with open(self.path, "r") as f:
                data = json.load(f)
        except:
            data = []

        data.append(entry)

        with open(self.path, "w") as f:
            json.dump(data, f, indent=4)

        print(f"Lineage logged: {source} → {target}")
import yaml

class Config:
    def __init__(self):
        with open("config/pipeline.yaml") as f:
            self.cfg = yaml.safe_load(f)

    def get(self, key):
        return self.cfg.get(key)
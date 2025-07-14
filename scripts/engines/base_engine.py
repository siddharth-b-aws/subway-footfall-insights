# scripts/engines/base_engine.py

class BaseEngine:
    def load(self, filepath):
        raise NotImplementedError("load() not implemented")

    def clean(self):
        raise NotImplementedError("clean() not implemented")

    def aggregate(self):
        raise NotImplementedError("aggregate() not implemented")

    def export(self, output_path):
        raise NotImplementedError("export() not implemented")

class DeltaWriter:
    def write(self, df, path):
        df.write.format("delta").mode("append").save(path)
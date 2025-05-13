from pyspark.sql import SparkSession
import argparse

def main(input_path, output_path):
    spark = SparkSession.builder.appName("CountLines").getOrCreate()

    # Legge il file di testo da HDFS
    df = spark.read.text(input_path)

    # Conta il numero di righe
    line_count = df.count()

    # Converte il risultato in DataFrame per poterlo salvare su HDFS
    result_df = spark.createDataFrame([(line_count,)], ["line_count"])

    # Scrive il risultato in output su HDFS (in formato parquet o CSV, qui CSV)
    result_df.write.mode("overwrite").csv(output_path)

    print(f"✔️ File: {input_path} contiene {line_count} righe. Salvato in {output_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conta il numero di righe in un file HDFS.")
    parser.add_argument("--input", required=True, help="Percorso del file di input su HDFS")
    parser.add_argument("--output", required=True, help="Percorso di output su HDFS per salvare il conteggio")
    args = parser.parse_args()

    main(args.input, args.output)

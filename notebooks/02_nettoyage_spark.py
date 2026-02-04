#====================================================================================================
#                             Etape 1.2 - Pipeline de nettoyage Spark
#                  Objectif: Traiter des donnees structurees avec un langage de programmation
#====================================================================================================
import os
import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from utils.utils_logs import log_message
import time

#-------------------------------------------------------------------------------
# Config. des Chemins des données
#-------------------------------------------------------------------------------
DATA_DIR = "/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "consommations_clean")
CONSOMMATION_PATH = os.path.join(DATA_DIR, "consommations_raw.csv")
BATIMENTS_PATH = os.path.join(DATA_DIR, "batiments.csv")

#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
def clear_console():
    os.system("cls" if os.name == "nt" else "clear")

def show_startup_message():
    print("=" * 100)
    print(" " * 30 + "Etape 1.2 - Pipeline de nettoyage Spark")
    print(" " * 20 + "Objectif: Traiter des donnees structurees avec un langage de programmation")
    print("=" * 100)

def create_spark_session():
    """Cree et configure la session Spark."""
    spark = SparkSession.builder \
        .appName("ECF Energie - Nettoyage - partie 1.2") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    ## Reduire les logs
    spark.sparkContext.setLogLevel("ERROR")

    ## Affichage de l'actuelle version de Spark & de l'url de Spark UI
    print()
    print(f"- Spark version: {spark.version}")
    print(f"- Spark UI: {spark.sparkContext.uiWebUrl}")

    ## Affichage du message du succès
    print("[ok]: creation de la session Spark avec succes.")

    return spark

def stop_spark_session(spark: SparkSession):
    """Stop Spark session"""
    spark.stop()
    # Force kill the Py4J gateway (Windows-safe)
    spark.sparkContext._gateway.shutdown()
    print("─" * 100)
    print("[info]: entire spark session is stopped successfully.")
    print("─" * 100)
    os._exit(0)

def parse_multi_format_timestamp(timestamp_str):
    """
    UDF pour parser les timestamps multi-formats.
    Formats supportes:
    - %Y-%m-%d %H:%M:%S (ISO)
    - %d/%m/%Y %H:%M (FR)
    - %m/%d/%Y %H:%M:%S (US)
    - %Y-%m-%dT%H:%M:%S (ISO avec T)
    """
    if timestamp_str is None:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    return None

def clean_consommation(consommation_str):
    """
    UDF pour nettoyer les valeurs numeriques.
    - Remplace la virgule par un point
    - Retourne None pour les valeurs non numeriques
    """
    if consommation_str is None:
        return None

    try:
        # Remplacer virgule par point
        clean_str = consommation_str.replace(",", ".")
        return float(clean_str)
    except (ValueError, AttributeError):
        return None

def close_spark_session(spark:SparkSession):
    ## Release resources 
    spark.stop()
    # Force kill the Py4J gateway (Windows-safe)
    spark.sparkContext._gateway.shutdown()
    print("─" * 100)
    print("[info]: la session spark a ete arretee avec succes.")
    print("─" * 100)
    os._exit(0)

#-------------------------------------------------------------------------------
# Main
#-------------------------------------------------------------------------------
def main():
    try:
        ## Clear console
        clear_console()

        ## start (pour mesurer le temps d'execution de ce script)
        start = time.time()

        ## Show startup message
        show_startup_message()
        
        ## Creation d'une session Spark 
        print("\nCreation d'une session Spark ...\n")
        spark = create_spark_session() 
        
        ## Enregistrer les UDFs
        parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
        clean_consommation_udf = F.udf(clean_consommation, DoubleType())
        
        ## Charger les donnees brutes 
        print("─" * 100)
        print("[1/6] Chargement des donnees brutes ...\n")

        df_consommations_raw = spark.read \
            .option("header", "true") \
            .csv(CONSOMMATION_PATH)
        
        initial_count = df_consommations_raw.count()
        print(f"    - Lignes en entree : {initial_count:,}")
        print(f"    - Nombre de colonnes : {len(df_consommations_raw.columns)}")
        print("    - Schema :")
        df_consommations_raw.printSchema()
        print("    - Appercu des données :")
        df_consommations_raw.show(10, truncate=False)
        
        ## Charger les batiments 
        df_batiments = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(BATIMENTS_PATH)
        
        # ----------------------------------------------------------------
        #               Nettoyage: Traitement des timestamps
        # ----------------------------------------------------------------
        # - Formats de dates multiples => parsing + suppression des timestamps nulles

        print("─" * 100)
        print("[2/6] Traitement des timestamps multi-formats (a partir de la colonne 'timestamp') ...\n")

        df_with_timestamp = df_consommations_raw.withColumn(
            "timestamp_parsed",
            parse_timestamp_udf(F.col("timestamp"))
        )

        ## Filtrer les timestamps nulles (apres le parsing)
        invalid_timestamps = df_with_timestamp.filter(F.col("timestamp_parsed").isNull()).count()
        df_with_timestamp = df_with_timestamp.filter(F.col("timestamp_parsed").isNotNull())
        print(f"- Timestamps invalides supprimes: {invalid_timestamps:,}")
        
        ## Affichage d'infos après parsing des timestamp
        print("- Nouveau Schema:")
        df_with_timestamp.printSchema()
        print("- Apercu des données :")
        df_with_timestamp.show()
        
        # ----------------------------------------------------------------
        #  Nettoyage: Traitement des valeurs (de la colonne consommation)
        # ----------------------------------------------------------------
        # - Valeurs non numeriques & nulles => Supression 
        # - Valeurs avec virgule            => Remplace la virgule decimal par un point (udf: clean_consommation_udf)
        # - Valeurs negatives               => Supression
        # - Outliers (> 10000)          => Supression

        print("─" * 100)
        print("[3/6] Traitement des valeurs de consommation (a partir de la colonne 'consommation') ...\n")

        ## Remplace la virgule decimal par un point
        df_with_consommations = df_with_timestamp.withColumn(
            "consommation_clean",
            clean_consommation_udf(F.col("consommation"))
        )
        # df_with_consommation.show()
        
        ## Filtrer les valeurs non numeriques => transformées en Null grace à l'udf 'clean_consommation_udf'
        
        ## Filtrer les valeurs nulles
        non_numeric_consommations = df_with_consommations.filter(
            F.col("consommation_clean").isNull()
        ).count()

        df_with_consommations = df_with_consommations.filter(
            F.col("consommation_clean").isNotNull()
        )

        print(f"- Valeurs non numeriques supprimees : {non_numeric_consommations:,}")

        ## Supprimer les valeurs négatives et les outliers (>10000)
        print("─" * 100)
        print("[4/6] Suppression des valeurs negatives et les outliers (aberrantes > 10000) ...\n")

        negative_count = df_with_consommations.filter(F.col("consommation_clean") < 0).count()
        outlier_count = df_with_consommations.filter(F.col("consommation_clean") > 10000).count()

        df_clean = df_with_consommations.filter(
            (F.col("consommation_clean") >= 0) & (F.col("consommation_clean") <= 10000)
        )
        print(f"- Nombre des valeurs negatives supprimees : {negative_count:,}")
        print(f"- Nombre des outliers (>10000) supprimes : {outlier_count:,}")

        # df_clean.show()

        ## Dédupliquer sur `(batiment_id, timestamp, type_energie)`
        print("─" * 100)
        print("[5/6] Deduplication ...\n")
        before_dedup = df_clean.count()
        df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp_parsed", "type_energie"])
        after_dedup = df_dedup.count()
        duplicates_removed = before_dedup - after_dedup
        print(f"- Doublons supprimes : {duplicates_removed:,}")

        ## Calculer les moyennes horaires par batiment et type_energie
        print("─" * 100)
        print("[6/6] Agregation horaire et sauvegarde ...\n")
        ## Ajouter les colonnes de temps
        df_with_time = df_dedup.withColumn(
            "date", F.to_date(F.col("timestamp_parsed"))
        ).withColumn(
            "hour", F.hour(F.col("timestamp_parsed"))
        ).withColumn(
            "year", F.year(F.col("timestamp_parsed"))
        ).withColumn(
            "month", F.month(F.col("timestamp_parsed"))
        )
        print("df_with_time : ")
        df_with_time.show()

        ## Agreger par heure (par batiment)
        df_hourly = df_with_time.groupBy(
            "batiment_id", "date", "hour", "year", "month"
        ).agg(
            F.round(F.mean("consommation_clean"), 2).alias("consommation_mean"),
            F.round(F.min("consommation_clean"), 2).alias("consommation_min"),
            F.round(F.max("consommation_clean"), 2).alias("consommation_max"),
            F.count("*").alias("measurement_count")
        )
        print("df_hourly :")
        df_hourly.show()
        
        ## Agreger par jour (par batiment et type d'energie)
        df_daily = df_with_time.groupBy(
            "batiment_id", "type_energie", "unite", "date", "year", "month"
        ).agg(
            F.min("timestamp_parsed").alias("timestamp_start"),
            F.round(F.mean("consommation_clean"), 2).alias("consommation_mean"),
            F.round(F.min("consommation_clean"), 2).alias("consommation_min"),
            F.round(F.max("consommation_clean"), 2).alias("consommation_max"),
            F.count("*").alias("measurement_count")
        )
        print("df_daily : ")
        df_daily.show()

        ## Agreger par mois (par commune)
        df_monthly = df_with_time.groupBy(
            "batiment_id", "type_energie", "unite", "date", "year", "month"
        ).agg(
            F.round(F.mean("consommation_clean"), 2).alias("consommation_mean"),
            F.round(F.min("consommation_clean"), 2).alias("consommation_min"),
            F.round(F.max("consommation_clean"), 2).alias("consommation_max"),
            F.count("*").alias("measurement_count")
        )
        print("df_monthly : ")
        df_monthly.show()

        df_monthly_with_commune = df_monthly.join(
            df_batiments.select("batiment_id", "nom", "commune", "type"),
            on="batiment_id",
            how="left"
        )
        print("df_monthly_with_commune : ")
        df_monthly_with_commune.show()  
        
        ## Joindre avec les informations des batiments (avec le df_daily var l'enregistrement du parquet sera partitionne par date et type d'energie)
        df_final = df_daily.join(
            df_batiments.select("batiment_id", "nom", "commune", "type"),
            on="batiment_id",
            how="left"
        )
        print("df_final : ")
        df_final.show()

        ## Sauvegarder en Parquet partitionné par `date` et `type_energie`
        #OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        df_final.write \
                .mode("overwrite") \
                .partitionBy("date", "type_energie") \
                .parquet(OUTPUT_DIR)

        ## Rapport : lignes en entrée, lignes supprimées, lignes en sortie
        final_count = df_final.count()

        ## Rapport final
        print("RAPPORT DE NETTOYAGE")
        print(f"Lignes en entree:              {initial_count:>12,}")
        print(f"Timestamps invalides:          {invalid_timestamps:>12,}")
        print(f"Valeurs non numeriques:        {non_numeric_consommations:>12,}")
        print(f"Valeurs negatives:             {negative_count:>12,}")
        print(f"Outliers (>10000):              {outlier_count:>12,}")
        print(f"Doublons:                      {duplicates_removed:>12,}")
        total_removed = invalid_timestamps + non_numeric_consommations + negative_count + outlier_count + duplicates_removed
        print(f"Total lignes supprimees:       {total_removed:>12,}")
        print(f"Lignes apres agregation:       {final_count:>12,}")
        print(f"\nFichiers Parquet sauvegardes dans: {OUTPUT_DIR}")

        ## Ecriture du rapport dans les logs également
        log_message("info", "*** RAPPORT DE NETTOYAGE - 'script : 02_nettoyage_spark.py' ***", True)
        log_message("info", f"Lignes en entree                  : {initial_count:>12,}", True)
        log_message("info", f"Timestamps invalides              : {invalid_timestamps:>12,}", True)
        log_message("info", f"Valeurs non numeriques            : {non_numeric_consommations:>12,}", True)
        log_message("info", f"Valeurs negatives                 : {negative_count:>12,}", True)
        log_message("info", f"Outliers (>10000)                 : {outlier_count:>12,}", True)
        log_message("info", f"Doublons                          : {duplicates_removed:>12,}", True)
        log_message("info", f"Total lignes supprimees           : {total_removed:>12,}", True)
        log_message("info", f"Lignes apres agregation           : {final_count:>12,}", True)
        log_message("info", f"Fichiers Parquet sauvegardes dans : {OUTPUT_DIR}", True)

        ## Afficher un apercu
        print("\nApercu des donnees nettoyees:")
        df_final.show(10)
        log_message("info", f"Apercu des donnees nettoyees : {df_final.show(10)}", True)

        ## Statistiques par type_energie
        print("\nStatistiques par type_energie:")
        df_final.groupBy("type_energie") \
            .agg(
                F.count("*").alias("records"),
                F.round(F.mean("consommation_mean"), 2).alias("avg_consommation"),
                F.round(F.min("consommation_min"), 2).alias("min_consommation"),
                F.round(F.max("consommation_max"), 2).alias("max_consommation")
            ) \
            .orderBy("type_energie") \
            .show()

        print("─" * 100)
        print("[ok]: le script a été exécuté avec succès.")
        print("─" * 100)

        ## end time (pour mesurer le temps d'execution de ce script)
        end = time.time()
        print(f"Temps d'exécution : {end - start:.4f} secondes")
        log_message("info", f"Temps d'exécution de ce script (02_nettoyage_spark.py) : {end - start:.4f} secondes", True)

    except Exception as e:
        print("─" * 100)
        print(f"[ko]: {e}")
        print("─" * 100)
        
    finally:
        ## Fermer Spark
        try:
            if spark:
                close_spark_session(spark)
        except:
            pass



if __name__ == "__main__":
    main()

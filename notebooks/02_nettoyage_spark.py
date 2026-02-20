"""
- Script 02_nettoyage_spark.py
- Ce script sera executé dans le node spark qui tourne sur docker
- Pour l'executer (sans executer le pipeline global 'run_pipeline_hybride.py') :
    1) Mettre en place la config. docker nécéssaire :
        > docker compose up -d --build
    2) Executer la commande :
        > docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /notebooks/02_nettoyage_spark.py
"""
#====================================================================================================
#                             Etape 1.2 - Pipeline de nettoyage Spark
#                  Objectif: Traiter des donnees structurees avec un langage de programmation
# Livrables :
#- Ce script 02_nettoyage_spark.py
#- Parquet dans output/02_consommations_clean/
#- Log de traitement (lignes en entree/sortie, lignes supprimees)
#====================================================================================================
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import time
import platform
from utils.utils_logs import log_message, log_df
from utils.utils_resources import get_machine_available_resources
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

#-------------------------------------------------------------------------------
# Config. des Chemins des données
#-------------------------------------------------------------------------------
# ───────────── INPUTS ─────────────
IN_DIR = "/data"
IN_CONSO_RAW_CSV = os.path.join(IN_DIR, "consommations_raw-test.csv")
# IN_CONSO_RAW_CSV = os.path.join(IN_DIR, "consommations_raw.csv")
IN_BAT_CSV = os.path.join(IN_DIR, "batiments.csv")

# ───────────── OUTPUTS ────────────
OUT_DIR = "/output"
OUT_CONSO_CLEAN_PARQUET = os.path.join(OUT_DIR, "02_consommations_clean")

# ───────────── OTHERS ─────────────
ROOT_DIR = Path(__file__).resolve().parent  # ...\ecf_energie\notebooks
CURRENT_SCRIPT_NAME = os.path.basename(os.path.abspath(__file__))
LOG_DIR = ROOT_DIR.parent / "logs"
LOG_FILE_NAME = "02_nettoyage.log"
TMP_DIR = Path("/my_tmp")
TMP_FILE_PATH = TMP_DIR / "tmp_02_resources.txt"

#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
def run(cmd, **kwargs):
    # --- Execute cmd
    log_message(msg_log=f"  Exec. cmd '{cmd}' ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    r = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # merge everything
        text=True,
        **kwargs
    )

    if r.returncode != 0:
        output = r.stdout or ""

        # --- Keep only lines that look like real errors
        error_lines = [
            line for line in output.splitlines()
            if "ERROR" in line
            or "Traceback" in line
            or "Exception" in line
            or "Py4JJavaError" in line
        ]

        # --- Raise SystemExit(r.returncode)
        raise RuntimeError(f"Commande échouée : {cmd} - returncode : {r.returncode} - erreur : {error_lines}")
    log_message(msg_log=f"  Succes Exec. cmd '{cmd}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

def clear_console():
    os.system("cls" if os.name == "nt" else "clear")

def show_startup_message():
    """Show startup message"""
    log_message(msg_log="=" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME, file_log_clear=True)
    log_message(msg_log=" " * 15 + f"Logs de traitement (nettoyage) - script: {CURRENT_SCRIPT_NAME}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=" " * 25 + f"Etape 1.2 - Pipeline de nettoyage Spark", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log="=" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

def show_current_available_resources():
    """Show current machine available resources"""
    log_message(msg_log="Ressources actuellement disponibles sur la machine :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    metrics = get_machine_available_resources()

    ## Show available resources
    log_message(msg_log=f"RAM actuellement disponible sur la machine (free) : {metrics['ram_available_gb']:.2f} GB", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=f"CPU actuellement disponible sur la machine (free) : {metrics['cpu_available_pct']:.1f}%", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=f"        Approx logical cores  (free) : {metrics['available_logical_cores']:.2f}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=f"        Approx physical cores (free) : {metrics['available_physical_cores']:.2f}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

def create_spark_session():
    """Cree et configure la session Spark."""
    spark = SparkSession.builder \
        .appName("ECF Energie - Nettoyage - partie 1.2") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=file:/opt/spark/conf/log4j2.properties") \
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=file:/opt/spark/conf/log4j2.properties") \
        .getOrCreate()

    ## Reduire les logs
    spark.sparkContext.setLogLevel("ERROR")

    ## Affichage versions
    log_message(f"Spark version  : {spark.version}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(f"Spark UI       : {spark.sparkContext.uiWebUrl}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(f"Python version : {platform.python_version()}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(f"Python path    : { sys.executable}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(f"Java version   : {spark._jvm.java.lang.System.getProperty('java.version')}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(f"Java home      : { spark._jvm.java.lang.System.getProperty('java.home')}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    log_message("[ok]: creation de la session Spark avec succès", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    return spark

def parse_multi_format_timestamp(timestamp_str):
    """
    UDF pour parser les timestamps multi-formats.
    Formats supportes:
    - %Y-%m-%d %H:%M:%S (ISO)
    - %d/%m/%Y %H:%M:%S (FR avec secondes) 
    - %d/%m/%Y %H:%M (FR)
    - %m/%d/%Y %H:%M:%S (US)
    - %m/%d/%Y %H:%M (US sans secondes) 
    - %Y-%m-%dT%H:%M:%S (ISO avec T)
    """
    if timestamp_str is None:
        return None

    # strip espaces
    try:
        timestamp_str = timestamp_str.strip()
    except Exception:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",  
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",     
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
        clean_str = consommation_str.replace(",", ".")
        return float(clean_str)
    except (ValueError, AttributeError):
        return None

def close_spark_session(spark:SparkSession):
    log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    spark.stop()
    spark.sparkContext._gateway.shutdown()
    log_message(msg_log="[ok]: la session spark a ete arretee avec succes", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

#-------------------------------------------------------------------------------
# Main
#-------------------------------------------------------------------------------
def main():
    spark = None
    df_journaliere = None

    try:

        # -----------------------------------------------------------------------------------
        # Clear de la console
        # -----------------------------------------------------------------------------------
        clear_console()
       
        # -----------------------------------------------------------------------------------
        # Affichage du message de démarrage
        # -----------------------------------------------------------------------------------
        show_startup_message()

        # -----------------------------------------------------------------------------------
        # Infos & mesures de la performance
        # -----------------------------------------------------------------------------------
        start_time_02 = time.time()

        metrics_start_02 = get_machine_available_resources() # mesure des ressources approximatives allouées au traitement de ce script
        show_current_available_resources()
        
        # -----------------------------------------------------------------------------------
        # Creation d'une session Spark
        # -----------------------------------------------------------------------------------
        log_message("Creation d'une session Spark ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        spark = create_spark_session() 
        
        # -----------------------------------------------------------------------------------
        # Enregistrer les UDFs
        # -----------------------------------------------------------------------------------
        log_message(msg_log="Enregistrement des udf parse_timestamp_udf & clean_consommation_udf", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
        clean_consommation_udf = F.udf(clean_consommation, DoubleType())
        log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    
        # -----------------------------------------------------------------------------------
        # Chargement des donnees brutes
        # -----------------------------------------------------------------------------------
        log_message(msg_log="[1/5] Chargement des donnees brutes ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # --- consommations_raw.csv (df_conso_raw)
        log_message(msg_log="Creation de df_conso_raw (à partir consommations_raw.csv)", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        df_conso_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(IN_CONSO_RAW_CSV)
        
        # Description de df_conso_raw :
        initial_count = df_conso_raw.count()
        log_message(msg_log=f"    - Lignes en entree : {initial_count:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log=f"    - Nombre de colonnes : {len(df_conso_raw.columns)}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="    - Schema :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log=f"\n{df_conso_raw._jdf.schema().treeString()}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_df(msg_log="    - Appercu des données (df_conso_raw):", df=df_conso_raw, file_log=True, file_log_name=LOG_FILE_NAME)
        
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- batiments.csv (df_bat)
        log_message(msg_log="Creation de df_bat (à partir batiments.csv)", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        df_bat = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(IN_BAT_CSV)

        log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # -----------------------------------------------------------------------------------
        # Nettoyage
        # -----------------------------------------------------------------------------------
        log_message(msg_log="[2/5] Nettoyage ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # --- Nettoyage à partir de la colonne timestamps
        log_message(msg_log="Nettoyage à partir de la colonne timestamps", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="Conversion timestamp(string) => nouvelle colonne timestamp_parsed (TimestampType) => parse_timestamp_udf", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # Creation de la colonne timestamp_parsed dans df_with_timestamp en utilisant l'udf parse_timestamp_udf
        df_with_timestamp = df_conso_raw.withColumn(
            "timestamp_parsed",
            parse_timestamp_udf(F.col("timestamp"))
        )
        
        # Suppression des lignes où timestamp_parsed est NULL
        log_message(msg_log="Suppression des lignes où timestamp_parsed est NULL", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        invalid_timestamps = df_with_timestamp.filter(F.col("timestamp_parsed").isNull()).count()
        df_with_timestamp = df_with_timestamp.filter(F.col("timestamp_parsed").isNotNull())
       
        log_message(msg_log=f"Timestamps invalides supprimes: {invalid_timestamps:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- Nettoyage à partir de la colonne consommation
        log_message(msg_log="Nettoyage à partir de la colonne consommation", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="Conversion consommation (string) => nouvelle colonne consommation_clean (DoubleType) => clean_consommation_udf", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # Creation de la colonne consommation_clean dans df_with_conso en utilisant l'udf clean_consommation_udf
        df_with_conso = df_with_timestamp.withColumn(
            "consommation_clean",
            clean_consommation_udf(F.col("consommation"))
        )

        # Suppression des valeurs négatives (consommation_clean < 0)
        log_message(msg_log="Suppression des valeurs négatives (consommation_clean < 0) & nulles (consommation_clean = NULL)", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        negative_count = df_with_conso.filter(F.col("consommation_clean") < 0).count()
        null_count = df_with_conso.filter(F.col("consommation_clean").isNull()).count()
        df_conso_clean = df_with_conso.filter((F.col("consommation_clean") >= 0)) # => supprime les négatives (et les nulles incluses)

        log_message(msg_log=f"Nombre des valeurs negatives supprimees : {negative_count:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log=f"Nombre des valeurs nulles supprimees : {null_count:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # Suppression des outliers (consommation_clean > 10000)
        log_message(msg_log="Suppression des outliers (consommation_clean > 10000)", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        outlier_count = df_conso_clean.filter(F.col("consommation_clean") > 10000).count()
        df_conso_clean = df_conso_clean.filter((F.col("consommation_clean") <= 10000)) # => supprime les outliers (et les nulles incluses(s'il y en a))
        log_message(msg_log=f"Nombre des outliers (>10000) supprimes : {outlier_count:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- Nettoyage des doublons (Deduplication sur (batiment_id, timestamp, type_energie))
        log_message(msg_log="Nettoyage des doublons (Deduplication sur (batiment_id, timestamp, type_energie))", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        before_dedup = df_conso_clean.count()
        df_conso_clean = df_conso_clean.dropDuplicates(["batiment_id", "timestamp_parsed", "type_energie"])
        after_dedup = df_conso_clean.count()
        nbr_duplicates_removed = before_dedup - after_dedup
        log_message(msg_log=f"Nombre de doublons supprimes : {nbr_duplicates_removed:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- Description de df_conso_clean après nettoyage :
        log_message(msg_log="Description de df_conso_clean après le nettoyage complet :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        count_df_conso_clean = df_conso_clean.count()
        log_message(msg_log=f"    - Nombre de lignes : {count_df_conso_clean:,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log=f"    - Nombre de colonnes : {len(df_conso_clean.columns)}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="    - Schema :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log=f"\n{df_conso_clean._jdf.schema().treeString()}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_df(msg_log="    - Appercu des données (df_conso_clean):", df=df_conso_clean, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- Global Checks de la df_conso_clean après nettoyage
        log_message(msg_log="Global checks de la df_conso_clean après nettoyage :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # check timestamp_parsed
        log_message(msg_log="check de la colonne timestamp_parsed :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            # => puisque timestamp a été parsée : timestamp_parsed aura soit des valeurs "corrects" soit des valeurs "nulls"
            
            # verif. nulls
        timestamps_null_after_clean = df_conso_clean.filter((F.col("timestamp_parsed").isNull())).count()
        if timestamps_null_after_clean != 0:
            log_message(level="error", msg_log=f"[ko]: Après nettoyage, df_conso_clean contient encore des lignes avec timestamp_parsed nulles : {timestamps_null_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            log_message(level="info", msg_log=f"[ok]: Après nettoyage, df_conso_clean ne contient plus des lignes avec timestamp_parsed nulles : {timestamps_null_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        log_message(msg_log="", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # check consommation_clean
        log_message(msg_log="check de la colonne consommation_clean :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            # => puisque consommation a été parsée : il y aura dans consommation_clean soit :
            # - les valeurs corrects (double)
            # - soit des Nones 
            # - soit des négatives
            # - soit des ouliers (> 10000)

            # verif. nulls
        conso_null_after_clean = df_conso_clean.filter((F.col("consommation_clean").isNull())).count()
        if conso_null_after_clean != 0:
            log_message(level="error", msg_log=f"[ko]: Après nettoyage, df_conso_clean contient encore des lignes avec consommation_clean nulles : {conso_null_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            log_message(level="info", msg_log=f"[ok]: Après nettoyage, df_conso_clean ne contient plus des lignes avec consommation_clean nulles : {conso_null_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
            # verif. negatives (< 0)
        conso_negative_after_clean = df_conso_clean.filter((F.col("consommation_clean") < 0)).count()
        if conso_negative_after_clean != 0:
            log_message(level="error", msg_log=f"[ko]: Après nettoyage, df_conso_clean contient encore des lignes avec consommation_clean < 0 : {conso_negative_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            log_message(level="info", msg_log=f"[ok]: Après nettoyage, df_conso_clean ne contient plus des lignes avec consommation_clean < 0 : {conso_negative_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # verif. outliers (> 10000)
        conso_outlier_after_clean = df_conso_clean.filter((F.col("consommation_clean") > 10000)).count()
        if conso_outlier_after_clean != 0:
            log_message(level="error", msg_log=f"[ko]: Après nettoyage, df_conso_clean contient encore des lignes avec consommation_clean > 10000 (outliers) : {conso_outlier_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            log_message(level="info", msg_log=f"[ok]: Après nettoyage, df_conso_clean ne contient plus des lignes avec consommation_clean > 10000 (outliers) : {conso_outlier_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # Check doublons :
        log_message(msg_log="Check des doublons :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        df_keys_doublons = (
            df_conso_clean
            .groupBy("batiment_id", "timestamp_parsed", "type_energie")
            .count()
            .filter(F.col("count") > 1)
        )

        doublons_after_clean = df_keys_doublons.count()

        if doublons_after_clean != 0:
            log_message(level="error", msg_log=f"[ko]: Après nettoyage, df_conso_clean contient encore des lignes dupliquées (en 'batiment_id', 'timestamp_parsed', 'type_energie') : {doublons_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            log_message(level="info", msg_log=f"[ok]: Après nettoyage, df_conso_clean ne contient plus des lignes dupliquées (en 'batiment_id', 'timestamp_parsed', 'type_energie') : {doublons_after_clean}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # -----------------------------------------------------------------------------------
        # Enrichissement temporel (pour preparer les aggregations)
        # -----------------------------------------------------------------------------------
        log_message(msg_log="[3/5] Enrichissement temporel (pour preparer les agregations) ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        df_with_time = df_conso_clean.withColumn(
            "date", F.to_date(F.col("timestamp_parsed"))
        ).withColumn(
            "heure", F.hour(F.col("timestamp_parsed"))
        ).withColumn(
            "annee", F.year(F.col("timestamp_parsed"))
        ).withColumn(
            "mois", F.month(F.col("timestamp_parsed"))
        )

        log_df(msg_log="Apperçu de la df_with_time :", df=df_with_time, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # -----------------------------------------------------------------------------------
        # Agregations
        # -----------------------------------------------------------------------------------
        log_message(msg_log="[4/5] Agregations ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- Agregation 1 : Consommations horaires moyennes par batiment
        log_message(msg_log="Agregation 1 : Consommations horaires moyennes par batiment (df_horaire)", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        df_horaire = (df_with_time
            .groupBy(
                "batiment_id", "type_energie", "unite", "date", "heure"
            ).agg(
                F.round(F.mean("consommation_clean"), 2).alias("consommation_moyenne"),
            )
            .select(
                    "batiment_id", "date", "heure", "type_energie", "consommation_moyenne", "unite",
            )
        )
        log_df(msg_log="Apperçu de df_horaire :", df=df_horaire, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # --- Agregation 2 : Consommations journalieres (par date) par batiment et type d'energie
        log_message(msg_log="Agregation 2 : Consommations journalieres (par date) par batiment et type d'energie", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        df_journaliere = (
            df_with_time
            .groupBy(
                "batiment_id", "date", "type_energie", "unite", 
            )
            .agg(
                F.round(F.mean("consommation_clean"), 2).alias("consommation_moyenne"),
            )
            .select(
                "batiment_id", "date", "type_energie", "consommation_moyenne", "unite",
            )
        )

        log_df(msg_log="Apperçu de df_journaliere :", df=df_journaliere, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # --- Agregation 3 : Consommations mensuelles par commune
        log_message(msg_log="Agregation 3 : Consommations mensuelles par commune", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        df_monthly = df_with_time.groupBy(
            "batiment_id", "type_energie", "unite", "annee", "mois"
        ).agg(
            F.round(F.mean("consommation_clean"), 2).alias("consommation_mean"),
            F.round(F.min("consommation_clean"), 2).alias("consommation_min"),
            F.round(F.max("consommation_clean"), 2).alias("consommation_max"),
            F.count("*").alias("measurement_count")
        )
        
        df_monthly_with_commune = df_monthly.join(
            df_bat.select("batiment_id", "commune"),
            on="batiment_id",
            how="left"
        )
 
        log_df(msg_log="Apperçu de df_monthly_with_commune :", df=df_monthly_with_commune, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # -----------------------------------------------------------------------------------
        # Sauvegarde (df_horaire)
        # -----------------------------------------------------------------------------------
        log_message(msg_log="[5/5] Sauvegarde ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="Sauvegarder en Parquet partitionne par date et type d'energie ", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        # => Sauvegarder en Parquet partitionne par date et type d'energie

        # Récupère le count (évite recalcul)
        final_count = df_horaire.count()

        # Ecriture en parquet
        log_message(msg_log="Ecriture en parquet ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        start_time_parquet = time.time()
        df_horaire.write \
                .mode("overwrite") \
                .partitionBy("date", "type_energie") \
                .parquet(OUT_CONSO_CLEAN_PARQUET)
        
        temps_enregistrement_parquet = time.time() - start_time_parquet
        log_message(msg_log="Ecriture en parquet terminée avec succès", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log=f"Temps d'enregistrement en parquet : {temps_enregistrement_parquet:.2f} secondes", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # -----------------------------------------------------------------------------------
        # Rapport
        # -----------------------------------------------------------------------------------
        log_message(msg_log="RAPPORT DE NETTOYAGE", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Lignes en entree                  : {initial_count:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Timestamps invalides              : {invalid_timestamps:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Valeurs non numeriques            : {null_count:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Valeurs negatives                 : {negative_count:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Outliers (>10000)                 : {outlier_count:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Doublons                          : {nbr_duplicates_removed:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        total_removed = invalid_timestamps + null_count + negative_count + outlier_count + nbr_duplicates_removed
        log_message(level="info", msg_log=f"Total lignes supprimees           : {total_removed:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Lignes apres agregation           : {final_count:>12,}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(level="info", msg_log=f"Fichiers Parquet sauvegardes dans : {OUT_CONSO_CLEAN_PARQUET}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # --- Logs du temps d'execution + ressources 
        temps_execution_02 = time.time() - start_time_02
        log_message(level="info", msg_log=f"Temps d'exécution de ce script ({CURRENT_SCRIPT_NAME}) : {temps_execution_02:.2f} secondes", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        ram_gb_02 = metrics_start_02['ram_available_gb']
        cpu_pct_02 = metrics_start_02['cpu_available_pct']
        logi_cores_02 = metrics_start_02['available_logical_cores']
        physi_cores_02 = metrics_start_02['available_physical_cores']
        log_message(level="info", msg_log=f"Ressources machine approximatives allouées à ce traitement : (RAM : {ram_gb_02:.2f} GB, CPU : {cpu_pct_02:.2f}% - Approx logical cores free({logi_cores_02:.2f}) - Approx physical cores free : {physi_cores_02:.2f})", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        # --- Enregistrement dans un fichier temporaire du temps d'execution + ressources pour 
        #     utilisation ultérieure (dans le script run_pipeline_hybride.py ou autres.)

        log_message(msg_log="", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # Ecrire des données du temps d'execution + ressources dans le fichier TMP_FILE_PATH
        log_message(msg_log=f"Ecrire des données du temps d'execution + ressources dans le fichier {TMP_FILE_PATH.name}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
        temps_resources = f"""
        Date : {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} (FR)

        temps_exec_sec={temps_execution_02:.2f}
        ram_gb={ram_gb_02:.2f}
        cpu_pct={cpu_pct_02:.2f}
        logi_cores={logi_cores_02:.1f}
        physi_cores={physi_cores_02:.1f}
        """

        TMP_FILE_PATH.write_text(temps_resources, encoding="utf-8")
        # --
        log_message(msg_log="---", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(level="info", msg_log="[ok]: Fin du traitement, le script a été exécuté avec succès", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    except Exception as e:
        log_message(level="error", msg_log=f"[ko]: {e}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        raise
        
    finally:
        try:
            if df_horaire is not None:
                try:
                    df_horaire.unpersist()
                    log_message(msg_log=f"[ok]: cache de df_horaire libéré", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                except:
                    log_message(level="error", msg_log="[ko]: error lors de la libération du cache pour df_horaire", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            if spark:
                close_spark_session(spark)
        except:
            pass

        log_message(msg_log="=" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)


if __name__ == "__main__":
    main()

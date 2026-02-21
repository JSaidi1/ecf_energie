import os
import subprocess
#from subprocess import run, DEVNULL
import sys
from pathlib import Path
import time

from utils.utils_logs import log_message, log_table
from utils.utils_resources import get_machine_available_resources
from utils.utils_global import read_key_value


# ======================================================================================================
#                                              CONFIG.
# ======================================================================================================
# --- Current script
ROOT_DIR = Path(__file__).resolve().parent  # ...\ecf_energie\pipeline
CURRENT_SCRIPT_NAME = os.path.basename(os.path.abspath(__file__))

# --- Scripts/notebooks
SCRIPTS_DIR = ROOT_DIR.parent / "notebooks"

NB1 = SCRIPTS_DIR / "01_exploration_spark.ipynb"      # local 
NB1_NAME = Path(NB1).name

NB2 = SCRIPTS_DIR / "02_nettoyage_spark.py"           # cluster (on docker)
NB2_NAME = Path(NB2).name
CONTAINER = "spark-master"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER_URL = "spark://spark-master:7077"
SCRIPT_IN_CONTAINER = f"/notebooks/{NB2_NAME}"

NB3 = SCRIPTS_DIR / "03_agregations_spark.ipynb"      # local 
NB3_NAME = Path(NB3).name

NB4 = SCRIPTS_DIR / "04_nettoyage_meteo_pandas.ipynb" # local 
NB4_NAME = Path(NB4).name

NB5 = SCRIPTS_DIR / "05_fusion_enrichissement.ipynb"  # local 
NB5_NAME = Path(NB5).name

NB6 = SCRIPTS_DIR / "06_statistiques_descriptives.ipynb"  # local 
NB6_NAME = Path(NB6).name

NB7 = SCRIPTS_DIR / "07_analyse_correlations.ipynb"  # local 
NB7_NAME = Path(NB7).name

NB8 = SCRIPTS_DIR / "08_detection_anomalies.ipynb"  # local 
NB8_NAME = Path(NB8).name

NB9 = SCRIPTS_DIR / "09_visualisations_matplotlib.ipynb"  # local 
NB9_NAME = Path(NB9).name

NB10 = SCRIPTS_DIR / "10_visualisations_seaborn.ipynb"  # local 
NB10_NAME = Path(NB10).name

NB11 = SCRIPTS_DIR / "11_dashboard_executif.ipynb"  # local 
NB11_NAME = Path(NB11).name

# --- Logs
LOG_DIR = ROOT_DIR.parent / "logs"
LOG_FILE_NAME = "pipeline_global.log"

# --- Autres
# Lectures des metrics (ressources)
TMP_FILE_01 = ROOT_DIR.parent / "my_tmp" / "tmp_01_resources.txt"
TMP_FILE_02 = ROOT_DIR.parent / "my_tmp" / "tmp_02_resources.txt"
TMP_FILE_03 = ROOT_DIR.parent / "my_tmp" / "tmp_03_resources.txt"
TMP_FILE_04 = ROOT_DIR.parent / "my_tmp" / "tmp_04_resources.txt"
TMP_FILE_05 = ROOT_DIR.parent / "my_tmp" / "tmp_05_resources.txt"
TMP_FILE_06 = ROOT_DIR.parent / "my_tmp" / "tmp_06_resources.txt"
TMP_FILE_07 = ROOT_DIR.parent / "my_tmp" / "tmp_07_resources.txt"
TMP_FILE_08 = ROOT_DIR.parent / "my_tmp" / "tmp_08_resources.txt"
TMP_FILE_09 = ROOT_DIR.parent / "my_tmp" / "tmp_09_resources.txt"
TMP_FILE_10 = ROOT_DIR.parent / "my_tmp" / "tmp_10_resources.txt"
TMP_FILE_11 = ROOT_DIR.parent / "my_tmp" / "tmp_11_resources.txt"

# Scripts Outputs
NB1_OUTPUT_STR = "-Rapport d'audit :\n'/output/01_rapport_audit_donnees.md'"
NB2_OUTPUT_STR = "-Parquet dans :\n'/output/02_consommations_clean/'\n\n-Log de traitement :\n'/logs/02_logs_nettoyage.log'"
NB3_OUTPUT_STR = "-Table agregee :\n'/output/03_consommations_agregees.parquet'\n\n-Requete Spark SQL\ndemonstrant l'utilisation\nde la vue (voir\n03_agregations_spark.ipynb)"
NB4_OUTPUT_STR = "-Dataset nettoye :\n'/output/04_meteo_clean.csv'\n\n-Rapport avant/apres \nnettoyage (voir\n04_nettoyage_meteo_pandas.ipynb)"
NB5_OUTPUT_STR = "-Dataset final :\n'/output/05_consommations_enrichies.csv'\n\n-Dataset final :\n'/output/05_consommations_enrichies.parquet'\n\nDictionnaire de donnees :\n(voir 05_fusion_enrichissement.ipynb)"
NB6_OUTPUT_STR = "-Tabs de synthèse :\n'/output/06_exports'"
NB7_OUTPUT_STR = "-Matrice de correlation :\n'/output/07_matrice_correlation.csv'\n\n-Synthese des insights :\n'output/07_rapport_insight.md'"
NB8_OUTPUT_STR = "-Liste des anomalies :\n'/output/08_anomalies_detectees.csv'\n\n-Rapport de \nrecommandations d'audit :\n'output/08_rapport_audit.md'"
NB9_OUTPUT_STR = "-5 figures PNG \n(300 dpi) dans :\n'/output/09_figures'"
NB10_OUTPUT_STR = "-6 figures PNG \n(300 dpi) dans :\n'/output/10_figures'"
NB11_OUTPUT_STR = "-Figure dashboard :\n'/11_figures/dashboard_energie.png'"


# ======================================================================================================
#                                             FUNCTIONS
# ======================================================================================================
def show_startup_message():
    """Show startup message"""
    log_message(msg_log="=" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME, file_log_clear=True)
    log_message(msg_log=" " * 10 + f"Pipeline global de traitement des données de consommation energetique des batiments", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=" " * 35 + f"script: {CURRENT_SCRIPT_NAME}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log="=" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

def show_current_available_resources():
    """Show current machine available resources"""
    log_message(msg_log="Ressources actuellement disponibles sur la machine :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    metrics = get_machine_available_resources()

    ## Show available resources
    log_message(msg_log=f"RAM actuellement disponible sur la machine (free) : {metrics['ram_available_gb']:.2f} GB", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=f"CPU actuellement disponible sur la machine (free) : {metrics['cpu_available_pct']:.1f}%", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=f"        Approx logical cores  (free) : {metrics['available_logical_cores']:.2f}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=f"        Approx physical cores (free) : {metrics['available_physical_cores']:.2f}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log="─" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

def write_metric_logs(temps_exec_sec, ram_gb, cpu_pct, logi_cores, physi_cores, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME):
    """Ecrire les logs des metrics."""
    log_message(msg_log=f"=> Temps d'execution = {temps_exec_sec:.2f} secondes", file_log=file_log, file_log_dir=file_log_dir, file_log_name=file_log_name)
    log_message(msg_log=f"=> Ressources approximatives allouées à l'execution de ce script :", file_log=file_log, file_log_dir=file_log_dir, file_log_name=file_log_name)
    log_message(msg_log=f"    RAM (free) : {ram_gb} GB", file_log=file_log, file_log_dir=file_log_dir, file_log_name=file_log_name)
    log_message(msg_log=f"    CPU (free) : {cpu_pct} %", file_log=file_log, file_log_dir=file_log_dir, file_log_name=file_log_name)
    log_message(msg_log=f"    LOGI_CORES (free) : {logi_cores}", file_log=file_log, file_log_dir=file_log_dir, file_log_name=file_log_name)
    log_message(msg_log=f"    PHYSI_CORES (free) : {physi_cores}", file_log=file_log, file_log_dir=file_log_dir, file_log_name=file_log_name)
            

def is_docker_running() -> bool:
    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        return True
    except Exception as e:
        return False

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


def run_notebook_local(path: Path):
    script_name = path.name
    log_message(msg_log=f"Execution en local du script '{script_name}' ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    if not path.exists():
        raise FileNotFoundError(f"Notebook introuvable: {path}")

    run([
        sys.executable, "-m", "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--inplace",
        str(path)
    #], stdout=DEVNULL, stderr=subprocess.PIPE, check=True)
    ])

def run_cluster_step(scrit_path: Path):
    script_name = scrit_path.name
    log_message(msg_log=f"Execution sur cluster spark du script '{script_name}' ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    
    # Optionnel: vérifier que le conteneur existe
    #run(["docker", "inspect", CONTAINER], stdout=DEVNULL, stderr=subprocess.PIPE, check=True)
    run(["docker", "inspect", CONTAINER])

    run([
        "docker", "exec", CONTAINER,
        SPARK_SUBMIT,
        "--master", SPARK_MASTER_URL,
        SCRIPT_IN_CONTAINER
    ])

# ======================================================================================================
#                                                 MAIN
# ======================================================================================================
if __name__ == "__main__":
    
    exec_cmd_docker = False
    succes_pipeline = False

    try:
        # --- Clear console
        os.system("cls" if os.name == "nt" else "clear")

        # --- Show startup message
        show_startup_message()

        # --- Check if docker is running
        if is_docker_running() == False:
            log_message(level="error", msg_log="Docker Engine is NOT running", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            exec_cmd_docker = True
            log_message(msg_log="Docker Engine is running ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
            # --- Recréer et démarrer les appli. Docker Compose en arrière-plan
            log_message(msg_log="Recreation et demarrage des application 'Docker Compose' en arrière-plan. Merci " \
            "d'attendre ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            start_time_exec_doc_compose = time.time()
            run(["docker", "compose", "up", "-d", "--build"])
            end_time_exec_doc_compose = time.time() - start_time_exec_doc_compose
            log_message(msg_log=f"Recreation et demarrage des application 'Docker Compose' en arrière-plan avec succès", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            log_message(msg_log=f"=> Temps d'execution = {end_time_exec_doc_compose:.2f} secondes", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            log_message(msg_log="─" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # --- Execution des scripts
            # NB1            
            run_notebook_local(NB1)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB1_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_01 = read_key_value(TMP_FILE_01)["temps_exec_sec"]
            ram_gb_01 = read_key_value(TMP_FILE_01)["ram_gb"]
            cpu_pct_01 = read_key_value(TMP_FILE_01)["cpu_pct"]
            logi_cores_01 = read_key_value(TMP_FILE_01)["logi_cores"]
            physi_cores_01 = read_key_value(TMP_FILE_01)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_01, ram_gb_01, cpu_pct_01, logi_cores_01, physi_cores_01)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
              
            # NB2
            run_cluster_step(NB2)    # cluster (on docker)
            log_message(msg_log=f"Execution avec succès du script '{NB2_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_02 = read_key_value(TMP_FILE_02)["temps_exec_sec"]
            ram_gb_02 = read_key_value(TMP_FILE_02)["ram_gb"]
            cpu_pct_02 = read_key_value(TMP_FILE_02)["cpu_pct"]
            logi_cores_02 = read_key_value(TMP_FILE_02)["logi_cores"]
            physi_cores_02 = read_key_value(TMP_FILE_02)["physi_cores"]
                
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_02, ram_gb_02, cpu_pct_02, logi_cores_02, physi_cores_02)

            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            
            # NB3
            run_notebook_local(NB3)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB3_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_03 = read_key_value(TMP_FILE_03)["temps_exec_sec"]
            ram_gb_03 = read_key_value(TMP_FILE_03)["ram_gb"]
            cpu_pct_03 = read_key_value(TMP_FILE_03)["cpu_pct"]
            logi_cores_03 = read_key_value(TMP_FILE_03)["logi_cores"]
            physi_cores_03 = read_key_value(TMP_FILE_03)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_03, ram_gb_03, cpu_pct_03, logi_cores_03, physi_cores_03)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB4
            run_notebook_local(NB4)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB4_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_04 = read_key_value(TMP_FILE_04)["temps_exec_sec"]
            ram_gb_04 = read_key_value(TMP_FILE_04)["ram_gb"]
            cpu_pct_04 = read_key_value(TMP_FILE_04)["cpu_pct"]
            logi_cores_04 = read_key_value(TMP_FILE_04)["logi_cores"]
            physi_cores_04 = read_key_value(TMP_FILE_04)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_04, ram_gb_04, cpu_pct_04, logi_cores_04, physi_cores_04)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB5
            run_notebook_local(NB5)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB5_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_05 = read_key_value(TMP_FILE_05)["temps_exec_sec"]
            ram_gb_05 = read_key_value(TMP_FILE_05)["ram_gb"]
            cpu_pct_05 = read_key_value(TMP_FILE_05)["cpu_pct"]
            logi_cores_05 = read_key_value(TMP_FILE_05)["logi_cores"]
            physi_cores_05 = read_key_value(TMP_FILE_05)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_05, ram_gb_05, cpu_pct_05, logi_cores_05, physi_cores_05)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB6
            run_notebook_local(NB6)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB6_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_06 = read_key_value(TMP_FILE_06)["temps_exec_sec"]
            ram_gb_06 = read_key_value(TMP_FILE_06)["ram_gb"]
            cpu_pct_06 = read_key_value(TMP_FILE_06)["cpu_pct"]
            logi_cores_06 = read_key_value(TMP_FILE_06)["logi_cores"]
            physi_cores_06 = read_key_value(TMP_FILE_06)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_06, ram_gb_06, cpu_pct_06, logi_cores_06, physi_cores_06)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB7
            run_notebook_local(NB7)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB7_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_07 = read_key_value(TMP_FILE_07)["temps_exec_sec"]
            ram_gb_07 = read_key_value(TMP_FILE_07)["ram_gb"]
            cpu_pct_07 = read_key_value(TMP_FILE_07)["cpu_pct"]
            logi_cores_07 = read_key_value(TMP_FILE_07)["logi_cores"]
            physi_cores_07 = read_key_value(TMP_FILE_07)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_07, ram_gb_07, cpu_pct_07, logi_cores_07, physi_cores_07)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB8
            run_notebook_local(NB8)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB8_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_08 = read_key_value(TMP_FILE_08)["temps_exec_sec"]
            ram_gb_08 = read_key_value(TMP_FILE_08)["ram_gb"]
            cpu_pct_08 = read_key_value(TMP_FILE_08)["cpu_pct"]
            logi_cores_08 = read_key_value(TMP_FILE_08)["logi_cores"]
            physi_cores_08 = read_key_value(TMP_FILE_08)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_08, ram_gb_08, cpu_pct_08, logi_cores_08, physi_cores_08)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB9
            run_notebook_local(NB9)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB9_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_09 = read_key_value(TMP_FILE_09)["temps_exec_sec"]
            ram_gb_09 = read_key_value(TMP_FILE_09)["ram_gb"]
            cpu_pct_09 = read_key_value(TMP_FILE_09)["cpu_pct"]
            logi_cores_09 = read_key_value(TMP_FILE_09)["logi_cores"]
            physi_cores_09 = read_key_value(TMP_FILE_09)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_09, ram_gb_09, cpu_pct_09, logi_cores_09, physi_cores_09)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB10
            run_notebook_local(NB10)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB10_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_10 = read_key_value(TMP_FILE_10)["temps_exec_sec"]
            ram_gb_10 = read_key_value(TMP_FILE_10)["ram_gb"]
            cpu_pct_10 = read_key_value(TMP_FILE_10)["cpu_pct"]
            logi_cores_10 = read_key_value(TMP_FILE_10)["logi_cores"]
            physi_cores_10 = read_key_value(TMP_FILE_10)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_10, ram_gb_10, cpu_pct_10, logi_cores_10, physi_cores_10)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB11
            run_notebook_local(NB11)  # en local
            log_message(msg_log=f"Execution avec succès du script '{NB11_NAME}'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
                
                # Lecture du temps d'execution + ressources à partir du fichier temporaire
            temps_exec_sec_11 = read_key_value(TMP_FILE_11)["temps_exec_sec"]
            ram_gb_11 = read_key_value(TMP_FILE_11)["ram_gb"]
            cpu_pct_11 = read_key_value(TMP_FILE_11)["cpu_pct"]
            logi_cores_11 = read_key_value(TMP_FILE_11)["logi_cores"]
            physi_cores_11 = read_key_value(TMP_FILE_11)["physi_cores"]
                # Enregistrement dans les logs
            write_metric_logs(temps_exec_sec_11, ram_gb_11, cpu_pct_11, logi_cores_11, physi_cores_11)
            
            log_message(msg_log=f"", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # --- Temps total d'execution du pipeline 
            temps_total = temps_exec_sec_01 + temps_exec_sec_02 + temps_exec_sec_03 + temps_exec_sec_04 + temps_exec_sec_05 + \
            temps_exec_sec_06 + temps_exec_sec_07 + temps_exec_sec_08 + temps_exec_sec_09 + temps_exec_sec_10 + temps_exec_sec_11
            log_message(msg_log=f"Temps total d'execution du pipeline : {temps_total:.2f} secondes", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            log_message(msg_log="─" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            
            # --- Message de succès
            log_message(msg_log=f"Pipeline terminé avec succès", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            succes_pipeline = True

    except Exception as e:
        log_message(level="error", msg_log=f"Pipeline arrêtée : {e}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    finally:
        if exec_cmd_docker:
            # --- Arrêt et suppression des ressources créées par 'docker compose up'
            #run(["docker", "compose", "down"], stdout=DEVNULL, stderr=DEVNULL)
            run(["docker", "compose", "down"])
            log_message(msg_log=f"Arrêt et suppression des ressources (créées par 'docker compose up')", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        # --- Rapport global
        if succes_pipeline:
            log_message(msg_log="─" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            log_message(msg_log="Rapport global d'execution du pipeline :", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            data = [
                {
                    "Script": NB1_NAME, "But": "Exploration\ninitiale\ndes données", 
                    "Output(s)": NB1_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_01} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_01} GB\nCPU: {cpu_pct_01}%\n phys. cores: {physi_cores_01}\n logi. cores: {logi_cores_01}"
                },
                {
                    "Script": NB2_NAME, "But": "Nettoyage\ndes données\nde consommation\nénergetique", 
                    "Output(s)": NB2_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nCluster\nspark\n(docker)\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_02} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_02} GB\nCPU: {cpu_pct_02}%\n phys. cores: {physi_cores_02}\n logi. cores: {logi_cores_02}"
                },
                {
                    "Script": NB3_NAME, "But": "Agregations\ndes données", 
                    "Output(s)": NB3_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_03} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_03} GB\nCPU: {cpu_pct_03}%\n phys. cores: {physi_cores_03}\n logi. cores: {logi_cores_03}"
                },
                {
                    "Script": NB4_NAME, "But": "Nettoyage\ndes donnees meteos\navec Pandas", 
                    "Output(s)": NB4_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_04} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_04} GB\nCPU: {cpu_pct_04}%\n phys. cores: {physi_cores_04}\n logi. cores: {logi_cores_04}"
                },
                {
                    "Script": NB5_NAME, "But": "Fusion et \nenrichissement", 
                    "Output(s)": NB5_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_05} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_05} GB\nCPU: {cpu_pct_05}%\n phys. cores: {physi_cores_05}\n logi. cores: {logi_cores_05}"
                },
                {
                    "Script": NB6_NAME, "But": "Statistiques\ndescriptives", 
                    "Output(s)": NB6_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_06} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_06} GB\nCPU: {cpu_pct_06}%\n phys. cores: {physi_cores_06}\n logi. cores: {logi_cores_06}"
                },
                {
                    "Script": NB7_NAME, "But": "Analyse\ndes correlations", 
                    "Output(s)": NB7_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_07} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_07} GB\nCPU: {cpu_pct_07}%\n phys. cores: {physi_cores_07}\n logi. cores: {logi_cores_07}"
                },
                {
                    "Script": NB8_NAME, "But": "Detection\nd'anomalies", 
                    "Output(s)": NB8_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_08} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_08} GB\nCPU: {cpu_pct_08}%\n phys. cores: {physi_cores_08}\n logi. cores: {logi_cores_08}"
                },
                {
                    "Script": NB9_NAME, "But": "Graphiques\nMatplotlib", 
                    "Output(s)": NB9_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_09} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_09} GB\nCPU: {cpu_pct_09}%\n phys. cores: {physi_cores_09}\n logi. cores: {logi_cores_09}"
                },
                {
                    "Script": NB10_NAME, "But": "Visualisations\nSeaborn", 
                    "Output(s)": NB10_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_10} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_10} GB\nCPU: {cpu_pct_10}%\n phys. cores: {physi_cores_10}\n logi. cores: {logi_cores_10}"
                },
                {
                    "Script": NB11_NAME, "But": "Création\nd'un Dashboard\nexecutif", 
                    "Output(s)": NB11_OUTPUT_STR, 
                    "Env.\nd'exec.\n&\nMetrics": f"-Env.\nd'exec. :\nLocal\n\n-Metrics :\nTemps d'exec. :\n{temps_exec_sec_11} sec \n\nRessources\nallouées\n(free) : \nRAM: {ram_gb_11} GB\nCPU: {cpu_pct_11}%\n phys. cores: {physi_cores_11}\n logi. cores: {logi_cores_11}"
                }
            ]
            log_table(data = data,file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

        log_message(msg_log="=" * 110, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

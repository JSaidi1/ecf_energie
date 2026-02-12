import os
import subprocess
from subprocess import run, DEVNULL
import sys
from pathlib import Path
import time

from utils.utils_logs import log_message
from utils.utils_resources import get_machine_available_resources


# ======================================================================================================
#                                              CONFIG.
# ======================================================================================================
ROOT = Path(__file__).resolve().parent  # ...\ecf_energie\notebooks

NB1 = ROOT / "01_exploration_spark.ipynb"
NB1_NAME = Path(NB1).name
#NB3 = ROOT / "03_agregations_spark.ipynb"

# CONTAINER = "spark-master"
# SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
# SPARK_MASTER_URL = "spark://spark-master:7077"
# SCRIPT_IN_CONTAINER = "/notebooks/02_nettoyage_spark.py"

# --- Logs
CURRENT_SCRIPT_NAME = os.path.basename(os.path.abspath(__file__))
LOG_DIR = ROOT.parent / "logs"
LOG_FILE_NAME = "pipeline_global.log"

# ======================================================================================================
#                                             FUNCTIONS
# ======================================================================================================
def show_startup_message():
    """Show startup message"""
    log_message(msg_log="=" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME, file_log_clear=True)
    log_message(msg_log=" " * 5 + f"Pipeline global de traitement des données de consommation energetique des batiments", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
    log_message(msg_log=" " * 30 + f"script: {CURRENT_SCRIPT_NAME}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
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
    
def is_docker_running() -> bool:
    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        return True
    except Exception:
        return False

def run(cmd, **kwargs):
    #print(f"\n {' '.join(cmd)}")
    r = subprocess.run(cmd, **kwargs)
    if r.returncode != 0:
        raise SystemExit(r.returncode)

def run_notebook_local(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Notebook introuvable: {path}")
    run([
        sys.executable, "-m", "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--inplace",
        str(path)
    ], stdout=DEVNULL, stderr=DEVNULL)

def run_cluster_step():
    # Optionnel: vérifier que le conteneur existe
    run(["docker", "inspect", CONTAINER], stdout=DEVNULL, stderr=DEVNULL)

    run([
        "docker", "exec", CONTAINER,
        SPARK_SUBMIT,
        "--master", SPARK_MASTER_URL,
        SCRIPT_IN_CONTAINER
    ], stdout=DEVNULL, stderr=DEVNULL)

# ======================================================================================================
#                                                 MAIN
# ======================================================================================================
if __name__ == "__main__":
    
    exec_cmd_docker = False

    try:
        # --- Clear console
        os.system("cls" if os.name == "nt" else "clear")

        # --- Show startup message
        show_startup_message()

        # --- Show les ressources actuellement disponible sur la machine
        show_current_available_resources()

        # --- Check if docker is running
        if is_docker_running() == False:
            log_message(level="error", msg_log="Docker Engine is NOT running", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        else:
            exec_cmd_docker = True
            log_message(msg_log="Docker Engine is running ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        
            # --- Recréer et démarrer les appli. Docker Compose en arrière-plan
            log_message(msg_log="Recreation et demarrage des application 'Docker Compose' en arrière-plan ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            run(["docker", "compose", "up", "-d", "--build"], stdout=DEVNULL, stderr=DEVNULL)
            log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # --- Execution les scripts
            # NB1
            log_message(msg_log=f"Execution en local du script '{NB1_NAME}' ...", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            start_time_nb_1 = time.time()
            run_notebook_local(NB1)  # en local
            time_exec_nb1 = time.time() - start_time_nb_1
            log_message(msg_log=f"[ok]: Execution avec succès du script '{NB1_NAME}' => Temps d'execution = {time_exec_nb1:.2f} secondes", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
            log_message(msg_log="-" * 3, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # NB2
            #run_cluster_step()       # cluster
            #run_notebook_local(NB3) # local 

            log_message(msg_log="─" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

            # --- Message de succès
            log_message(msg_log=f"[ok]: Pipeline terminé avec succès", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    except Exception as e:
        log_message(level="error", msg_log=f"Pipeline arrêtée : {e}", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

    finally:
        if exec_cmd_docker:
            # --- Arrêt et suppression des ressources créées par 'docker compose up'
            run(["docker", "compose", "down"], stdout=DEVNULL, stderr=DEVNULL)
            log_message(msg_log=f"Arrêt et suppression des ressources créées par 'docker compose up'", file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)
        log_message(msg_log="=" * 95, file_log=True, file_log_dir=LOG_DIR, file_log_name=LOG_FILE_NAME)

# ECF – Concepteur d’Applications de Données (Data Engineer)  
## Titre Professionnel : Data Engineer – RNCP35288  
### Compétences évaluées : C2.1, C2.2, C2.3, C2.4  
## Sujet : Analyse et prédiction de la consommation énergétique des bâtiments publics

---

## Contexte
Ce projet simule une mission de **Data Engineer** au sein d’une agence régionale de transition énergétique.  
L’objectif est de construire un pipeline complet d’**ingestion**, **nettoyage**, **enrichissement**, **analyse** et **visualisation** des consommations (électricité, gaz, eau) de bâtiments publics, enrichies par la météo et les caractéristiques des bâtiments.

---

## Objectifs
- Industrialiser le traitement de données volumineuses (Spark)
- Nettoyer et enrichir des sources hétérogènes (Pandas)
- Calculer des KPI énergétiques et analyser les facteurs d’influence
- Détecter des anomalies et proposer des recommandations actionnables
- Produire des visualisations professionnelles + dashboard exécutif + rapport final

---

## Structure du projet
        ecf_energie
        |   .gitignore
        |   docker-compose.yml
        |   Dockerfile
        |   README.md
        |   requirements.txt
        |   
        +---data
        |       batiments.csv
        |       consommations_raw-test-1000.csv
        |       consommations_raw.csv
        |       meteo_raw.csv
        |       tarifs_energie.csv
        |       
        +---docs
        |       ECF_CONCEPTEUR_APPLI_DONNEES.md
        |      
        |           
        +---logs
        |       02_nettoyage.log
        |       pipeline_global.log
        |       
        +---notebooks
        |       01_exploration_spark.ipynb
        |       02_nettoyage_spark.py
        |       03_agregations_spark.ipynb
        |       04_nettoyage_meteo_pandas.ipynb
        |       05_fusion_enrichissement.ipynb
        |       06_statistiques_descriptives.ipynb
        |       07_analyse_correlations.ipynb
        |       08_detection_anomalies.ipynb
        |       09_visualisations_matplotlib.ipynb
        |       10_visualisations_seaborn.ipynb
        |       11_dashboard_executif.ipynb
        |       
        +---output
        |   |   01_rapport_audit_donnees.md
        |   |   04_meteo_clean.csv
        |   |   05_consommations_enrichies.csv
        |   |   05_consommations_enrichies.parquet
        |   |   07_matrice_correlation.csv
        |   |   07_rapport_insight.md
        |   |   08_anomalies_detectees.csv
        |   |   08_rapport_audit.md
        |   |   presentation_conclusions.odp
        |   |   rapport_synthese.md
        |   |   
        |   +---02_consommations_clean
        |   |            
        |   +---03_consommations_agregees.parquet
        |   |       
        |   +---06_exports
        |   |       comparaison_dpe.csv
        |   |       repartition_dpe.csv
        |   |       stats_par_type_energie.csv
        |   |       tendance_mensuelle.csv
        |   |       top_moins_energivores.csv
        |   |       top_plus_energivores.csv
        |   |       
        |   +---09_figures
        |   |       1_evolution_conso_par_energie.png
        |   |       2_boxplot_conso_par_type_batiment.png
        |   |       3_heatmap_heure_jour_semaine.png
        |   |       4_scatter_temperature_vs_chauffage_regression.png
        |   |       5_bar_conso_par_classe_energetique.png
        |   |       
        |   +---10_figures
        |   |       01_pairplot_conso_par_saison.png
        |   |       02_violin_electricite_par_type_batiment.png
        |   |       03_heatmap_correlation_complete.png
        |   |       04_facetgrid_evolution_mensuelle_top6_communes.png
        |   |       05_jointplot_surface_vs_conso.png
        |   |       06_catplot_conso_par_dpe_et_type.png
        |   |       
        |   +---11_figures
        |           dashboard_executif_2x3.png
        |           
        +---pipeline
        |       run_pipeline_hybride.py
        |       
        +---spark-conf
        |       log4j2.properties
        |       spark-defaults.conf
        |       
        +---utils
                utils_global.py
                utils_logs.py
                utils_resources.py
                __init__.py

## Installation & environnement
### Prérequis
- Python 3.9+ 
- Java 11 (pour Spark)
- PySpark 3.5+
- Pandas / Matplotlib / Seaborn

### Installation

#### Docker
        docker compose up --d --build

#### Environement
        python -m venv .venv
        source .venv/bin/activate          # Linux/Mac
        # .venv\Scripts\activate           # Windows

        pip install -r requirements.txt

## Données (dossier `data/`)
Les données peuvent être régénérées via :

```bash
python generate_data_ecf.py
```

## Exécution du pipeline (ordre recommandé)
        
### Execution du pipeline complet (Recommandé) 
        python -m pipeline.run_pipeline_hybride

### Description des scripts 
#### 1) Exploration Spark (audit qualité)
* Ouvrir et exécuter :

notebooks/01_exploration_spark.ipynb

* Livrables :

audit qualité (Markdown / DataFrame)

#### 2) Nettoyage Spark 
* Exécuter :

        docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /notebooks/02_nettoyage_spark.py


* Livrables :

output/consommations_clean/ (parquet partitionné)

log du traitement (entrées/sorties/rejets)

#### 3) Agrégations Spark (SQL & intensité énergétique)
* Ouvrir et exécuter :

notebooks/03_agregations_spark.ipynb

* Livrables :

output/consommations_agregees.parquet

#### 4) Nettoyage météo (Pandas)

* Ouvrir et exécuter :

notebooks/04_nettoyage_meteo_pandas.ipynb

* Livrables :

output/meteo_clean.csv

#### 5) Fusion + enrichissement (Pandas)

* Ouvrir et exécuter :

notebooks/05_fusion_enrichissement.ipynb

* Livrables :

output/consommations_enrichies.csv

output/consommations_enrichies.parquet

dictionnaire de données

#### 6) Analyse exploratoire (KPI, corrélations, anomalies)

* Ouvrir et exécuter :

notebooks/06_statistiques_descriptives.ipynb → export CSV synthèse

notebooks/07_analyse_correlations.ipynb → output/matrice_correlation.csv + insights

notebooks/08_detection_anomalies.ipynb → output/anomalies_detectees.csv + rapport audit

#### 7) Visualisations & dashboard

* Ouvrir et exécuter :

notebooks/09_visualisations_matplotlib.ipynb → 5 figures

notebooks/10_visualisations_seaborn.ipynb → 6 figures

notebooks/11_dashboard_executif.ipynb → dashboard 2x3

* Livrables :

output/figures/*.png (300 dpi)

output/figures/dashboard_energie.png

#### 8) Rapport final

* Générer / compléter :

output/rapport_synthese.md

Présentation format libre (odp) (5 slides max) : presentation_conclusions.odp


## Auteur

Nom : J. SAIDI

Date : 03-Feb-2026
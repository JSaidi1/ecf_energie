FROM apache/spark:3.5.3

USER root

# # Install docker CLI
# RUN apt-get update \
#  && apt-get install -y docker.io \
#  && rm -rf /var/lib/apt/lists/*

 # Instal other modules
RUN python3 -m pip install --upgrade pip \
 && python3 -m pip install --no-cache-dir \
    psutil \
    pandas \
    tabulate

# # Créer le binaire "python" pour satisfaire l'entrypoint
# RUN ln -sf "$(command -v python3)" /usr/bin/python

# # Rendre pyspark + py4j visibles pour python3
# ENV PYTHONPATH="/:/opt/spark/python:/opt/spark/python/lib/*"

USER spark
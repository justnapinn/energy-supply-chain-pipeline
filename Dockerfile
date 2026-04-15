FROM apache/airflow:3.2.0

# change to root to install Java and dependencies
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# switch back to airflow user before installing python packages
USER airflow
ADD requirements.txt .
RUN pip install -r requirements.txt
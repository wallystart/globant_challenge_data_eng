# -- Software Stack Version
SPARK_VERSION="3.0.2"
HADOOP_VERSION="2.7"
JUPYTERLAB_VERSION="2.1.5"

# -- Removing config file
# rm  ~/.docker/config.json 

# -- Building the Images
docker build \
    -f ./func/Dockerfile \
    -t api_az_function .

docker build \
    -f ./spark-cluster/cluster-base.Dockerfile \
    -t cluster-base .

docker build \
    -f ./spark-cluster/cluster-apache-spark.Dockerfile \
    -t cluster-apache-spark:3.0.2 .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f ./spark-cluster/jupyterlab.Dockerfile \
  -t jupyterlab .
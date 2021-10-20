# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
#ARG BASE_CONTAINER=jupyter/minimal-notebook
# docker build -t ubuntu1604py36
FROM ubuntu:16.04


# Fix DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root
RUN apt update
RUN apt upgrade -y
RUN apt-get install -y python-minimal
RUN apt install -y python-pip
RUN pip install --upgrade pip
RUN pip --no-cache-dir install \
        jupyter \
        ipykernel \
        matplotlib \
        numpy \
        pandas \
        plotly \
        py4j \
        tornado==5.1.1 \
        && \
    python -m ipykernel.kernelspec

ENV CONFIG /root/.jupyter/jupyter_notebook_config.py
ENV CONFIG_IPYTHON /root/.ipython/profile_default/ipython_config.py 

RUN jupyter notebook --generate-config --allow-root && \
    ipython profile create

RUN echo "c.NotebookApp.ip = '0.0.0.0'" >>${CONFIG} && \
    echo "c.NotebookApp.port = 8888" >>${CONFIG} && \
    echo "c.NotebookApp.open_browser = False" >>${CONFIG} && \
    echo "c.NotebookApp.iopub_data_rate_limit=10000000000" >>${CONFIG} && \
    echo "c.MultiKernelManager.default_kernel_name = 'python3'" >>${CONFIG} 

RUN echo "c.InteractiveShellApp.exec_lines = ['%matplotlib inline']" >>${CONFIG_IPYTHON} 

# Copy sample notebooks.
# COPY notebooks /notebooks

# Port
EXPOSE 8888
EXPOSE 6006 

VOLUME /notebooks

# Run Jupyter Notebook
WORKDIR "/notebooks"
CMD ["jupyter","notebook", "--allow-root"]
# Spark dependencies
ENV APACHE_SPARK_VERSION=2.4.5 \
    HADOOP_VERSION=2.7
RUN apt update
RUN apt install -y openjdk-8-jdk openjdk-8-jre && \
 	apt install -y wget && \
	apt install -y tar

RUN rm -rf /var/lib/apt/lists/* 



	    # Using the preferred mirror to download Spark
	    WORKDIR /tmp

	    # hadolint ignore=SC2046
		
	    RUN wget https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
		#https://www.apache.org/dyn/closer.lua/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz 
		RUN tar xvf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz  -C /usr/local --owner root --group root --no-same-owner 
		RUN	rm "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

		WORKDIR /usr/local
		RUN ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" spark

			    # Configure Spark
		ENV SPARK_HOME=/usr/local/spark
		ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip \
			SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
			PATH=$PATH:$SPARK_HOME/bin
		ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
		USER $NB_UID

		WORKDIR $HOME

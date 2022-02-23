FROM ubuntu

RUN apt-get update && \
    apt-get install -y git build-essential

RUN sudo apt install dirmngr gnupg apt-transport-https ca-certificates software-properties-common \
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
    sudo add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/' \
    sudo apt install r-base

RUN git clone https://github.com/apache/arrow-datafusion.git
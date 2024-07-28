FROM core:latest

# Install nfd
#RUN apt update
#RUN apt install -y software-properties-common
#RUN add-apt-repository ppa:named-data/ppa -y
#RUN apt update
#RUN apt install -y nfd ndn-tools

# Install ndn-cxx
# RUN apt update
# RUN apt install -y build-essential git pkg-config python3-minimal libboost-all-dev libssl-dev libsqlite3-dev
# WORKDIR /
# RUN git clone https://github.com/named-data/ndn-cxx.git
# WORKDIR /ndn-cxx
# RUN git checkout ndn-cxx-0.8.1
# RUN ./waf configure
# RUN ./waf
# RUN ./waf install
# RUN ldconfig

# Install ndn-tools
# RUN apt update
# RUN apt install -y libpcap-dev
# WORKDIR /
# RUN git clone https://github.com/named-data/ndn-tools.git
# WORKDIR /ndn-tools
# RUN git checkout ndn-tools-22.12
# RUN ./waf configure
# RUN ./waf
# RUN ./waf install

# Install python bindings
#RUN apt update
#RUN apt install -y python3-pip
#RUN pip3 install python-ndn

#Install wasm_rest dependencies
RUN apt update
RUN pip3 install wasmtime uvicorn fastapi python-multipart requests psutil zeroconf fastapi-pagination readerwriterlock

#Install wasmtime
RUN apt update
RUN apt install curl -y
RUN curl https://wasmtime.dev/install.sh -sSf | bash

#Install bwn-ng
RUN apt update
RUN apt install bwm-ng sysstat -y

RUN echo "custom_config_services_dir = /root/.coregui/custom_services" >> /etc/core/core.conf

ENTRYPOINT ["core-daemon"]

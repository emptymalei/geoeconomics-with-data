FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y software-properties-common vim
RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update

RUN apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
RUN apt-get install -y osmium-tool
RUN apt-get install -y libspatialindex-dev

# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

RUN pip install --upgrade pip setuptools

COPY requirements.txt /
RUN pip install -r requirements.txt --force --ignore-installed

COPY app.tar.gz /
RUN pip install /app.tar.gz --force --ignore-installed

CMD distance_calculator
FROM python:3.8-slim
LABEL maintainer = "Mitra Ardron <mitra@mitra.biz>"
# Haven't finalized on that base, but generally recommended for python over Alpine.
USER root

RUN apt-get update
RUN apt-get -yq install apt-utils gcc

# Check you have assumed prerequisites in the chosen base, if not enable install below
# Usually want to make sure bash is there
# RUN bash --version >/dev/null || apt-get -yq install bash
# If need git - currently don't
# RUN git --version >/dev/null || apt-get -yq install git
#If want to ssh into running server (normally don't)
# RUN ssh -V >/dev/null || apt-get -yq install openssh-server
# Uncomment this to aid development
# RUN ps || apt-get -yq install procps

# Install anything at the OS level, and clean up afterwards
RUN apt-get -yq install supervisor
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN mkdir /data

EXPOSE 5000

COPY . /app
COPY etc /etc
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt
RUN cp /app/sample_docker_config.ini /app/config.ini
CMD [ "/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf" ]

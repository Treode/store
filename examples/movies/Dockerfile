FROM debian:wheezy
MAINTAINER questions@treode.com

RUN \
  apt-get update && \
  apt-get install -y procps vim nginx curl wget unzip openjdk-7-jre-headless

# TODO
#RUN \
#  curl -SL http://d3kbcqa49mib13.cloudfront.net/spark-1.1.1-bin-hadoop2.4.tgz | \
#  tar -xJC /var/lib/spark/

RUN mkdir -p \
  /var/lib/treode/db \
  /var/lib/treode/lib \
  /var/lib/treode/logs \
  /var/lib/treode/www \
  /var/lib/spark

COPY server/target/scala-2.11/movies-server.jar /var/lib/treode/lib/movies-server.jar
COPY webui/dist /var/lib/treode/www

COPY config/nginx /etc/nginx/sites-available/movies
RUN \
  rm /etc/nginx/sites-enabled/default && \
  ln -s /etc/nginx/sites-available/movies /etc/nginx/sites-enabled/movies

COPY config/run.sh /root/run.sh
CMD /root/run.sh

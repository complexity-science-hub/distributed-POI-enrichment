FROM centos:7.4.1708

RUN yum groupinstall -y development
RUN yum install -y epel-release
RUN mkdir /root/.gradle

RUN yum install -y java-1.8.0-openjdk

# https://stackoverflow.com/questions/34911622/dockerfile-set-env-to-result-of-command
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-8.b10.el7_5.x86_64/jre

COPY init.gradle /root/.gradle/

RUN mkdir /root/.ssh
COPY known_hosts /root/.ssh/

CMD ["/bin/bash"]

FROM grafana/grafana:8.2.2
ADD ./provisioning /etc/grafana/provisioning
ADD ./config.ini /etc/grafana/config.ini
ADD ./grafana.ini /etc/grafana/grafana.ini
ADD ./dashboards /var/lib/grafana/dashboards
user root
RUN chown -R 472:472 /etc/grafana/provisioning
RUN chown -R 472:472 /etc/grafana/config.ini
RUN chown -R 472:472 /etc/grafana/grafana.ini
RUN chown -R 472:472 /var/lib/grafana/dashboards
user grafana
EXPOSE 3000/tcp

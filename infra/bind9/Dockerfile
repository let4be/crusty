FROM internetsystemsconsortium/bind9:9.16
COPY named.conf /etc/bind
EXPOSE 53/UDP
CMD [ "/usr/sbin/named", "-4", "-g", "-c", "/etc/bind/named.conf" ]

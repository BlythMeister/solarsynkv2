ARG BUILD_FROM
FROM $BUILD_FROM

# Copy data for add-on
COPY run.sh /
RUN chmod a+x /run.sh

RUN apt-get install openssl

CMD [ "/run.sh" ]
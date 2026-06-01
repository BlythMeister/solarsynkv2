ARG BUILD_FROM="alpine"
FROM $BUILD_FROM

# Copy data for add-on
COPY run.sh /
RUN chmod a+x /run.sh

RUN apk add openssl

CMD [ "/run.sh" ]

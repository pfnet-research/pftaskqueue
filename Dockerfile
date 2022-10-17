FROM gcr.io/distroless/base-debian11
WORKDIR /
COPY pftaskqueue /usr/local/bin/pftaskqueue
ENTRYPOINT ["/usr/local/bin/pftaskqueue"]

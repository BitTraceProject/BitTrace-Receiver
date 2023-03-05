FROM ubuntu:20.04

WORKDIR /bittrace

COPY ./output/receiver-cli /bittrace/

VOLUME ["/root/.bittrace"]

# for receiver
EXPOSE 8080
# for mq
EXPOSE 8081
# for meta
EXPOSE 8082

ENTRYPOINT ["/bittrace/receiver-cli"]

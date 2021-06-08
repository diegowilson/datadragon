FROM rust:1.50

WORKDIR /usr/src/solistener
COPY . .
COPY datadragon-solistener-sa.json /usr/src/solistener/datadragon-solistener-sa.json

RUN apt-get update && apt-get install -y libudev-dev && rm -rf /var/lib/apt/lists/*

RUN cargo install --path .

ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/src/solistener/datadragon-solistener-sa.json"

CMD ["solistener", "--project", "datadragonio", "--dataset", "solana"]

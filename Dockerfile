FROM rust:1.86-alpine3.21

WORKDIR /usr/src/app
COPY . .

RUN cargo install --path .

CMD ["users"]

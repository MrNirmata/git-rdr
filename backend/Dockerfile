# --- ეტაპი 1: კოდის კომპილაცია (Builder) ---
# 👇👇👇 სწორედ ეს ხაზი სწორდება! ვიყენებთ Go-ს 1.24 ვერსიას. 👇👇👇
FROM golang:1.24-alpine AS builder

WORKDIR /app

# დავაკოპიროთ მთლიანი backend დირექტორიის კოდი ერთიანად.
COPY . .

# ჩავტვირთოთ დამოკიდებულებები თითოეული სერვისისთვის.
# go mod tidy შექმნის go.sum ფაილებს, თუ ისინი არ არსებობს.
RUN cd ingestor && go mod tidy && go mod download
RUN cd archiver && go mod tidy && go mod download
RUN cd api && go mod tidy && go mod download

# სამივე სერვისის კომპილაცია
RUN cd ingestor && CGO_ENABLED=0 go build -o /out/ingestor .
RUN cd archiver && CGO_ENABLED=0 go build -o /out/archiver .
RUN cd api && CGO_ENABLED=0 go build -o /out/api .


# კომპილაცია ახალი სერვისისთვის
RUN cd orderbook_manager && CGO_ENABLED=0 go build -o /out/orderbook_manager .

# --- ეტაპი 2: საბოლოო, მცირე ზომის კონტეინერი ---
FROM alpine:latest

WORKDIR /app

# გადმოვიტანოთ მხოლოდ დაკომპილირებული ფაილები builder-დან
COPY --from=builder /out/* .

# გაშვების ბრძანება მითითებული იქნება docker-compose.yml-ში

# AppFlowy Cloud
- Cloud Server for AppFlowy

## Deployment

### Environmental Variables before starting
- you can set it explicitly(below) or in a `.env` file (use `dev.env`) as template
```bash
# authentication key, change this and keep the key safe and secret
GOTRUE_JWT_SECRET=secret_auth_pass

# enabled by default, if you dont want need email confirmation, set to false
GOTRUE_MAILER_AUTOCONFIRM=true

# if you enable mail confirmation, you need to set the SMTP configuration below
GOTRUE_SMTP_HOST=smtp.gmail.com
GOTRUE_SMTP_PORT=465
GOTRUE_SMTP_USER=email_sender@some_company.com
GOTRUE_SMTP_PASS=email_sender_password
GOTRUE_SMTP_ADMIN_EMAIL=comp_admin@@some_company.com

# Change 'localhost:9998' to the public host of machine that is running on.
# This is for email confirmation link
API_EXTERNAL_URL=http://localhost:9998

# Enable Google OAuth2, default: false, quick link for set up:
# https://console.cloud.google.com/apis/credentials
# https://console.cloud.google.com/apis/credentials/consent
GOTRUE_EXTERNAL_GOOGLE_ENABLED=false
GOTRUE_EXTERNAL_GOOGLE_CLIENT_ID=some_id
GOTRUE_EXTERNAL_GOOGLE_SECRET=some_secret
# Change 'localhost:9998' to the public host of machine that is running on.
GOTRUE_EXTERNAL_GOOGLE_REDIRECT_URI=http://localhost:9998/callback
```
- additional settings can be modified in `docker-compose.yml`
## SSL Certificate
- To use your own SSL, replace `certificate.crt` and `private_key.key`
with your own in `nginx/ssl/` directory

### Start Cloud Server
```bash
docker-compose up -d
```

### Ports
Host Server is required to expose the following Ports:
- `443` (https)
- `80`  (http)

## Local Development

### Pre-requisites

You'll need to install:

- [Rust](https://www.rust-lang.org/tools/install)
- [Docker](https://docs.docker.com/get-docker/)

### Configuration
- copy the configurations from `dev.env` to `.env`
- edit the `.env` as required (such as SMTP configurations)

### Run the dependency servers
```bash
docker compose --file docker-compose-dev.yml up -d
```

### Install sqlx-cli
```bash
cargo install sqlx-cli
```

### Run sqlx migration
```bash
sqlx database create
sqlx migrate run
```

### Run the AppFlowy-Cloud server
```bash
cargo run
```

### Run the tests

#### Test
```bash
cargo test
```

### Debugging
- Postgres
```bash
    export PGPASSWORD=password
    psql --host=localhost --username=postgres --port=5433
```
- Redis
```bash
    redis-cli -p 6380
```

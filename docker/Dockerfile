# Usa Python 3.13 sobre Debian Bookworm
FROM python:3.13.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# 1. Dependencias del sistema (gcc y libpq-dev sustituyen lo que buscabas con python3-devel)
RUN apt-get update && apt-get install -y \
    libaio1 \
    libpq-dev \
    gcc \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# 2. Oracle Instant Client 21.12
WORKDIR /opt/oracle
RUN curl -o instantclient.zip https://download.oracle.com/otn_software/linux/instantclient/2112000/instantclient-basic-linux.x64-21.12.0.0.0dbru.zip \
    && unzip instantclient.zip \
    && rm instantclient.zip \
    && echo /opt/oracle/instantclient_21_12 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_21_12:$LD_LIBRARY_PATH"

# 3. Instalar Meltano y el conector de Postgres para la System DB
# Instalamos psycopg2-binary para que Meltano pueda usar PostgreSQL como System DB
RUN pip install --no-cache-dir meltano==4.0.9 psycopg2-binary

WORKDIR /project

# 4. Instalación de plugins
# Copiamos solo el meltano.yml primero.
# Esto es un truco de Docker: si el yml no cambia, no reinstala los plugins cada vez que buildeas.
COPY meltano.yml ./
RUN meltano install

# 5. Copiar TODO el proyecto
# Aquí es donde entran tus carpetas 'transform/', 'extract/', etc.
COPY . ./

# 6. Preparar dbt (Opcional pero recomendado)
# Esto descarga los paquetes de dbt (si usas alguno) para que la imagen ya los tenga.
RUN meltano invoke dbt-postgres:deps || true

ENTRYPOINT ["meltano"]
# 1. Imagen base
FROM python:3.10-slim-bullseye

# 2. Establecer el directorio de trabajo
WORKDIR /app

# 3. Instalar el JDK (Java)
#    ¡CAMBIO! Cambiamos de openjdk-11 a openjdk-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# 4. Establecer la variable de entorno JAVA_HOME
#    ¡CAMBIO! Apuntamos a la nueva ruta de Java 17
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# 5. Instalar PySpark
RUN pip install pyspark

# 6. Copiar tus archivos de datos y el script al contenedor
#    (Nota: Si usas el docker-compose.yml con volúmenes,
#     estas líneas no son estrictamente necesarias,
#     pero es una buena práctica dejarlas)
COPY Cabs.csv .
COPY Drivers.csv .
COPY main.py .

# 7. El comando que se ejecutará cuando el contenedor inicie
CMD ["python3", "main.py"]
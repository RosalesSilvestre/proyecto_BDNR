import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, to_date, substring, lit,
    count, min, max, desc, asc, sum, avg,
    when, lpad, rpad, trim, isnull,
    current_date, add_months, quarter,
    floor, regexp_replace, length
)

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Solucion Tarea 3") \
    .getOrCreate()

print("SparkSession iniciada.")

## Tarea 3 (Archivo Cabs.csv)

# a)
print("--- Inciso 2.a ---")

cabs_raw = spark.read.csv("Cabs.csv", header=True, inferSchema=True, sep=',')

cabs = cabs_raw.withColumnRenamed("VehicleYear", "Year")

cabs.show()
cabs.printSchema()

# b)
print("--- Inciso 2.b ---")
# 
taxis_con_taxi = cabs.filter(col("Name").contains("TAXI"))
taxis_con_taxi.show()
print(f"Total con 'TAXI' en el nombre: {taxis_con_taxi.count()}")

# c)
print("--- Inciso 2.c ---")
# 
con_licencia = cabs.filter(col("PermitLicenseNumber").isNotNull())
print(f"Total con 'PermitLicenseNumber' significativo: {con_licencia.count()}")

# d)
print("--- Inciso 2.d ---")
# 
sin_t_2025 = cabs.filter(
    (col("Year") == 2025) &
    (~col("CabNumber").startswith("T"))
)
print(f"Total (Año 2025, CabNumber no inicia con T): {sin_t_2025.count()}")

# e)
print("--- Inciso 2.e ---")
# 
# "década pasada" (asumiendo 2010-2019)
no_decada_pasada = cabs.filter(
    (col("Year") < 2010) | (col("Year") > 2019)
)
print(f"Total que no son de la década pasada (2010-2019): {no_decada_pasada.count()}")

# f)
print("--- Inciso 2.f ---")
# 
c_con_l = cabs.filter(
    col("Name").startswith("C") &
    (substring(col("Name"), 5, 1) == 'L')
)
c_con_l.show()
print(f"Total (Inicia con C, L en 5ta pos): {c_con_l.count()}")

# g)
print("--- Inciso 2.g ---")
# 
entre_f_t = cabs.filter(
    substring(col("Name"), 1, 1).between('F', 'T')
).select("Name", "Year")

entre_f_t.show()
print(f"Total (Nombre inicia entre F y T): {entre_f_t.count()}")

# h)
print("--- Inciso 2.h ---")
# 
# Filtrar solo los que tienen dígitos
solo_digitos = cabs.filter(col("VehicleLicenseNumber").rlike("^[0-9]+$"))

# Convertir a numérico (BigInt) para comparar correctamente
solo_digitos_num = solo_digitos.withColumn("LicNum", col("VehicleLicenseNumber").cast("bigint"))

# Encontrar el máximo
max_lic = solo_digitos_num.agg(max("LicNum")).collect()[0][0]

# Mostrar los taxis que tienen esa licencia máxima
taxis_max_lic = solo_digitos_num.filter(col("LicNum") == max_lic)
taxis_max_lic.show()

# i)
print("--- Inciso 2.i ---")
# 
conteo_por_anio = cabs.groupBy("Year").count().orderBy("Year")
conteo_por_anio.show()

# j)
print("--- Inciso 2.j ---")
# 
# Reutilizando el DF 'solo_digitos_num' del inciso h
lic_por_anio = solo_digitos_num.groupBy("Year") \
    .agg(
        min("LicNum").alias("MinLicense"),
        max("LicNum").alias("MaxLicense")
    ) \
    .orderBy("Year")
    
lic_por_anio.show()

# k)
print("--- Inciso 2.k ---")
# 
conteo_f_i = cabs.withColumn("Inicial", substring(col("Name"), 1, 1)) \
    .filter(col("Inicial").between('F', 'I')) \
    .groupBy("Inicial") \
    .count() \
    .orderBy("Inicial")

conteo_f_i.show()

# l)
print("--- Inciso 2.l ---")
# 
# Empleando el resultado anterior (conteo_f_i)
print("Letra inicial con el MÍNIMO de vehículos:")
conteo_f_i.orderBy(col("count").asc()).show(1)

print("Letra inicial con el MÁXIMO de vehículos:")
conteo_f_i.orderBy(col("count").desc()).show(1)

# m)
print("--- Inciso 2.m ---")
# 
# (Rango 2007-2025)
decadas_df = cabs.withColumn("Decada",
    when(col("Year").between(2000, 2009), "2000s")
    .when(col("Year").between(2010, 2019), "2010s")
    .when(col("Year").between(2020, 2029), "2020s")
    .otherwise("Otra")
)

conteo_decadas = decadas_df.groupBy("Decada").count().orderBy("Decada")
conteo_decadas.show()

# n)
print("--- Inciso 2.n ---")
# 
decadas_df_n = cabs.withColumn("Decada",
    when(col("Year").between(2000, 2009), 2000)
    .when(col("Year").between(2010, 2019), 2010)
    .when(col("Year").between(2020, 2029), 2020)
    .otherwise(None)
).select("Name", "Year", "Decada")

decadas_df_n.show()

## Preguntas SQL (Archivo Cabs.csv)

# 
# Registrar el DataFrame como vista temporal para usar SQL
cabs.createOrReplaceTempView("cabs_view")
print("Vista 'cabs_view' registrada para SQL.")

# o)
print("--- Inciso 2.o ---")
# 
sql_o = """
SELECT COUNT(*) 
FROM cabs_view 
WHERE CabNumber NOT LIKE '%C'
"""
spark.sql(sql_o).show()

# p)
print("--- Inciso 2.p ---")
# 
# "esta década" (asumiendo 2020-2029)
sql_p = """
SELECT COUNT(*)
FROM cabs_view
WHERE Year >= 2020 AND VehicleLicenseNumber RLIKE '[^0-9]'
"""
spark.sql(sql_p).show()

# q)
print("--- Inciso 2.q ---")
# 
sql_q = """
SELECT Year, COUNT(*) as Total
FROM cabs_view
GROUP BY Year
HAVING Total > 700
ORDER BY Year
"""
spark.sql(sql_q).show()


## Continuación (Archivo Drivers.csv)

# 
# Cargar Drivers.csv. Asumiré que las columnas se llaman 'Name' y 'ExpirationDate'
# basado en las preguntas de la Parte 1.
drivers = spark.read.csv("Drivers.csv", header=True, inferSchema=False, sep=',')

# Convertir la columna de fecha usando el formato de 
drivers_typed = drivers.withColumn(
    "ExpirationDate_dt", 
    to_date(col("ExpirationDate"), 'MM/dd/yyyy')
)
print("Drivers.csv cargado y fecha convertida.")
drivers_typed.printSchema()

# r)
print("--- Inciso 2.r ---")
# "siguiente año" (estamos en 2025 ) es 2026.
# "segundo trimestre" (Q2)
q2_2026 = drivers_typed.filter(
    (year(col("ExpirationDate_dt")) == 2026) &
    (quarter(col("ExpirationDate_dt")) == 2)
)
print(f"Total que vencen Q2 2026: {q2_2026.count()}")

# s)
print("--- Inciso 2.s ---")
# 
vence_en_1_anio_dinamico = drivers_typed.filter(
    col("ExpirationDate_dt") == add_months(current_date(), 12)
)

print("Conductores que vencen exactamente en un año (calculado dinámicamente):")
vence_en_1_anio_dinamico.show()


# Detener la sesión de Spark
spark.stop()
print("SparkSession detenida.")
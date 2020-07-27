// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val myDataSchema = StructType(
Array(
  StructField("id", DecimalType(26,0), true),
  StructField("anio", IntegerType, true),
  StructField("mes", IntegerType, true),
  StructField("provincia", IntegerType, true),
  StructField("canton", IntegerType, true),
  StructField("area", StringType, true),
  StructField("genero", StringType, true),
  StructField("edad", IntegerType, true),
  StructField("estado_civil", StringType, true),
  StructField("nivel_de_instruccion", StringType, true),
  StructField("etnia", StringType, true),
  StructField("ingreso_laboral", IntegerType, true),
  StructField("condicion_actividad", StringType, true),
  StructField("sectorizacion", StringType, true),
  StructField("grupo_ocupacion", StringType, true),
  StructField("rama_actividad", StringType, true),
  StructField("factor_expansion", DoubleType, true)
)); 

// COMMAND ----------

val data = spark
.read
.format("csv")
.schema(myDataSchema)
.option("header", "true")
.option("sep", ",")
.load("/your/location/mydata2/part-00000-tid-1661806583519070719-654a6859-0955-4fcb-9026-4ed85cca63ea-4007-1-c000.csv")

// COMMAND ----------

// DBTITLE 1,Filtrado de Outliers
// se otbienen los cuartiles
val dfIngresoLaboral = data.select("ingreso_laboral").where($"ingreso_laboral".isNotNull)
val cuartiles = data.stat.approxQuantile("ingreso_laboral", Array(0.25,0.75), 0.0)
val q1 = cuartiles(0)
val q3 = cuartiles(1)
// se calcula el intercuartil
val iqr = q3 - q1
// se calculan los limites
val inferiorIQR = q1 - 1.5 *iqr
val superiorIQR = q3 + 1.5 *iqr
// se hace el filtrado
val valoresMenoresInferior = dfIngresoLaboral.where($"ingreso_laboral" < inferiorIQR)
val valoresMayoresSuperior = dfIngresoLaboral.where($"ingreso_laboral" > superiorIQR)
// Resultados
val dataFinal = data.where($"ingreso_laboral" > inferiorIQR && $"ingreso_laboral" < superiorIQR)

// COMMAND ----------

// DBTITLE 1,Join de provincias y cantones
// Se obtiene el ds de la data provincias
val provincias = spark
.read
.option("header", "true")
.option("delimiter", ";")
.option("inferSchema", "true")
.csv("/FileStore/tables/dataProvincias-2.csv")
// Se obtiene el ds de la data cantones
val cantones = spark
.read
.option("header", "true")
.option("delimiter", ";")
.option("inferSchema", "true")
.csv("/FileStore/tables/dataCantones-2.csv")

// Se crean los dataset con las columnas respectivas a provincia y canton
val data1 = dataFinal.join(provincias, "provincia")
val data2 = data1.join(cantones, "canton")

// COMMAND ----------



// COMMAND ----------

// DBTITLE 1,¿Cuales son las Etnias encuestadas más comunes  por año?
display(dataFinal.groupBy("anio").pivot("etnia").count().orderBy($"anio"))

// COMMAND ----------

// DBTITLE 1,¿Cual es el promedio de edad de etnias encuestadas por año?
display(dataFinal.groupBy("anio").pivot("etnia").agg(round(avg("edad")).cast(IntegerType)).orderBy("anio"))

// COMMAND ----------

// DBTITLE 1,¿Cual es el numero de cada etnia que pertenece a un grupo de ocupacion en especifico?
display(dataFinal.groupBy("grupo_ocupacion").pivot("etnia").count().orderBy($"afroecuatoriano".desc,
                                                                   $"blanco".desc,
                                                                   $"indígena".desc,
                                                                   $"mestizo".desc,
                                                                   $"montubio".desc,
                                                                   $"mulato".desc,
                                                                   $"negro".desc,
                                                                   $"otro".desc))

// COMMAND ----------

// DBTITLE 1,¿Cual es el numero de cada etnia que pertenece a un estado civil en especifico?
display(dataFinal.groupBy("estado_civil").pivot("etnia").count)

// COMMAND ----------

// DBTITLE 1,¿Cual es el numero de cada etnia que pertenece a un Nivel de Instruccion en especifico?
display(dataFinal.groupBy("nivel_de_instruccion").pivot("etnia").count)

// COMMAND ----------

// DBTITLE 1,¿Cual es el ingreso promedio en cada rama actividad según etnias?
display( dataFinal.groupBy("rama_actividad").pivot("etnia").agg(round(avg("ingreso_laboral")).cast(IntegerType)).orderBy($"rama_actividad".desc))

// COMMAND ----------

// DBTITLE 1,¿Cual es el numero de cada etnia que pertenece a una Condición de actividad en especifico?
display(dataFinal.groupBy("condicion_actividad").pivot("etnia").count)

// COMMAND ----------

// DBTITLE 1,EDA de los ultimos años en base a la provincia de Loja
val dataLoja19= dataFinal.where($"provincia" === 11 && $"anio" === 2019)
val dataLoja15 = dataFinal.where($"provincia" === 11 && $"anio" === 2015)
val dataLoja = dataFinal.where($"provincia" === 11 && ($"anio" === 2019 || $"anio" === 2015))

// COMMAND ----------

// DBTITLE 1,Distribución de niveles de instrucción en Loja con respecto al año 2015
display(dataLoja15.groupBy("nivel_de_instruccion").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Distribución de niveles de instrucción en Loja con respecto al año 2019
display(dataLoja19.groupBy("nivel_de_instruccion").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Ramas de actividad distribuidas en la provincia de Loja en base al genero del año 2015
display(dataLoja15.groupBy("rama_actividad").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Ramas de actividad distribuidas en la provincia de Loja en base al genero del año 2019
display(dataLoja19.groupBy("rama_actividad").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Area en la que pertencen hombres y mujeres en la provincia de Loja, en el año 2015
display(dataLoja15.groupBy("area").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Area en la que pertencen hombres y mujeres en la provincia de Loja, en el año 2019
display(dataLoja19.groupBy("area").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,El salario promedio por etnia en la provincia de Loja, en el año 2015
display(dataLoja15.select($"etnia", $"ingreso_laboral").groupBy("etnia").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")))

// COMMAND ----------

// DBTITLE 1,El salario promedio por etnia en la provincia de Loja, en el año 2019
display(dataLoja19.select($"etnia", $"ingreso_laboral").groupBy("etnia").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")))

// COMMAND ----------

display(dataLoja.groupBy(col("etnia").as("Etnia")).pivot("anio").count().orderBy("Etnia"))

// COMMAND ----------

display(dataLoja.select($"etnia", $"ingreso_laboral").where($"etnia" === "Mulato" && $"anio" === 2019))

// COMMAND ----------

display(dataLoja.where($"etnia" === "Afroecuatoriano" && $"anio" === 2019).groupBy(col("anio").as("Año")).pivot("etnia").avg("ingreso_laboral").orderBy("Año"))


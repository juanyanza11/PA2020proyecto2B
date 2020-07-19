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
.schema(myDataSchema)
.option("header", "true")
.option("delimiter", "\t")
.csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameNaFunctions
val dataLimpieza1 = data.na.replace ("genero", Map("1 - Hombre" -> "Hombre", "2 - Mujer" -> "Mujer"))

val dataLimpieza2 = dataLimpieza1.na.replace ("etnia", Map("1 - Indígena" -> "Indígena", "4 - Mulato" -> "Mulato", "8 - Otro" -> "Otro", "2 - Afroecuatoriano" -> "Afroecuatoriano", "7 - Blanco" -> "Blanco", "3 - Negro" -> "Negro", "5 - Montubio" -> "Montubio", "6 - Mestizo" -> "Mestizo"))

val dataLimpieza3 = dataLimpieza2.na.replace ("estado_civil", Map("4 - Viudo(a)" -> "Viudo(a)", "2 - Separado(a)" -> "Separado(a)", "6 - Soltero(a)" -> "Soltero(a)",
"5 - Unión libre" -> "Unión libre", "1 - Casado(a)" -> "Casado(a)", "3 - Divorciado(a)" -> "Divorciado(a)"))

val dataLimpieza4 = dataLimpieza3.na.replace ("area", Map("1 - Urbana" -> "Urbana", "2 - Rural" -> "Rural"))

val dataLimpieza5 = dataLimpieza4.na.replace ("nivel_de_instruccion", Map("01 - Ninguno" -> "Ninguno", "02 - Centro de alfabetización" -> "Centro de alfabetización",
"04 - Primaria" -> "Primaria", "05 - Educación Básica" -> "Educación Básica", "06 - Secundaria" -> "Secundaria", "07 - Educación  Media" -> "Educación  Media",
"08 - Superior no universitario" -> "Superior no universitario", "09 - Superior Universitario" -> "Superior Universitario", "10 - Post-grado" -> "Post-grado"))

val dataLimpieza6 = dataLimpieza5.na.replace ("condicion_actividad", Map("7 - Desempleo abierto" -> "Desempleo abierto", "2 - Subempleo por insuficiencia de tiempo de trabajo" -> "Subempleo por insuficiencia de tiempo de trabajo", "1 - Empleo Adecuado/Pleno" -> "Empleo Adecuado/Pleno", "6 - Empleo no clasificado" -> "Empleo no clasificado", "8 - Desempleo oculto" -> "Desempleo oculto", "3 - Subempleo por insuficiencia de ingresos" -> "Subempleo por insuficiencia de ingresos", 
"5 - Empleo no remunerado" -> "Empleo no remunerado", "4 - Otro empleo no pleno" -> "Otro empleo no pleno"))

val dataLimpieza71 = dataLimpieza6.na.fill("N.D", Seq("sectorizacion"))
val dataLimpieza72 = dataLimpieza71.na.replace ("sectorizacion", Map("2 - Sector Informal" -> "Sector Informal", "4 - No Clasificados por Sector" -> "No Clasificados por Sector", "3 - Empleo Doméstico" -> "Empleo Doméstico", "1 - Sector Formal" -> "Sector Formal"))

val dataLimpieza81 = dataLimpieza72.na.fill("N.D", Seq("grupo_ocupacion"))
val dataLimpieza82 = dataLimpieza81.na.replace ("grupo_ocupacion", Map("01 - Personal direct./admin. pública y empresas" -> "Personal direct./admin. pública y empresas", "02 - Profesionales científicos e intelectuales" -> "Profesionales científicos e intelectuales", "03 - Técnicos y profesionales de nivel medio" -> "Técnicos y profesionales de nivel medio", "04 - Empleados de oficina" -> "Empleados de oficina", "05 - Trabajad. de los servicios y comerciantes" -> "Trabajad. de los servicios y comerciantes", "06 - Trabajad. calificados agropecuarios y pesqueros" -> "Trabajad. calificados agropecuarios y pesqueros", "07 - Oficiales operarios y artesanos" -> 
"Oficiales operarios y artesanos", "08 - Operadores de instalac. máquinas y montad." -> "Operadores de instalac. máquinas y montad", "09 - Trabajadores no calificados, ocupaciones elementales" -> "Trabajadores no calificados, ocupaciones elementales", "10 - Fuerzas Armadas" -> "Fuerzas Armadas", "99 - No especificado" -> 
"No especificado"))

val dataLimpieza91 = dataLimpieza82.na.fill("N.D", Seq("rama_actividad"))
val dataFinal = dataLimpieza91.na.replace ("rama_actividad", Map("01 - A. Agricultura, ganadería caza y silvicultura y pesca" -> "Agricultura, ganadería caza y silvicultura y pesca", "02 - B. Explotación de minas y canteras" -> "Explotación de minas y canteras", "03 - C. Industrias manufactureras" -> "Industrias manufactureras", "04 - D. Suministros de electricidad, gas, aire acondicionado" -> "Suministros de electricidad, gas, aire acondicionado", "05 - E. Distribución de agua, alcantarillado" -> "Distribución de agua, alcantarillado", "06 - F. Construcción" -> "Construcción", "07 - G. Comercio, reparación vehículos" -> "Comercio, reparación vehículos", 
"08 - H. Transporte y almacenamiento" -> "Transporte y almacenamiento", "09 - I. Actividades de alojamiento y servicios de comidas" -> "Actividades de alojamiento y servicios de comida", "10 - J. Información y comunicación" -> "Información y comunicación", "22 - No especificado" -> "No especificado", "11 - K. Actividades financieras y de seguros" -> "Actividades financieras y de seguros", "12 - L. Actividades inmobiliarias" -> "Actividades inmobiliarias", "13 - M. Actividades profesionales, científicas y técnicas" -> "Actividades profesionales, científicas y técnicas", "14 - N. Actividades y servicios administrativos y de apoyo" -> "Actividades y servicios administrativos y de apoyo", "15 - O .Administración pública, defensa y seguridad social" -> "Administración pública, defensa y seguridad social", "16 - P. Enseñanza" -> "Enseñanza", "17 - Q. Actividades, servicios sociales y de salud" -> "Actividades, servicios sociales y de salud", "18 - R. Artes, entretenimiento y recreación" -> "Artes, entretenimiento y recreación", "19 - S. Otras actividades de servicios" -> "Otras actividades de servicios", "20 - T  Actividades en hogares privados con servicio doméstico" -> "Actividades en hogares privados con servicio doméstico", "21 - U  Actividades de organizaciones extraterritoriales" -> "Actividades de organizaciones extraterritoriales"))


// COMMAND ----------

// DBTITLE 1,Promedio de edad de etnias encuestadas por año
display(dataFinal.groupBy("anio").pivot("etnia").agg(round(avg("edad")).cast(IntegerType)).orderBy("anio"))

// COMMAND ----------

// DBTITLE 1,Etnias encuestadas más comunes distribuidas por año
display(dataFinal.groupBy("anio", "etnia").count().orderBy($"anio", $"count".desc))

// COMMAND ----------

display(dataFinal.groupBy("anio").pivot("etnia").count().orderBy($"anio"))

// COMMAND ----------

// DBTITLE 1,Distribución de niveles de instrucción en Loja con respecto al año 2019
val dataLoja11 = dataFinal.where($"provincia" === 11 && $"anio" === 2019)

display(dataLoja11.groupBy("nivel_de_instruccion").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Distribución de niveles de instrucción en Loja con respecto al año 2015
val dataLoja12 = dataFinal.where($"provincia" === 11 && $"anio" === 2015)

display(dataLoja12 .groupBy("nivel_de_instruccion").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Ramas de actividad distribuidas en la provincia de Loja en base al genero del año 2015
val dataLoja21 = dataFinal.where($"provincia" === 11 && $"anio" === 2015)
display(dataLoja21.groupBy("rama_actividad").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

// DBTITLE 1,Ramas de actividad distribuidas en la provincia de Loja en base al genero del año 2019
val dataLoja22 = dataFinal.where($"provincia" === 11 && $"anio" === 2019)
display(dataLoja22.groupBy("rama_actividad").pivot("genero").count().orderBy($"hombre".desc, $"mujer".desc))

// COMMAND ----------

display(dataFinal.groupBy("grupo_ocupacion").pivot("etnia").count().orderBy($"afroecuatoriano".desc,
                                                                   $"blanco".desc,
                                                                   $"indígena".desc,
                                                                   $"mestizo".desc,
                                                                   $"montubio".desc,
                                                                   $"mulato".desc,
                                                                   $"negro".desc,
                                                                   $"otro".desc))


// COMMAND ----------

// DBTITLE 1,Distribución de grupos de ocupación por etnias Mestizo - Indigena - Negro - Afroecuatoriano
display(dataFinal.groupBy("grupo_ocupacion").pivot("etnia").count().orderBy($"afroecuatoriano".desc,
                                                                   $"blanco".desc,
                                                                   $"indígena".desc,
                                                                   $"mestizo".desc,
                                                                   $"montubio".desc,
                                                                   $"mulato".desc,
                                                                   $"negro".desc,
                                                                   $"otro".desc))

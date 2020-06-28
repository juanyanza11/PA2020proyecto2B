// Databricks notebook source
// DBTITLE 1,Esquema Data
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

// DBTITLE 1,Creacion DataFrame
val data = spark
.read
.schema(myDataSchema)
.option("header", "true")
.option("delimiter", "\t")
.csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

// DBTITLE 1,Limpieza DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameNaFunctions
val dataLimpieza1 = data.na.replace ("genero", Map("1 - Hombre" -> "Hombre", "2 - Mujer" -> "Mujer"))

val dataLimpieza2 = dataLimpieza1.na.replace ("etnia", Map("1 - Indígena" -> "Indígena", "4 - Mulato" -> "Mulato", "8 - Otro" -> "Otro", "2 - Afroecuatoriano" -> "Afroecuatoriano", "7 - Blanco" -> "Blanco", "3 - Negro" -> "Negro", "5 - Montubio" -> "Montubio", "6 - Mestizo" -> "Mestizo"))

val dataLimpieza3 = dataLimpieza2.na.replace ("estado_civil", Map("4 - Viudo(a)" -> "Viudo(a)", "2 - Separado(a)" -> "Separado(a)", "6 - Soltero(a)" -> "Soltero(a)", "5 - Unión libre" -> "Unión libre", "1 - Casado(a)" -> "Casado(a)", "3 - Divorciado(a)" -> "Divorciado(a)"))

val dataLimpieza4 = dataLimpieza3.na.replace ("area", Map("1 - Urbana" -> "Urbana", "2 - Rural" -> "Rural"))

val dataLimpieza5 = dataLimpieza4.na.replace ("nivel_de_instruccion", Map("01 - Ninguno" -> "Ninguno", "02 - Centro de alfabetización" -> "Centro de alfabetización","04 - Primaria" -> "Primaria", "05 - Educación Básica" -> "Educación Básica", "06 - Secundaria" -> "Secundaria", "07 - Educación  Media" -> "Educación  Media", "08 - Superior no universitario" -> "Superior no universitario", "09 - Superior Universitario" -> "Superior Universitario", "10 - Post-grado" -> "Post-grado"))

val dataLimpieza6 = dataLimpieza5.na.replace ("condicion_actividad", Map("7 - Desempleo abierto" -> "Desempleo abierto", "2 - Subempleo por insuficiencia de tiempo de trabajo" -> "Subempleo por insuficiencia de tiempo de trabajo", "1 - Empleo Adecuado/Pleno" -> "Empleo Adecuado/Pleno", "6 - Empleo no clasificado" -> "Empleo no clasificado", "8 - Desempleo oculto" -> "Desempleo oculto", "3 - Subempleo por insuficiencia de ingresos" -> "Subempleo por insuficiencia de ingresos",  "5 - Empleo no remunerado" -> "Empleo no remunerado", "4 - Otro empleo no pleno" -> "Otro empleo no pleno"))

val dataLimpieza71 = dataLimpieza6.na.fill("N.D", Seq("sectorizacion"))
val dataLimpieza72 = dataLimpieza71.na.replace ("sectorizacion", Map("2 - Sector Informal" -> "Sector Informal", "4 - No Clasificados por Sector" -> "No Clasificados por Sector", "3 - Empleo Doméstico" -> "Empleo Doméstico", "1 - Sector Formal" -> "Sector Formal"))

val dataLimpieza81 = dataLimpieza72.na.fill("N.D", Seq("grupo_ocupacion"))
val dataLimpieza82 = dataLimpieza81.na.replace ("grupo_ocupacion", Map("01 - Personal direct./admin. pública y empresas" -> "Personal direct./admin. pública y empresas", "02 - Profesionales científicos e intelectuales" -> "Profesionales científicos e intelectuales", "03 - Técnicos y profesionales de nivel medio" -> "Técnicos y profesionales de nivel medio", "04 - Empleados de oficina" -> "Empleados de oficina", "05 - Trabajad. de los servicios y comerciantes" -> "Trabajad. de los servicios y comerciantes", "06 - Trabajad. calificados agropecuarios y pesqueros" -> "Trabajad. calificados agropecuarios y pesqueros", "07 - Oficiales operarios y artesanos" -> 
"Oficiales operarios y artesanos", "08 - Operadores de instalac. máquinas y montad." -> "Operadores de instalac. máquinas y montad", "09 - Trabajadores no calificados, ocupaciones elementales" -> "Trabajadores no calificados, ocupaciones elementales", "10 - Fuerzas Armadas" -> "Fuerzas Armadas", "99 - No especificado" -> 
"No especificado"))

val dataLimpieza91 = dataLimpieza82.na.fill("N.D", Seq("rama_actividad"))
val dataFinal = dataLimpieza91.na.replace ("rama_actividad", Map("01 - A. Agricultura, ganadería caza y silvicultura y pesca" -> "Agricultura, ganadería caza y silvicultura y pesca", "02 - B. Explotación de minas y canteras" -> "Explotación de minas y canteras", "03 - C. Industrias manufactureras" -> "Industrias manufactureras", "04 - D. Suministros de electricidad, gas, aire acondicionado" -> "Suministros de electricidad, gas, aire acondicionado", "05 - E. Distribución de agua, alcantarillado" -> "Distribución de agua, alcantarillado", "06 - F. Construcción" -> "Construcción", "07 - G. Comercio, reparación vehículos" -> "Comercio, reparación vehículos", 
"08 - H. Transporte y almacenamiento" -> "Transporte y almacenamiento", "09 - I. Actividades de alojamiento y servicios de comidas" -> "Actividades de alojamiento y servicios de comida", "10 - J. Información y comunicación" -> "Información y comunicación", "22 - No especificado" -> "No especificado", "11 - K. Actividades financieras y de seguros" -> "Actividades financieras y de seguros", "12 - L. Actividades inmobiliarias" -> "Actividades inmobiliarias", "13 - M. Actividades profesionales, científicas y técnicas" -> "Actividades profesionales, científicas y técnicas", "14 - N. Actividades y servicios administrativos y de apoyo" -> "Actividades y servicios administrativos y de apoyo", "15 - O .Administración pública, defensa y seguridad social" -> "Administración pública, defensa y seguridad social", "16 - P. Enseñanza" -> "Enseñanza", "17 - Q. Actividades, servicios sociales y de salud" -> "Actividades, servicios sociales y de salud", "18 - R. Artes, entretenimiento y recreación" -> "Artes, entretenimiento y recreación", "19 - S. Otras actividades de servicios" -> "Otras actividades de servicios", "20 - T  Actividades en hogares privados con servicio doméstico" -> "Actividades en hogares privados con servicio doméstico", "21 - U  Actividades de organizaciones extraterritoriales" -> "Actividades de organizaciones extraterritoriales"))

dataFinal.show()

// COMMAND ----------

// DBTITLE 1,Exportamos data final

dataFinal.write.csv("/FileStore/tables/DataFinal.csv")

// COMMAND ----------

// DBTITLE 1,DataFrames de Etnias
val dataMulato = dataFinal.where($"etnia" === "Mulato")
val dataOtro = dataFinal.where($"etnia" === "Otro")
val dataIndigena = dataFinal.where($"etnia" === "Indígena")
val dataMontubio = dataFinal.where($"etnia" === "Montubio")
val dataBlanco = dataFinal.where($"etnia" === "Blanco")
val dataNegro = dataFinal.where($"etnia" === "Negro")
val dataMestizo = dataFinal.where($"etnia" === "Mestizo")
val dataAfro = dataFinal.where($"etnia" === "Afroecuatoriano")

// COMMAND ----------

// DBTITLE 1,2.1 Promedio de edad por cada etnia

dataMulato.select(avg("edad").as("Promedio edad (Mulato)")).show()
dataOtro.select(avg("edad").as("Promedio edad (Otro)")).show()
dataIndigena.select(avg("edad").as("Promedio edad (Indigena)")).show()
dataMontubio.select(avg("edad").as("Promedio edad (Montubio)")).show()
dataBlanco.select(avg("edad").as("Promedio edad (Blanco)")).show()
dataNegro.select(avg("edad").as("Promedio edad (Negro)")).show()
dataMestizo.select(avg("edad").as("Promedio edad (Mestizo)")).show()
dataAfro.select(avg("edad").as("Promedio edad (Afro)")).show()



// COMMAND ----------

// DBTITLE 1,2.2 Promedio del Nivel de instrucción en las etnias Mestizo - Indígena

println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Secundaria").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Secundaria"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Ninguno").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Ninguno"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Centro de alfabetización").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Centro de alfabetización"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Primaria").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Primaria"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Educación Básica").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Educación Básica"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Educación  Media").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Educación  Media"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Superior no universitario").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Superior no universitario"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Superior Universitario").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Superior Universitario"))
println((f"${(dataMestizo.where($"nivel_de_instruccion" === "Post-grado").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Post-grado"))
println("-----------------------------------")
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Secundaria").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Secundaria"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Ninguno").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Ninguno"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Centro de alfabetización").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Centro de alfabetización"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Primaria").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Primaria"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Educación Básica").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Educación Básica"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Educación  Media").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Educación  Media"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Superior no universitario").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Superior no universitario"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Superior Universitario").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Superior Universitario"))
println((f"${(dataIndigena.where($"nivel_de_instruccion" === "Post-grado").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Post-grado"))

// COMMAND ----------

// DBTITLE 1,2.3 Promedio del Estado Civil en las etnias Mestizo - Indígena
println((f"${(dataMestizo.where($"estado_civil" === "Viudo(a)").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Viudo(a)"))
println((f"${(dataMestizo.where($"estado_civil" === "Separado(a)").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Separado(a)"))
println((f"${(dataMestizo.where($"estado_civil" === "Soltero(a)").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Soltero(a)"))
println((f"${(dataMestizo.where($"estado_civil" === "Unión libre").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Unión libre"))
println((f"${(dataMestizo.where($"estado_civil" === "Casado(a)").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Casado(a)"))
println((f"${(dataMestizo.where($"estado_civil" === "Divorciado(a)").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Divorciado(a)"))
println("-----------------------------------")
println((f"${(dataIndigena.where($"estado_civil" === "Viudo(a)").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Viudo(a)"))
println((f"${(dataIndigena.where($"estado_civil" === "Separado(a)").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Separado(a)"))
println((f"${(dataIndigena.where($"estado_civil" === "Soltero(a)").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Soltero(a)"))
println((f"${(dataIndigena.where($"estado_civil" === "Unión libre").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Unión libre"))
println((f"${(dataIndigena.where($"estado_civil" === "Casado(a)").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Casado(a)"))
println((f"${(dataIndigena.where($"estado_civil" === "Divorciado(a)").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Divorciado(a)"))


// COMMAND ----------

// DBTITLE 1,2.4 Numero de encuestados por año según las etnias Mestizo - Indígena
println((f"${(dataMestizo.where($"anio" === 2015).count)}%d MESTIZOS encuestados en el 2015 "))
println((f"${(dataMestizo.where($"anio" === 2016).count)}%d MESTIZOS encuestados en el 2016 "))
println((f"${(dataMestizo.where($"anio" === 2017).count)}%d MESTIZOS encuestados en el 2017 "))
println((f"${(dataMestizo.where($"anio" === 2018).count)}%d MESTIZOS encuestados en el 2018 "))
println((f"${(dataMestizo.where($"anio" === 2019).count)}%d MESTIZOS encuestados en el 2019 "))
println((f"${(dataMestizo.count)}%d TOTAL MESTIZOS encuestados"))
println("-----------------------------------")
println((f"${(dataIndigena.where($"anio" === 2015).count)}%d INDIGENAS encuestados en el 2015 "))
println((f"${(dataIndigena.where($"anio" === 2016).count)}%d INDIGENAS encuestados en el 2016 "))
println((f"${(dataIndigena.where($"anio" === 2017).count)}%d INDIGENAS encuestados en el 2017 "))
println((f"${(dataIndigena.where($"anio" === 2018).count)}%d INDIGENAS encuestados en el 2018 "))
println((f"${(dataIndigena.where($"anio" === 2019).count)}%d INDIGENAS encuestados en el 2019 "))
println((f"${(dataIndigena.count)}%d TOTAL INDIGENAS encuestados"))


// COMMAND ----------

// DBTITLE 1,2.5 Promedio de la Rama Actividad en las etnias Mestizo - Indígena
println((f"${(dataMestizo.where($"rama_actividad" === "Agricultura, ganadería caza y silvicultura y pesca").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Agricultura, ganadería caza y silvicultura y pesca"))
println((f"${(dataMestizo.where($"rama_actividad" === "Explotación de minas y canteras").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Explotación de minas y canteras"))
println((f"${(dataMestizo.where($"rama_actividad" === "Industrias manufactureras").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Industrias manufactureras"))
println((f"${(dataMestizo.where($"rama_actividad" === "Suministros de electricidad, gas, aire acondicionado").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Suministros de electricidad, gas, aire acondicionado"))
println((f"${(dataMestizo.where($"rama_actividad" === "Distribución de agua, alcantarillado").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Distribución de agua, alcantarillado"))
println((f"${(dataMestizo.where($"rama_actividad" === "Construcción").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Construcción"))
println((f"${(dataMestizo.where($"rama_actividad" === "Comercio, reparación vehículos").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Comercio, reparación vehículos"))
println((f"${(dataMestizo.where($"rama_actividad" === "Transporte y almacenamiento").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Transporte y almacenamiento"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades de alojamiento y servicios de comida").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades de alojamiento y servicios de comida"))
println((f"${(dataMestizo.where($"rama_actividad" === "Información y comunicación").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Información y comunicación"))
println((f"${(dataMestizo.where($"rama_actividad" === "No especificado").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - No especificado"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades financieras y de seguros").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades financieras y de seguros"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades inmobiliarias").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades inmobiliarias"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades profesionales").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades profesionales"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades y servicios administrativos y de apoyo").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades y servicios administrativos y de apoyo"))
println((f"${(dataMestizo.where($"rama_actividad" === "Administración pública, defensa y seguridad social").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Administración pública, defensa y seguridad social"))
println((f"${(dataMestizo.where($"rama_actividad" === "Enseñanza").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Enseñanza"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades, servicios sociales y de salud").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades, servicios sociales y de salud"))
println((f"${(dataMestizo.where($"rama_actividad" === "Artes, entretenimiento y recreación").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Artes, entretenimiento y recreación"))
println((f"${(dataMestizo.where($"rama_actividad" === "Otras actividades de servicios").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Otras actividades de servicios"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades en hogares privados con servicio doméstico").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades en hogares privados con servicio doméstico"))
println((f"${(dataMestizo.where($"rama_actividad" === "Actividades de organizaciones extraterritoriales").count / dataMestizo.count.toDouble) *100}%.2f%% MESTIZO - Actividades de organizaciones extraterritoriales"))

println("-----------------------------------") 

println((f"${(dataIndigena.where($"rama_actividad" === "Agricultura, ganadería caza y silvicultura y pesca").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Agricultura, ganadería caza y silvicultura y pesca"))
println((f"${(dataIndigena.where($"rama_actividad" === "Explotación de minas y canteras").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Explotación de minas y canteras"))
println((f"${(dataIndigena.where($"rama_actividad" === "Industrias manufactureras").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Industrias manufactureras"))
println((f"${(dataIndigena.where($"rama_actividad" === "Suministros de electricidad, gas, aire acondicionado").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Suministros de electricidad, gas, aire acondicionado"))
println((f"${(dataIndigena.where($"rama_actividad" === "Distribución de agua, alcantarillado").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Distribución de agua, alcantarillado"))
println((f"${(dataIndigena.where($"rama_actividad" === "Construcción").count / dataIndigena.count.toDouble) *100}%.2f%% MESTIZO - Construcción"))
println((f"${(dataIndigena.where($"rama_actividad" === "Comercio, reparación vehículos").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Comercio, reparación vehículos"))
println((f"${(dataIndigena.where($"rama_actividad" === "Transporte y almacenamiento").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Transporte y almacenamiento"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades de alojamiento y servicios de comida").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades de alojamiento y servicios de comida"))
println((f"${(dataIndigena.where($"rama_actividad" === "Información y comunicación").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Información y comunicación"))
println((f"${(dataIndigena.where($"rama_actividad" === "No especificado").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - No especificado"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades financieras y de seguros").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades financieras y de seguros"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades inmobiliarias").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades inmobiliarias"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades profesionales").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades profesionales"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades y servicios administrativos y de apoyo").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades y servicios administrativos y de apoyo"))
println((f"${(dataIndigena.where($"rama_actividad" === "Administración pública, defensa y seguridad social").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Administración pública, defensa y seguridad social"))
println((f"${(dataIndigena.where($"rama_actividad" === "Enseñanza").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Enseñanza"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades, servicios sociales y de salud").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades, servicios sociales y de salud"))
println((f"${(dataIndigena.where($"rama_actividad" === "Artes, entretenimiento y recreación").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Artes, entretenimiento y recreación"))
println((f"${(dataIndigena.where($"rama_actividad" === "Otras actividades de servicios").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Otras actividades de servicios"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades en hogares privados con servicio doméstico").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades en hogares privados con servicio doméstico"))
println((f"${(dataIndigena.where($"rama_actividad" === "Actividades de organizaciones extraterritoriales").count / dataIndigena.count.toDouble) *100}%.2f%% INDIGENA - Actividades de organizaciones extraterritoriales"))

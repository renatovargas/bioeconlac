# Script de procesamiento COU
# Renato Vargas

# Librerías
library(readxl) # Importar datos de Excel
library(dplyr) # Manipulación de datos
library(tidyr) # Limpieza de datos
library(stringr) # Manipulación de textos
library(openxlsx) # Exportación a Excel
library(arrow)

# Limpiar el área de trabajo
rm(list = ls())

# Datos
archivo <- "datos/cou/COU_2022_PRECIOSCORRIENTES_111x181.xlsx"

# Facilitar el proceso con funciones a la medida

# Funcion para procesar cuadrantes
source("funciones/procesar_cuadrante.R")

test <- procesar_cuadrante(
  "datos/cou/COU_2022_PRECIOSCORRIENTES_111x181.xlsx",
  "ui",
  "02 Utilización",
  "5",
  "C14:DI194",
  integer(0),
  integer(0)
)

# Información general necesaria

config <- data.frame(
  cuadrante = c(
    "mp",
    "ot",
    "ui",
    "ut",
    "va"
  ),
  cuadro = c(
    "01 Oferta",
    "01 Oferta",
    "02 Utilización",
    "02 Utilización",
    "03 Valor Agregado"
  ),
  hoja = c("1", "2", "5", "6", "23"),
  rango = c(
    "C14:DI194",
    "C15:K195",
    "C14:DI194",
    "D16:L196",
    "D20:DJ29"
  ),
  excluir_columnas = I(list(
    integer(0), # mp
    c(1, 3, 6, 9), # ot
    integer(0), # ui
    c(1, 8, 9), # ut
    integer(0) # va
  )),
  excluir_filas = I(list(
    integer(0), # mp
    integer(0), # ot
    integer(0), # ui
    integer(0), # ut
    c(2, 3, 5, 6, 8, 9) # va
  ))
)

# Ahora usamos nuestra función para procesar todos los cuadrantes
# de manera iterativa

cuadrantes <- lapply(seq_len(nrow(config)), function(i) {
  procesar_cuadrante(
    archivo = archivo,
    cuadrante = config$cuadrante[i],
    cuadro = config$cuadro[i],
    hoja = config$hoja[i],
    rango = config$rango[i],
    excluir_filas = config$excluir_filas[[i]],
    excluir_columnas = config$excluir_columnas[[i]]
  )
})

# Inspeccionamos la lista resultante
cuadrantes

# Y los unimos en un solo objeto

cou_2022 <- bind_rows(cuadrantes)

#Le damos significado a las filas y columnas
clasificacionColumnas <- read_xlsx(
  "datos/equivalencias/CHL_Equivalencias.xlsx",
  sheet = "Columnas",
  col_names = TRUE,
)
clasificacionFilas <- read_xlsx(
  "datos/equivalencias/CHL_Equivalencias.xlsx",
  sheet = "Filas",
  col_names = TRUE,
)

# Hacemos una unión
cou_2022 <- left_join(cou_2022, clasificacionColumnas, by = "Columnas")
cou_2022 <- left_join(cou_2022, clasificacionFilas, by = "Filas")

# Y lo exportamos a Excel
# write.xlsx(
#   cou_2022,
#   "salidas/CHL_SCN_BD_2022.xlsx",
#   sheetName = "CHL_SCN_BD",
#   rowNames = FALSE,
#   colnames = FALSE,
#   overwrite = TRUE,
#   asTable = FALSE
# )

# Podemos usar el mismo procedimiento
# para crear una función para procesar
# todo el COU de un archivo

# Importamos nuestra función
source("funciones/procesar_cou.R")

test2 <- procesar_cou("datos/cou/COU_2022_PRECIOSCORRIENTES_111x181.xlsx")

# ...y aplicamos los siguientes pasos

# Como funciona, por qué no aplicamos los pasos a todos nuestros cous
# en una sola partida.

# Rutas de todos los archivos de Excel
archivos <- list.files(
  path = "datos/cou", # el directorio
  pattern = "\\.xlsx$", # solamente archivos .xlsx
  full.names = TRUE # retornar la ruta completa
)

# La podemos aplicar recursivamente a todos los archivos
cous <- lapply(archivos, procesar_cou)

scn_chl <- dplyr::bind_rows(cous)

#Le damos significado a las filas y columnas
clasificacionColumnas <- read_xlsx(
  "datos/equivalencias/CHL_Equivalencias.xlsx",
  sheet = "Columnas",
  col_names = TRUE,
)
clasificacionFilas <- read_xlsx(
  "datos/equivalencias/CHL_Equivalencias.xlsx",
  sheet = "Filas",
  col_names = TRUE,
)

# Hacemos una unión
scn_chl <- left_join(scn_chl, clasificacionColumnas, by = "Columnas")
scn_chl <- left_join(scn_chl, clasificacionFilas, by = "Filas")


# Un ejemplo del año 2022
scn_chl_2022 <- scn_chl |>
  filter(`Año` == 2022)
# |> 
#   mutate(across(everything(), ~ ifelse(is.na(.x), "-", .x)))

# Y lo exportamos a Excel
write.xlsx(
  scn_chl_2022,
  "salidas/CHL_SCN_BD_2022.xlsx",
  sheetName = "CHL_SCN_BD",
  rowNames = FALSE,
  colnames = FALSE,
  overwrite = TRUE,
  asTable = FALSE
)

# Y lo exportamos a Excel
write.xlsx(
  scn_chl,
  "salidas/CHL_SCN_BD.xlsx",
  sheetName = "CHL_SCN_BD",
  rowNames = FALSE,
  colnames = FALSE,
  overwrite = TRUE,
  asTable = FALSE
)

# Toda la base de datos en RDS
# Formato binario de R que ocupa muy poco espacio en disco
saveRDS(scn_chl, file = "salidas/chl_scn_bd.rds")

# Toda la base de datos en Parquet
# Formato de Apache Arrow de muy poco espacio en disco
# pero más fácil de compartir con otros sistemas.
write_parquet(scn_chl, "salidas/schl_scn_bd.parquet")

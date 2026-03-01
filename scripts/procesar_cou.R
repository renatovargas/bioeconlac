# Función de procesamiento de COU

source("funciones/procesar_cuadrante.R")

# Información general necesaria

procesar_cou <- function(archivo) {
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
      "C14:DJ194",
      "C15:K195",
      "C14:DJ194",
      "D16:L196",
      "D20:DK29"
    ),
    excluir_columnas = I(list(
      c(112), # mp
      c(1, 3, 6, 9), # ot
      c(112), # ui
      c(1, 8, 9), # ut
      c(112) # va
    )),
    excluir_filas = I(list(
      integer(0), # mp
      integer(0), # ot
      integer(0), # ui
      integer(0), # ut
      c(2, 3, 5, 6, 8, 9) # va
    ))
  )
  out <- lapply(seq_len(nrow(config)), function(i) {
    procesar_cuadrante(
      archivo = archivo,
      cuadrante = config$cuadrante[i],
      cuadro = config$cuadro[i],
      hoja = config$hoja[i],
      rango = config$rango[i],
      excluir_filas = config$excluir_filas[[i]],
      excluir_columnas = config$excluir_columnas[[i]]
    )
  }) |>
    dplyr::bind_rows()

  out
}

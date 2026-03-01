# Función de procesamiento de cuadrante
# Renato Vargas

procesar_cuadrante <- function(
  archivo,
  cuadrante,
  cuadro,
  hoja,
  rango,
  excluir_filas = integer(0),
  excluir_columnas = integer(0)
) {
  # Información general de la hoja
  info <- readxl::read_excel(
    archivo,
    range = sprintf("'%s'!B4:B6", hoja),
    col_names = FALSE,
    col_types = "text"
  )

  anio <- as.integer((stringr::str_extract(info[3, ], "\\d{4}")))
  unidad <- info[3, ]

  # Importar el cuerpo de datos
  datos1 <- readxl::read_excel(
    archivo,
    range = sprintf("'%s'!%s", hoja, rango),
    col_names = FALSE,
    col_types = "numeric"
  )

  # Crear códigos de columna y fila estables
  n_filas <- nrow(datos1)
  n_columnas <- ncol(datos1)
  cod_filas <- sprintf("%s_f%03d", cuadrante, seq_len(n_filas))
  cod_columnas <- sprintf("%s_c%03d", cuadrante, seq_len(n_columnas))

  # Reemplazar las celdas vacías con ceros
  datos2 <- datos1 |>
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        ~ tidyr::replace_na(.x, 0)
      )
    ) |>
    setNames(cod_columnas) |>
    dplyr::mutate(Filas = cod_filas, .before = 1)

  # Deshacernos de filas y columnas redundantes o vacías
  datos3 <- datos2 |>
    dplyr::filter(
      !Filas %in% cod_filas[excluir_filas]
    ) |>
    dplyr::select(
      -cod_columnas[excluir_columnas]
    )

  # Alargar Y agregar columnas informativas
  out <- datos3 |>
    tidyr::pivot_longer(
      cols = -Filas,
      names_to = "Columnas",
      values_to = "Valor"
    ) |>
    dplyr::transmute(
      Filas,
      Columnas,
      `Año` = anio,
      Cuadro = cuadro,
      Cuadrante = cuadrante,
      Unidades = stringr::str_extract(unidad, "(?<=\\().*?(?= de \\d{4}\\))"),
      Precios = "corrientes",
      Valor
    )
  out
}

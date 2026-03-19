## Datos

# Primero llamamos las librerías necesarias.

library(tidyverse)
library(gt)
library(pivottabler)
rm(list = ls())

biolatam <- readRDS(
  "/home/renato/Dropbox/work/2025-06-CEPAL_Latam/data/old_databases/SCN_BD_stitched.rds"
)

# Y probamos hacer un resumen para una actividad económica en particular.

test <- biolatam |>
  filter(
    ISO3 == "PAN" &
      Año == 2019 &
      Valor > 0 &
      Cuadro %in% c("Oferta", "Utilización") &
      `Área transaccional` == "Producción / Consumo intermedio" &
      `Descripción Actividades` ==
        "Fabricación de productos farmacéuticos, sustancias químicas medicinales y de productos botánicos"
  ) |>
  group_by(Cuadro, `Nomenclatura Local de Productos`) |>
  summarize(Valor = sum(Valor, na.rm = T)) |>
  arrange(Cuadro, desc(Valor)) |>
  ungroup()

# test$Porcentaje <- format((test$Valor / sum(test$Valor))* 100, )
test$Porcentaje <- test$Valor / sum(test$Valor) * 100

test |>
  gt()

test2 <- biolatam |>
  filter(
    Año == 2019 &
      Valor > 0 &
      Cuadro == "Utilización" &
      Área.transaccional == "Producción / Consumo intermedio" &
      # Actividades de interés
      Código.Clasificación.Actividades %in%
        c("011001", "025002", "034001", "036002", "031001", "021001")
    # & Descripción.Actividades == "Construcción"
  ) |>
  select(Descripción.Actividades, Bioeconomía.Productos, Valor) |>
  group_by(Descripción.Actividades, Bioeconomía.Productos) |>
  summarize(Valor = sum(Valor, na.rm = T)) |>
  mutate(
    Pct = Valor * 100 / sum(Valor),
    Pct_label = paste0(sprintf("%.2f", Pct), "%")
  ) |>
  rename(
    Actividades = Descripción.Actividades,
    Insumos = Bioeconomía.Productos,
    Valor = Valor
  ) |>
  ungroup()

test3 <- test2[test2$Insumos == "Bioeconomía", ] |>
  arrange(desc(Pct))

test2$Actividades <- factor(test2$Actividades, levels = test3$Actividades)
test2$Insumos <- fct_relevel(
  test2$Insumos,
  "No bioeconomía",
  "Bioeconomía extendida",
  "Bioeconomía"
)

# Grafico}
ggplot(
  test2,
  aes(
    x = Actividades,
    y = Valor,
    fill = Insumos
  )
) +
  geom_bar(
    stat = "identity",
    position = "fill"
  ) +
  geom_text(
    aes(label = paste0(sprintf("%1.1f", Pct), "%")),
    position = position_fill(vjust = 0.5),
    colour = "black",
    size = 3
  ) +
  ylab("Porcentaje") +
  xlab("Actividades económicas") +
  labs(fill = "Insumos") +
  # theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1)) +
  scale_x_discrete(
    labels = c(
      "Carne",
      "Servicio de \nalimentos",
      "Muebles",
      "Textiles",
      "Construcción",
      "Químicos"
    )
  )

# Mapas
adm1 <- sf::read_sf("datos/gis/ecu_adm_1.shp") |>
  dplyr::select(shapeISO, shapeName, geometry) |>
  dplyr::filter(shapeISO != "EC-W") |>
  dplyr::mutate(map_label = paste0(shapeISO, " ", shapeName))

tmap::tm_shape(adm1) +
  tmap::tm_fill(
    "map_label",
    legend.show = TRUE,
    palette = "Set1",
    title = "Provincia"
  ) +
  tmap::tm_text("shapeISO", size = 0.65, auto.placement = T, col = "black") +
  tmap::tm_layout(frame = T, legend.outside = T)


# Aquí también hacemos un cuadro dinámico.

for_pivot <- as.data.frame(biolatam) |>
  filter(Año == 2019)

library(pivottabler)

pt <- PivotTable$new()
pt$addData(for_pivot)
pt$addColumnDataGroups("Área.transaccional.columnas.No.")
pt$addColumnDataGroups("Área.transaccional", addTotal = FALSE)
# pt$addColumnDataGroups("CIIU.Secciones", addTotal=FALSE)
# pt$addColumnDataGroups("CIIU.Corto", addTotal=FALSE)
pt$addRowDataGroups("No..Cuadro", addTotal = FALSE)
pt$addRowDataGroups("Cuadro")
pt$addRowDataGroups(
  "Bioeconomía.Productos",
  totalCaption = "Bioeconomía",
  addTotal = FALSE
)
pt$sortColumnDataGroups(levelNumber = 2, orderBy = "value")
pt$sortRowDataGroups(levelNumber = 2, orderBy = "value")
pt$defineCalculation(
  calculationName = "Gran Total",
  summariseExpression = "sum(Valor, na.rm=TRUE)",
  format = list(
    digits = 1,
    nsmall = 1,
    big.mark = ",",
    decimal.mark = ".",
    scientific = FALSE
  )
)
pt$renderPivot()

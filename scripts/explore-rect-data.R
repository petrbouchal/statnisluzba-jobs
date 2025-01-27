
orgdata_rect <- rectangularise_orgdata(orgdata_raw)

write_csv(orgdata_rect, "data-export/struktura-hierarchie.csv")

library(collapsibleTree)
collapsibleTree(
  orgdata_rect |> filter(str_detect(level1_nazev, "Ministerstvo školství")),
  hierarchy = c("level1_nazev", "level2_nazev", "level3_nazev", "level4_nazev", "level5_nazev"),
  width = 1200,
  heigth = 1200,
  root = NULL,
  nodeSize = "mista_sluz",
  zoomable = TRUE
)

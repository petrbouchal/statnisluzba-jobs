source("_targets_packages.R")

orgdata_types <- orgdata_nodes |>
  group_by(urad_skupina) |>
  mutate(pocet_uradu = n_distinct(urad_nazev),
         mista_celkem = mista_prac + mista_sluz)

orgdata_types |> count(urad_skupina, wt = n_distinct(urad_nazev), sort = T) |> View()
orgdata_types |>
  filter(urad_skupina == "Ostatní") |>
  distinct(urad_zkratka, urad_nazev) |> View()

library(reactable)
dt_for_table <- orgdata_types |>
  group_by(urad_nazev, urad_zkratka, urad_skupina) |>
  summarise(mista_celkem = sum(mista_celkem, na.rm = T), .groups = "drop") |>
  distinct(urad_nazev, urad_zkratka, urad_skupina, mista_celkem) |>
  mutate(mista_celkem = na_if(mista_celkem, 0))

reactable(
  dt_for_table,
  groupBy = c("urad_skupina"),
  columns = list(
    urad_skupina = colDef(aggregate = "sum", format = colFormat(separators = TRUE)),
    mista_celkem = colDef(aggregate = "sum", format = colFormat(separators = TRUE), filterable = FALSE),
    urad_nazev = colDef(cell = function(value, index) {
      url <- sprintf("https://cs.wikipedia.org/wiki/%s", dt_for_table[index, "urad_nazev"], value)
      htmltools::tags$a(href = url, as.character(value))
    })
  ),
  filterable = TRUE,
  searchable = TRUE,
  bordered = FALSE,
  defaultPageSize = 20
)


source("_targets_packages.R")

targets::tar_load(syst_all)
targets::tar_load(syst_annual)
targets::tar_load(syst_pocty_long)
conflict_prefer("filter", "dplyr")

syst_annual |>
  filter(date == "2018-01-01")

x <- read_excel("data-input/syst/syst_2019.xlsx", skip = 4)
x
x <- read_excel("data-input/syst/syst_2018.xlsx", skip = 3)
x
x <- read_excel("data-input/syst/syst_2020.xlsx", skip = 4)
x
x[,-1:-2]
x <- x[,-2:-3]
x
x <- x[-1,]
x |> janitor::remove_empty("rows")

syst_pocty_long |>
  count(date, vztah, level, wt = pocet) |>
  pivot_wider(names_from = c(vztah, level), values_from = n)

syst_all |>
  group_by(date, vztah) |>
  summarise(across(c(pocet_predst, pocet_ostat), sum, na.rm = T)) |>
  pivot_wider(names_from = c(vztah), values_from =c(pocet_predst, pocet_ostat))

syst_annual |>
  group_by(date, vztah) |>
  summarise(across(c(pocet_predst, pocet_ostat), sum, na.rm = T)) |>
  pivot_wider(names_from = c(vztah), values_from =c(pocet_predst, pocet_ostat))

syst_sluz |>
  group_by(date) |>
  summarise(across(c(pocet_predst, pocet_ostat), sum, na.rm = T))

syst_annual |>
  mutate(clkm = kapitola_kod == "Celkem") |>
  select(date, pocet_predst, pocet_ostat, vztah, pozad_obcanstvi, pozad_zakazkonkurence, clkm) |>
  group_by(date, vztah, clkm) |>
  summarise(across(c(pocet_predst, pocet_ostat), sum, na.rm = TRUE), .groups = "drop") |>

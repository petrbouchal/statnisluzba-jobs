source("_targets_packages.R")
conflict_prefer("filter", "dplyr")
targets::tar_load(syst_all)
targets::tar_load(syst_pocty_long)

library(dplyr)
library(readr)
syst_pocty_long |>
  filter(kapitola_kod != "Celkem", rok == 2022,
         kapitola_vladni) |>
  count(kapitola_zkr, organizace_typ, level_nazev, wt = pocet) |>
  write_csv("sums.csv")

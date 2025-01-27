targets::tar_load(syst_pocty_long)
syst_pocty_long

library(ggplot2)
library(dplyr)
library(lubridate)

syst_pocty_long |>
  filter(kapitola_kod == "Celkem") |>
  count(rok)

syst_pocty_long |>
  filter(kapitola_kod == 364)

ggplot(syst_pocty_long |>
         filter(kapitola_kod != "Celkem", !is.na(pocet)) |>
         filter(month(date) == 1) |>
         count(kapitola_typ, kapitola_vladni, organizace_typ, rok, wt = pocet, name = "pocet"),
       aes(rok, pocet, grp = 1)) +
  geom_col(aes(fill = paste(organizace_typ, "-", kapitola_typ)))

month(syst_pocty_long$date)

unique(syst_pocty_long$kapitola_nazev)

syst_pocty_long |>
  filter(organizace_typ == "Ústřední orgán", is.na(kapitola_typ),
         kapitola_kod != "Celkem", !is.na(pocet)) |>
  relocate(pocet)

syst_pocty_long |>
  filter(kapitola_kod != "Celkem") |>
  filter(month(date) == 1) |>
  count(kapitola_typ, kapitola_vladni, organizace_typ, rok, wt = pocet, name = "pocet") |>
  arrange((pocet))

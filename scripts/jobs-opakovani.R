library(xml2)
library(purrr)
library(tidyr)
library(tibble)
library(dplyr)
library(furrr)
library(stringr)
library(lubridate)
library(ggplot2)
library(forcats)

targets::tar_load(jobs)
targets::tar_load(cis_predstaveni)

jobs |>
  filter(!str_detect(nazev, "^[0-9]|MSMT[0-9]|[Ss]\\s?[0-9]"), str_detect(urad_nazev, "Ministerstvo")) |>
  select(nazev, urad_nazev) |>
  count(urad_nazev)

jobs |>
  group_by(id_nodate) |>
  mutate(pocet_opakovani = n_distinct(zverejneno)) |>
  filter(pocet_opakovani > 1) |>
  distinct(id_nodate, zverejneno) |>
  arrange(id_nodate)

jobs |>
  distinct(id_nodate)

jobs_opak <- jobs |>
  group_by(id_nodate, platova_trida, urad_nazev, predstaveny) |>
  mutate(pocet_opakovani = n_distinct(zverejneno)) |>
  ungroup() |>
  distinct(id_nodate, platova_trida, urad_nazev, predstaveny, zverejneno, pocet_opakovani) |>
  mutate(opakovany = pocet_opakovani > 1,
         is_predstaveny = !is.na(predstaveny))

jobs_opak2 <- jobs |>
  group_by(nazev, platova_trida, urad_nazev, predstaveny) |>
  summarise(pocet_opakovani = n_distinct(zverejneno)) |>
  ungroup()

jobs_opak |>
  count(pocet_opakovani)

jobs_opak2 |>
  count(pocet_opakovani)

jobs_opak |>
  count(platova_trida, wt = mean(pocet_opakovani))

jobs_opak |>
  left_join(cis_predstaveni,
            by = join_by(predstaveny, between(zverejneno, od, do))) |>
  count(pocet_opakovani, predstaveny_nazev) |>
  spread(pocet_opakovani, n)

jobs_opak_for_plot <- jobs_opak |>
  arrange(zverejneno) |>
  group_by(id_nodate) |>
  mutate(poradi = row_number(),
         min_zverejneno = min(zverejneno),
         prodleva = zverejneno - min_zverejneno) |>
  ungroup() |>
  arrange(id_nodate)

jobs_opak_for_plot |>
  filter(prodleva > 0) |>
  ggplot(aes(x = as.numeric(prodleva), fill = as.factor(poradi))) +
  geom_histogram() +
  scale_fill_viridis_d() +
  facet_wrap(~ !is.na(predstaveny))

jobs_opak |>
  count(pocet_opakovani)

jobs_opak_for_plot |>
  filter(prodleva > 0, poradi == 2) |>
  ggplot(aes(pocet_opakovani, platova_trida, colour = !is.na(predstaveny))) +
  geom_point(position = "jitter", alpha = .3)

jobs_opak_for_plot |>
  filter(poradi == 1) |>
  count(is_predstaveny, platova_trida, wt = mean(opakovany)) |>
  ggplot(aes(platova_trida, n, colour = is_predstaveny)) +
  geom_line()

jobs_opak_for_plot |>
  filter(poradi == 1) |>
  count(dtm = floor_date(zverejneno, "month"), opakovany) |>
  ggplot(aes(dtm, n, fill = opakovany)) +
  geom_col()





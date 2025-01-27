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
targets::tar_load(syst_pocty_long)
targets::tar_load(cis_sluz_urady)
targets::tar_load(cis_predstaveni)

jobs |>
  select(-file) |>
  distinct() |>
  nrow()

table(cis_sluz_urady$urad_nazev %in% syst_pocty_long$organizace_nazev)
table(unique(syst_pocty_long$organizace_nazev) %in% cis_sluz_urady$urad_nazev)

syst_pocty_slct <- syst_pocty_long |>
  mutate(organizace_nazev_long =
           str_replace(organizace_nazev, "KVV", "Krajské vojenské velitelství") |>
           str_replace("OSSZ", "Okresní správa sociálního zabezpečení") |>
           str_replace("OIP", "Oblastní inspektorát práce") |>
           str_replace("MSSZ", "Městská správa sociálního zabezpečení") |>
           str_replace("kr\\.$", "kraj") |>
           str_replace("Zeměm. a kat.", "Zeměměřický a katastrální") |>
           str_replace("Kr. hyg.", "Krajská hygienická") |>
           str_replace("OBÚ", "Obvodní báňský úřad")) |>
  filter(rok == 2022)

syst_pocty_slct |>
  filter(!organizace_nazev_long %in% cis_sluz_urady$urad_nazev) |>
  distinct(organizace_nazev_long) |> pull()

syst_pocty_slct_sum <- syst_pocty_slct |>
  filter(vztah == "sluz", trida != "M") |>
  replace_na(list(pocet = 0)) |>
  mutate(trida = as.numeric(trida)) |>
  group_by(organizace_nazev, ustredni_organ, level) |>
  summarise(trida_mean = weighted.mean(x = trida,
                                       w = pocet, na.rm = TRUE), .groups = "drop")

jobs |>
  count(platova_trida, sort = TRUE)

jobs |>
  count(urad_obec_nazev, sort = TRUE)

# průměrná platová třída v čase

jobs |>
  count(yrmon, wt = mean(platova_trida), sort = T) |>
  drop_na() |>
  ggplot(aes(yrmon, n, group = 1)) +
  geom_line()

jobs |>
  filter(yrmon == "2023-01") |>
  select(id_nodate, urad_nazev, nazev, popis)

# Složení nabízených pozic podle platových tříd
jobs |>
  mutate(un = fct_lump_n(urad_nazev, 18) |> fct_infreq()) |>
  ggplot(aes(x = floor_date(zverejneno, "1 month"))) +
  geom_bar(aes(fill = as.factor(platova_trida) |> fct_rev())) +
  scale_fill_viridis_d() +
  facet_wrap(~ un) +
  scale_x_date(date_breaks = "1 month", date_labels = "%m")

jobs |>
  count(urad_nazev, wt = mean(platova_trida), sort = TRUE)

jobs |>
  filter(str_detect(urad_nazev, "Ministers|Úřad vlády")) |>
  drop_na(yrmon) |>
  ggplot(aes(zverejneno, as.integer(platova_trida), group = 1)) +
  facet_wrap(~urad_nazev) +
  geom_point(position = position_jitter(), alpha = .4) +
  scale_y_continuous(breaks = 7:16) +
  ptrr::theme_ptrr("both", multiplot = TRUE) +
  scale_x_date(breaks = scales::pretty_breaks(), labels = scales::label_date_short()) +
  geom_smooth()

jobs |>
  group_by(urad_nazev) |>
  slice_min(zverejneno, n = 1, with_ties = FALSE) |>
  arrange(desc(zverejneno)) |>
  select(urad_nazev, zverejneno)

jobs_to_syst <- jobs |>
  mutate(level = ifelse(!is.na(predstaveny), "predst", "ostat")) |>
  group_by(organizace_nazev = urad_nazev, level) |>
  summarise(trida_mean_advert = mean(x = platova_trida, na.rm = TRUE), .groups = "drop") |>
  left_join(syst_pocty_slct_sum)

# Nabíené vs. stávající pozice: platové třídy běžných a představených
jobs_to_syst |>
  filter(ustredni_organ) |>
  ggplot(aes(y = organizace_nazev, yend = organizace_nazev)) +
  geom_segment(data = \(x) x |> filter(level == "predst"),
               aes(colour = level, x = trida_mean, xend = trida_mean_advert),
               arrow = arrow(length = unit(0.2, "cm")),
               position = position_nudge(y = 0)) +
  geom_segment(data = \(x) x |> filter(level == "ostat"),
               mapping = aes(colour = level, x = trida_mean, xend = trida_mean_advert),
               arrow = arrow(length = unit(0.2, "cm")),
               position = position_nudge(y = 0)) +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(size = 6))

targets::tar_load(jobs_uniq)

jobs_uniq |>
  count(urad_nazev, wt = mean(platova_trida), sort = TRUE)

jobs_uniq

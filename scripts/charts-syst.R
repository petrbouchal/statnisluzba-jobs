source("_targets_packages.R")

targets::tar_load(syst_all)
targets::tar_load(syst_annual)
targets::tar_load(syst_pocty_long)
conflict_prefer("filter", "dplyr")

syst_all |>
  filter(kapitola_kod != "Celkem", date < "2022-04-01") |>
  select(date, pocet_predst, pocet_ostat, vztah, pozad_obcanstvi, pozad_zakazkonkurence) |>
  group_by(date, vztah) |>
  summarise(across(c(pocet_predst, pocet_ostat), sum, na.rm = TRUE), .groups = "drop") |>
  gather("var", "pocet", -vztah, -date) |>
  filter(var %in% c("pocet_predst", "pocet_ostat")) |>
  ggplot(aes(x = date)) +
  geom_col(aes(y = pocet/1e3, fill = var)) +
  facet_wrap(~vztah)

syst_pocty_long |>
  filter(kapitola_kod != "Celkem",
         src == "annual",
         ustredni_organ,
         kapitola_vladni) |>
  count(date, kapitola_zkr, level_nazev, wt = pocet) |>
  ggplot(aes(x = date)) +
  scale_y_number_cz(n.breaks = 6) +
  geom_col(aes(y = n, fill = level_nazev)) +
  # facet_wrap(~kapitola_zkr) +
  theme_ptrr("y", multiplot = TRUE, legend.key.size = unit(10, "pt"))

syst_all |>
  filter(rok == 2021,
         vztah == "sluz",
         # kap == "Celkem",
         ustredni_organ, kapitola_vladni) |>
  count(wt = pocet_ostat)

syst_pocty_long |>
  filter(date == "2022-04-01",
         vztah == "sluz",
         # kap == "Celkem",
         ustredni_organ,
         kapitola_vladni) |>
  group_by(kapitola_nazev) |>
  mutate(podil = pocet/sum(pocet, na.rm = TRUE)) |>
  ggplot(aes(trida, podil, fill = level)) +
    geom_col() +
    facet_wrap(~ kapitola_nazev, scales = "free_x")

## podíl představených

syst_pocty_long |>
  filter(kapitola_vladni) |>
  filter(ustredni_organ) |>
  count(date, kapitola_zkr, level, wt = pocet) |>
  group_by(date, kapitola_zkr) |>
  mutate(podil = n/sum(n)) |>
  filter(level == "predst") |>
  arrange(-podil) |>
  ggplot(aes(date, podil)) +
  geom_line() +
  geom_point() +
  ptrr::scale_y_percent_cz() +
  facet_wrap(~kapitola_zkr) +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  ptrr::theme_ptrr(multiplot = TRUE) +
  labs(title = "Podíl představených na celkovém počtu míst",
       subtitle = "Podle systemizace.\nJen ústřední orgány ve vládních kapitolách",
       caption = "Plánovaný stav podle systemizace")

syst_pocty_long |>
  filter(kapitola_vladni, rok == 2022, date < "2022-05-01") |>
  filter(ustredni_organ) |>
  count(rok, kapitola_zkr, level, date, wt = pocet) |>
  group_by(kapitola_zkr) |>
  mutate(podil = n/sum(n)) |>
  filter(level == "predst") |>
  ungroup() |>
  mutate(kapitola_zkr = as.factor(kapitola_zkr) |>
           fct_reorder(podil)) |>
  arrange(-podil) |>
  ggplot(aes(podil, kapitola_zkr)) +
  # geom_col(width = 0.05) +
  geom_point(size = 3, aes(colour = as.factor(date))) +
  scale_color_manual(values = c("grey", "black")) +
  ptrr::scale_x_percent_cz() +
  ptrr::theme_ptrr("x", multiplot = FALSE) +
  labs(title = "Podíl představených na celkovém počtu míst",
       subtitle = "Podle systemizace.\nJen ústřední orgány ve vládních kapitolách",
       caption = "Plánovaný stav podle systemizace")

# průměrná platová třída

syst_pocty_long |>
  filter(ustredni_organ, kapitola_vladni) |>
  mutate(trida = as.integer(trida)) |>
  group_by(rok, kapitola_zkr, level_nazev) |>
  drop_na(pocet) |>
  summarise(trida_mean = weighted.mean(trida, w = pocet, na.rm = TRUE)) |>
  ggplot(aes(rok, trida_mean, colour = level_nazev)) +
  geom_line() +
  geom_point() +
  facet_wrap(~kapitola_zkr) +
  scale_color_manual(name = "Úroveň řízení", values = c("grey50", "darkblue")) +
  ptrr::theme_ptrr(multiplot = TRUE, legend.position = "top") +
  labs(title = "Průměrná platová třída",
       subtitle = "Podle systemizace.\nJen ústřední orgány ve vládních kapitolách",
       caption = "Plánovaný stav podle systemizace")

syst_pocty_long |>
  filter(kapitola_kod != "Celkem") |>
  # filter(ustredni_organ, kapitola_vladni) |>
  filter(ustredni_organ) |>
  # filter(str_detect(organizace_nazev, "[Hh]yg")) |>
  mutate(trida = as.integer(trida)) |>
  group_by(rok, organizace_nazev, level_nazev) |>
  drop_na(pocet) |>
  filter(date == "2022-04-01") |>
  summarise(trida_mean = weighted.mean(trida, w = pocet, na.rm = TRUE), .groups = "drop") |>
  mutate(organizace_nazev = as.factor(organizace_nazev) |> fct_reorder(trida_mean, min)) |>
  ggplot(aes(trida_mean, organizace_nazev, colour = level_nazev)) +
  geom_point(size = 3) +
  scale_color_manual(name = "Úroveň řízení", values = c("grey50", "darkblue")) +
  scale_x_continuous(limits = c(8, 15)) +
  ptrr::theme_ptrr("both", multiplot = FALSE, legend.position = "top") +
  labs(title = "Průměrná platová třída (2022)",
       subtitle = "Podle systemizace 2022.\nJen ústřední orgány ve vládních kapitolách",
       caption = "Plánovaný stav podle systemizace 2022")

syst_all |>
  filter(ustredni_organ, kapitola_vladni, date < "2022-05-01") |>
  group_by(rok, kapitola_zkr) |>
  summarise(plat_prumer = sum(platy_celkem, na.rm = TRUE)/sum(pocet_celkem,
                                                              na.rm = T)/12,
            .groups = "drop") |>
  mutate(kapitola_zkr = as.factor(kapitola_zkr) |>
           fct_reorder(plat_prumer, max, .desc = TRUE)) |>
  ggplot(aes(rok, plat_prumer)) +
  geom_line() +
  geom_point() +
  facet_wrap(~kapitola_zkr) +
  ptrr::theme_ptrr(multiplot = TRUE) +
  labs(title = "Průměrný plat na ministerstvech",
       subtitle = "Kč, nominálně, podle systemizace.\nJen ústřední orgány ve vládních kapitolách",
       caption = "Plánované počty podle systemizace")

syst_all |>
  filter(ustredni_organ, kapitola_vladni,
         date == "2022-04-01") |>
  group_by(rok, kapitola_zkr) |>
  summarise(plat_prumer = sum(platy_celkem, na.rm = TRUE)/sum(pocet_celkem,
                                                              na.rm = T)/12,
            .groups = "drop") |>
  mutate(kapitola_zkr = as.factor(kapitola_zkr) |>
           fct_reorder(plat_prumer, max, .desc = FALSE)) |>
  ggplot(aes(plat_prumer, kapitola_zkr)) +
  geom_line() +
  geom_point() +
  ptrr::theme_ptrr("x", multiplot = TRUE) +
  labs(title = "Průměrný plat na ministerstvech",
       subtitle = "Kč, nominálně, podle systemizace.\nJen ústřední orgány ve vládních kapitolách",
       caption = "Plánované počty podle systemizace")



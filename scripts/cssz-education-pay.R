
# https://data.cssz.cz/web/otevrena-data/-/zamestnanci-cssz-dle-vzdelani
csszv <- read_csv("https://data.cssz.cz/dump/zamestnanci-cssz-dle-vzdelani.csv")

csszv |>
  ggplot(aes(rok, pocet_zamestnancu_cssz, fill = dosazene_vzdelani)) +
  geom_col(position = "fill")

syst_platy |>
  filter(rok == "2021",
         organizace_nazev == "ČSSZ - vše") |>
  ungroup() |>
  summarise(plat_prumer = weighted.mean(plat_prumer, pocet_celkem))

csszv |>
  filter(rok == 2021) |>
  mutate(podil = pocet_zamestnancu_cssz/sum(pocet_zamestnancu_cssz))

# viz https://www.czso.cz/csu/czso/struktura-mezd-zamestnancu-2020 - vzdělání mezi zaměstnanci (https://www.czso.cz/documents/10180/142398903/11002621a04.pdf/39919e04-78ac-4cf6-b742-5cabe12b1d92?version=1.8)

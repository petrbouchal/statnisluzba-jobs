source("_targets_packages.R")

conflict_prefer("filter", "dplyr")
conflict_prefer("lag", "dplyr")

targets::tar_load(jobs_uniq)
targets::tar_load(tarify)
targets::tar_load(cis_sluz_urady)
targets::tar_load(priplatky_vedeni)
targets::tar_load(urady_roztridene)

urady_roztridene |>
  count(urad_kategorie_general, urad_kategorie_osobko, urad_kategorie_priplatek)

tarif_max <- tarify |>
  filter(stupen == max(stupen)) |>
  select(rok, trida, tarif_max = plat) |>
  mutate(trida = as.numeric(trida))


# https://www.zakonyprolidi.cz/cs/2014-304#prilohy
obory_klicove = c(finance = "101", it = "128", pravo = "122", audit = "103",
                  zdrav = "121", zakazky = "137", perso = "163")

jobs_uniq |>
  # filter(file == max(file)) |>
  left_join(urady_roztridene) |>
  group_by(urad_kategorie_priplatek) |>
  mutate(rok = year(prace_od),
         klicove_priznak = if_else(is.na(klicove_priznak), FALSE, TRUE)) |>
  mutate(klicove_lze = map2_lgl(sluzba_obor, platova_trida,
                                \(x, y) any(obory_klicove %in% x) & y >= 12)) |>
  filter(klicove_lze, urad_kategorie_osobko == "ministerstva") |>
  group_by(urad_kategorie_osobko, klicove_lze, urad_nazev) |>
  count(wt = mean(klicove_priznak))

jobs_uniq |>
  # filter(file == max(file)) |>
  left_join(urady_roztridene) |>
  group_by(urad_kategorie_priplatek) |>
  mutate(rok = year(prace_od),
         klicove_priznak = if_else(is.na(klicove_priznak), FALSE, TRUE)) |>
  mutate(klicove_lze = map2_lgl(sluzba_obor, platova_trida,
                                \(x, y) any(obory_klicove %in% x) & y >= 12)) |>
  filter(klicove_lze, urad_kategorie_osobko == "ministerstva") |>
  group_by(urad_kategorie_osobko, klicove_lze, urad_nazev)

jobs_uniq |>
  unnest_longer(sluzba_obor) |>
  mutate(klicove_lze = (sluzba_obor %in% obory_klicove) & platova_trida >= 12,
         klicove_priznak = if_else(is.na(klicove_priznak), FALSE, TRUE)) |>
  filter(klicove_lze) |>
  count(sluzba_obor, wt = mean(klicove_priznak))


jobs_uniq |>
  filter(id_nodate == "11000016_30028488") |>
  unnest(sluzba_obor) |>
  select(sluzba_obor)


pay_simulations <- jobs_uniq |>
  # filter(file == max(file)) |>
  select(trida = platova_trida, predstaveny, urad_id, rok, id_nodate, nazev,
         popis, urad_nazev, klicove_priznak, klicove_lze) |>
  left_join(priplatky_vedeni) |>
  left_join(urady_roztridene) |>
  left_join(tarif_max) |>
  full_join(tarify |> mutate(trida = as.numeric(trida)), multiple = "all") |>
  rename(tarif = plat) |>
  mutate(pay_nokey_noexpert_min = tarif,
         pay_nokey_noexpert_typicallower = if_else(is.na(predstaveny), tarif / (1 - 0.14), tarif / (1 - 0.17)), # dobré hodnocení, střední hodnota typu org
         pay_nokey_noexpert_typicalmid = if_else(is.na(predstaveny), tarif / (1 - 0.20), tarif / (1 - 0.26)), # mezi vynikajícím a velmi dobrým, střední hodnota typu org
         pay_nokey_noexpert_typicalmax = if_else(is.na(predstaveny), tarif / (1 - 0.24), tarif / (1 - 0.30)), # mezi vynikajícím a velmi dobrým, střední hodnota typu org
         pay_nokey_noexpert_max = tarif + tarif_max * 0.5,
         pay_nokey_expert_typicalmid = if_else(is.na(predstaveny), tarif / (1 - 0.52), tarif / (1 - 0.54)), # expart, střední hodnota org
         pay_nokey_expert_max = tarif + tarif_max, # vynikající, max
         pay_key_noexpert_maxmultminosobko = tarif * 2, # max osobka
         pay_key_noexpert_maxmultmaxosobko = tarif * 2 + tarif_max * 0.5, # max osobka
         pay_key_expert_max = tarif * 2 + tarif_max,
         vedeni_max = case_match(urad_kategorie_priplatek,
                                    "ustredni" ~ tarif_max * usu_max/100,
                                    "uzemni" ~ tarif_max * suu_max/100,
                                    "ostatni" ~ tarif_max * su_max/100,

                                ),
         vedeni_min = case_match(urad_kategorie_priplatek,
                                    "ustredni" ~ tarif_max * usu_min/100,
                                    "uzemni" ~ tarif_max * suu_min/100,
                                    "ostatni" ~ tarif_max * su_min/100,

                                ),
         ) |>
  replace_na(list(vedeni_max = 0, vedeni_min = 0)) |>
  mutate(across(starts_with("pay_"), \(x) x + vedeni_min, .names = "min_{.col}"),
         across(starts_with("pay_"), \(x) x + vedeni_max, .names = "max_{.col}"),
         is_predstaveny = !is.na(predstaveny))

pay_sim_data <- pay_simulations |>
  select(id_nodate, is_predstaveny, contains("_pay_"), stupen, praxe_do,
         klicove_priznak, klicove_lze) |>
  pivot_longer(contains("_pay_")) |>
  separate(name, into = c("ved", "pay", "key", "expert", "range")) |>
  select(-pay) |>
  mutate(key = key == "key",
         expert = expert == "expert",
         value = round(value)) |>
  filter(!(!is_predstaveny & ved == "max"),
         !(!klicove_lze & key))

pay_sim_data_wide <- pay_sim_data |>
  drop_na(id_nodate) |>
  filter(id_nodate %in% jobs$id_nodate) |>
  pivot_wider(names_from = range, values_from = value)

saveRDS(pay_sim_data, "data-export/paysims.rds")

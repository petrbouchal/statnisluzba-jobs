parse_job <- function(x) {

  obory = x[names(x) == "SluzbaOborId"] |> map_chr(`[[`, 1) |> unname()

  tibble(
    id = x[["id"]][[1]],
    urad_id = x[["poptavajici"]][["UradSluzebniId"]][[1]],
    obory = list(obory),
    urad_obec_kod = x[["adresa_pracoviste"]][["kod_obce"]][[1]],
    urad_obec_nazev = x[["adresa_pracoviste"]][["nazev_obce"]][[1]],
    urad_okres_kod = x[["adresa_pracoviste"]][["kod_okresu"]][[1]],
    urad_okres_nazev = x[["adresa_pracoviste"]][["nazev_okresu"]][[1]],
    urad_stat_kod = x[["adresa_pracoviste"]][["Stat"]][[1]],
    nazev = x[["nazev"]][[1]],
    popis = x[["popis"]][[1]],
    platova_trida = x[["platova_trida"]][[1]],
    vzdelani_min = x[["min_vzdelani"]][[1]],
    uvazek_vyse = x[["uvazek_vyse"]][[1]],
    nabidka_platna_do = x$nabidka_platna_do[[1]],
    zverejneno = x[["zverejneno"]][[1]],
    prace_od = x[["prace_od"]][[1]],
    oznaceni_sluzebni = x[["OznaceniSluzebni"]][[1]],
    uroven_organizacni = x[["UrovenOrganizacni"]][[1]],
    utajeni_stupen = x[["UtajeniStupen"]][[1]],
    zpusobilost_zdravotni = x[["ZpusobilostZdravotni"]][[1]],
    predstaveny_oznaceni_sluzebni = x[["Predstaveny"]][["OznaceniSluzebni"]][[1]],
    predstaveny_predpoklad_specialni = x[["Predstaveny"]][["PredpokladSpecialni"]][[1]],
    odkaz = x[["InformaceZdrojExterniOdkaz"]][[1]]
  )
}

parse_job2 <- function(x) {

  smpl_df <- suppressMessages(
    tibble(data = list(x)) |>
      unnest_wider(data, simplify = TRUE, names_repair = janitor::make_clean_names) |>
      select(-zadost_podani_misto, -zadost_podani_adresa) |>
      mutate(across(everything(), ~map(.x, `[[`, 1)),
             across(everything(), ~map_chr(.x, `[[`, 1))) |>
      pivot_longer(starts_with("sluzba_obor"), values_to = "sluzba_obor", names_to = "xx") |>
      select(-xx) |>
      chop(sluzba_obor))

  if(any(grepl("zadost_podani_priloha", names(smpl_df)))) {
    smpl_df2 <- smpl_df |>
      pivot_longer(starts_with("zadost_podani_priloha"), values_to = "zadost_podani_priloha", names_to = "xx") |>
      select(-xx) |>
      chop(zadost_podani_priloha)
  } else {
    smpl_df2 <- smpl_df
  }

  return(smpl_df)

}

process_jobs <- function(jobs_raw, urady_roztridene, cis_predstaveni) {

  obory_klicove = c(finance = "101", it = "128", pravo = "122", audit = "103",
                    zdrav = "121", zakazky = "137", perso = "163")

  jbs <- jobs_raw |>
    ungroup() |>
    mutate(id_nodate = str_remove(id, "\\_20[0-9]{2}\\-[0-9]{2}-[0-9]{2}$"),
           id_date = str_extract(id, "\\_20[0-9]{2}\\-[0-9]{2}-[0-9]{2}$"),
           across(c(prace_od, zverejneno, nabidka_platna_do), as.Date)) |>
    rename(urad_id = poptavajici) |>
    mutate(platova_trida = as.numeric(platova_trida),
           yrmon = format(zverejneno, "%Y-%m")) |>
    left_join(urady_roztridene, by = join_by(urad_id)) |>
    left_join(cis_predstaveni,
              by = join_by(predstaveny, between(zverejneno, od, do))) |>
    replace_na(list(predstaveny_nazev = "není představený")) |>
    mutate(rok = year(prace_od),
           is_predstaveny = !is.na(predstaveny),
           klicove_priznak = if_else(is.na(klicove_priznak), FALSE, TRUE),
           klicove_lze = map2_lgl(sluzba_obor, platova_trida,
                                  \(x, y) any(obory_klicove %in% x) & y >= 12),
           latest_file = file == max(file),
           aktualni = nabidka_platna_do >= Sys.Date())

  return(jbs)
}

dedupe_jobs <- function(jobs) {
  jobs |>
    arrange(id_nodate, desc(id_date)) |>
    group_by(id_nodate) |>
    slice_head(n = 1) |>
    ungroup()
}



parse_job_list <- function(path) {
  # print(path)

  possibly(function(path) {
    xml_loaded <- read_xml(path)
    lst <- as_list(xml_loaded)
    x <- lst[["sluzebni_mista"]]

    jbs <- map_dfr(x, parse_job2)

    jbs$file <- path

    return(jbs)
  }, otherwise = tibble(file = path))(path)
}

simulate_salaries <- function(jobs_uniq, tarify, priplatky_vedeni) {

  tarify$trida <- as.numeric(tarify$trida)

  tarif_max <- tarify |>
    filter(stupen == max(stupen)) |>
    select(rok, trida, tarif_max = plat)

  pay_simulations_base <- jobs_uniq |>
    # filter(file == max(file)) |>
    select(trida = platova_trida, predstaveny, urad_id, rok, id_nodate, nazev,
           popis, urad_nazev, klicove_priznak, klicove_lze,
           starts_with("urad_kategorie")) |>
    left_join(priplatky_vedeni, by = "predstaveny") |>
    left_join(tarif_max, by = join_by(trida, rok)) |>
    full_join(tarify, by = join_by(trida, rok),
              multiple = "all", relationship = "many-to-many") |>
    rename(tarif = plat)

  pay_simulations <- pay_simulations_base |>
    mutate(pay_nokey_noexpert_min = tarif,
           pay_nokey_noexpert_typicallower = if_else(is.na(predstaveny), tarif + tarif_max * 0.14, tarif + tarif_max * 0.17), # dobré hodnocení, střední hodnota typu org
           pay_nokey_noexpert_typicalmid = if_else(is.na(predstaveny), tarif + tarif_max * 0.20, tarif + tarif_max * 0.26), # mezi vynikajícím a velmi dobrým, střední hodnota typu org
           pay_nokey_noexpert_typicalmax = if_else(is.na(predstaveny), tarif + tarif_max * 0.24, tarif + tarif_max * 0.30), # mezi vynikajícím a velmi dobrým, střední hodnota typu org
           pay_nokey_noexpert_max = tarif + tarif_max * 0.5,
           pay_nokey_expert_typicalmid = if_else(is.na(predstaveny), tarif + tarif_max * 0.52, tarif + tarif_max * 0.54), # expart, střední hodnota org
           pay_nokey_expert_max = tarif + tarif_max, # vynikající, max
           pay_key_noexpert_maxmultminosobko = tarif * 2, # max osobka
           pay_key_noexpert_maxmultmaxosobko = tarif * 2 + tarif_max * 0.5, # max osobka
           pay_key_expert_max = tarif * 2 + tarif_max,
           vedeni_max = case_match(urad_kategorie_priplatek,
                                   "ustredni" ~ tarif_max * usu_max / 100,
                                   "uzemni" ~ tarif_max * suu_max / 100,
                                   "celostatni" ~ tarif_max * su_max / 100,
                                   "ostatni" ~ tarif_max * osu_max / 100,

           ),
           vedeni_min = case_match(urad_kategorie_priplatek,
                                   "ustredni" ~ tarif_max * usu_min / 100,
                                   "uzemni" ~ tarif_max * suu_min / 100,
                                   "celostatni" ~ tarif_max * su_min / 100,
                                   "ostatni" ~ tarif_max * osu_min / 100,

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
    mutate(key = ifelse(key == "key", TRUE, FALSE),
           expert = ifelse(expert == "expert", TRUE, FALSE),
           value = round(value)) |>
    filter(!(!is_predstaveny & ved == "max"),
           !(!klicove_lze & key))


  pay_sim_data_wide <- pay_sim_data |>
    drop_na(id_nodate) |>
    pivot_wider(names_from = range, values_from = value)

  return(pay_sim_data_wide)
}

sub_jobs_for_app <- function(jobs_uniq) {
  dta <- jobs_uniq |>
    drop_na(id_nodate) |>
    select(id_nodate,
           urad_nazev,
           aktualni,
           nazev,
           popis,
           is_predstaveny,
           platova_trida,
           klicove_priznak,
           klicove_lze,
           predstaveny_nazev,
           predstaveny,
           id_nodate)

  return(dta)
}

export_jobs_for_app <- function(jobs_uniq_subbed) {
  outfile <- "data-export/app_jobs.json"
  write_json(jobs_uniq_subbed, outfile)
  return(outfile)
}

sub_sims_for_app <- function(jobs_salary_sims) {
  dta <- jobs_salary_sims |>
    drop_na(id_nodate) |>
    select(
      id_nodate,
      expert,
      key,
      praxe_do,
      ved,
      min,
      typicallower,
      typicalmid,
      typicalmax,
      max,
      maxmultminosobko,
      maxmultmaxosobko
    )

  return(dta)
}

export_sims_for_app <- function(jobs_salary_sims_subbed) {
  outfile_stem <- "data-export/app_sims/sims_"
  dta <- jobs_salary_sims_subbed

  fls <- lapply(unique(dta$id_nodate), \(x) {
    dta_sub <- dta[dta$id_nodate == x,]
    outfile <- paste0(outfile_stem, "_", x, ".json")
    write_json(dta_sub, outfile)
    return(outfile)
  })
  return(as.character(fls))
}


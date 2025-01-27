label_urady <- function(cis_sluz_urady, ustredni_nemini) {

  ustredni_nemini <- ustredni_nemini$urad_nemini

  cis_sluz_urady |>
    # pro osobko: ministerstva, ostatní ústřední, podřízené
    mutate(urad_kategorie_general = case_when(
      str_detect(tolower(urad_nazev), "minist|úřad vlády") ~ "ministerstva",
      urad_nazev %in% ustredni_nemini ~ "ustredni",
      str_detect(tolower(urad_nazev), "kraj|okres|oblast|měst|pražsk|újezd|inspektorát\\sv\\s|zemsk") ~ "uzemni",
      str_detect(tolower(urad_nazev), "^(národní)|(úřad)|(státní)|(česk)|rada|ústřední|finanční ředitelství") ~ "celostatni",
      .default = "ostatni"
    )) |>
    # filter(is.na(kategorie)) |>
    # pull(urad_nazev) |>
    # pro přípratek: Ústřední, celostátní, územní, ostatní
    mutate(urad_kategorie_osobko = case_match(urad_kategorie_general,
                                              c("celostatni", "uzemni", "ostatni") ~ "podrizene",
                                              .default = urad_kategorie_general),
           urad_kategorie_priplatek = case_match(urad_kategorie_general,
                                                 c("ministerstva", "ustredni") ~ "ustredni",
                                                 .default = urad_kategorie_general)) |>
    select(starts_with("urad"))
}

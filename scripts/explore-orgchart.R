library(xml2)
library(purrr)
library(tibble)
library(future)
library(furrr)
library(tictoc)
library(dplyr)
library(tidygraph)
library(ggraph)
library(visNetwork)
library(dplyr)

source("_targets_packages.R")
plan(multisession)

source("R/load-orgchart.R")
source("R/visnetwork.R")
conflict_prefer("filter", "dplyr")

# Načtení -----------------------------------------------------------------

targets::tar_load(starts_with("orgdata"))

orgdata_nodes_uv <- extract_orgdata_nodes_from_graph(orgdata_graph, urad_zkratka == "ÚV ČR")
write_csv(orgdata_nodes_uv, "data-export/struktura-nodes-uv.csv")
orgdata_edges_uv <- extract_orgdata_edges_from_graph(orgdata_graph, urad_zkratka == "ÚV ČR")
write_csv(orgdata_edges_uv, "data-export/struktura-edges-uv.csv")

orgdata_nodes_uv
orgdata_edges_uv

visNetwork(orgdata_nodes_uv, orgdata_edges_uv)

# Vizualizace statická ---------------------------------------------------

orgdata_edges[nrow(orgdata_edges),]

orgdata_graph |> activate(nodes) |> as_tibble() |> View()

## Výběr útvarů podle názvu ------------------------------------------------

orgdata_graph |>
  activate(nodes) |>
  filter(nazev != "nic") |>
  mutate(xx = node_distance_from(str_detect(nazev, "Odbor ekonomických analýz"))) |>
  filter(xx != Inf) |>
  ggraph(layout = "tree") +
  geom_node_point(aes(size = child_ftes)) +
  geom_edge_diagonal() +
  coord_flip() +
  geom_node_label(aes(label = nazev), hjust = "inward") +
  scale_y_reverse()

## Jedno ministerstvo ------------------------------------------------------

orgdata_graph |>
  activate(nodes) |>
  # mutate(xx = node_distance_to(nazev %in% c("odd.zákl.vzd."))) |>
  filter(nazev != "nic", urad_zkratka == "MPO ČR") |>
  # ggraph("star") +
  ggraph("kk") +
  geom_node_point(aes(size = child_ftes)) +
  geom_edge_diagonal0() +
  geom_node_label(aes(label = nazev), vjust = "outward") +
  scale_x_reverse()

## Všechny útvary nadřazené danému útvaru ----------------------------------

orgdata_graph |>
  activate(nodes) |>
  # mutate(xx = node_distance_to(nazev %in% c("odd.zákl.vzd."))) |>
  filter(nazev != "nic", urad_zkratka == "MPO ČR") |>
  mutate(xx = node_distance_to(str_detect(nazev, "Odbor ekonomických analýz"), mode = "out")) |>
  # activate(nodes) |> as_tibble() |>  View()
  filter(xx != Inf) |>
  ggraph(layout = "tree") +
  geom_node_point(aes(size = child_ftes)) +
  geom_edge_diagonal() +
  geom_node_label(aes(label = nazev), vjust = 0) +
  scale_x_reverse()

## Všechny útvary podřazené danému útvaru ----------------------------------

orgdata_graph |>
  activate(nodes) |>
  mutate(xx = node_distance_from(str_detect(nazev, "Odbor ekonomických analýz") &
                                   urad_zkratka == "MPO ČR")) |>
  filter(xx != Inf) |>
  ggraph(layout = "tree") +
  geom_node_point(aes(size = child_ftes)) +
  geom_edge_diagonal() +
  coord_flip() +
  geom_node_label(aes(label = nazev), hjust = "inward") +
  scale_y_reverse()

## Jeden úřad --------------------------------------------------------------

ggraph(orgdata_graph |>
         activate(nodes) |>
         filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády"),
                urad_zkratka == "MPO ČR",
                dpth <= 3),
       layout = "tree", circular = TRUE) +
  geom_edge_diagonal() +
  geom_node_point(aes(colour = dpth)) +
  geom_node_label(aes(label = paste0(nazev, "\n", mista_sluz, " + ", mista_prac),
                      fill = dpth), check_overlap = TRUE) +
  coord_flip() +
  # facet_nodes(~urad_zkratka, scales = "free") +
  theme_graph()

ggraph(orgdata_graph |>
         activate(nodes) |>
         filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády"),
                urad_zkratka == "MMR ČR"),
       layout = "tree", circular = TRUE) +
  geom_edge_diagonal() +
  geom_node_point(aes(colour = dpth)) +
  geom_node_label(aes(label = paste0(nazev, "\n", mista_sluz, " + ", mista_prac),
                      fill = dpth), check_overlap = TRUE) +
  coord_flip() +
  # facet_nodes(~urad_zkratka, scales = "free") +
  theme_graph()

ggraph(orgdata_graph |>
         activate(nodes) |>
         filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády")),
       layout = "tree", circular = TRUE) +
  geom_edge_diagonal() +
  # geom_node_label(aes(label = zkratka, fill = dpth), check_overlap = TRUE) +
  geom_node_point(aes(colour = dpth, size = mista_sluz + mista_prac)) +
  coord_flip() +
  # facet_nodes(~urad_zkratka, scales = "free") +
  theme_graph()

ggraph(orgdata_graph |>
         activate(nodes) |>
         filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády")),
       layout = "partition", circular = FALSE) +
  geom_node_tile(aes(fill = dpth, width = mista_prac + mista_sluz), colour = "white") +
  theme_graph()

orgdata_graph |>
  activate(nodes) |>
  filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády")) |>
  as_tibble() |>
  select(dpth, nazev, urad_nazev) |>
  pivot_wider(names_from = dpth, values_from = nazev, values_fn = list) |>
  set_names(c("urad_nazev", paste0("level_", 1:5))) |>
  unnest(cols = starts_with("level"))

# https://stackoverflow.com/questions/50840808/tidygraph-calculate-child-summaries-at-parent-level

ggraph(orgdata_graph |>
         activate(nodes) |>
         filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády")),
       layout = "partition", circular = FALSE) +
  geom_node_tile(aes(fill = urad_nazev, width = child_ftes), colour = "white") +
  theme_graph()

library(ggiraph)

ggg <- ggraph(orgdata_graph |>
                activate(nodes) |>
                filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády"),
                       urad_nazev == "Ministerstvo práce a sociálních věcí"),
              layout = "tree", circular = TRUE) +
  geom_edge_diagonal() +
  geom_node_point(aes(fill = urad_nazev, size = child_ftes), colour = "black") +
  # geom_node_label(aes(label = child_stat)) +
  geom_point_interactive(size = 4, colour = "grey", pch = 21,
                         mapping = aes(x = x, y = y, data_id = nazev,
                                       tooltip = paste0(nazev))
  ) +
  theme_graph()
ggg
girafe(ggobj = ggg)

orgdata_graph |>
  activate(nodes) |>
  filter(dpth == 1) |>
  select(urad_nazev, child_ftes) |>
  arrange(-child_ftes) |> as_tibble() |> View()

sum(orgdata_nodes$mista_prac, na.rm = TRUE) + sum(orgdata_nodes$mista_sluz, na.rm = TRUE)

orgdata_graph |>
  activate(nodes) |>
  filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády"),
         urad_zkratka == "MPSV ČR", str_detect(nazev, "začl|integ|vylouč")) |>
  select(nazev) |> as_tibble()

orgdata_graph |>
  activate(nodes) |>
  filter(str_detect(urad_nazev, "^Ministerstvo|Úřad vlády"),
         urad_zkratka == "MMR ČR", str_detect(nazev, "začl|integ|vylouč")) |>
  select(nazev) |> as_tibble()

orgdata_graph

# Vizualizace dynamická ---------------------------------------------------

library(visNetwork)

orgdata_jednomini_edges <- extract_orgdata_edges_from_graph(orgdata_graph, urad_zkratka == "MF ČR")
orgdata_jednomini_nodes <- extract_orgdata_nodes_from_graph(orgdata_graph, urad_zkratka == "MF ČR")
orgdata_druhemini_edges <- extract_orgdata_edges_from_graph(orgdata_graph, urad_zkratka == "MD ČR")
orgdata_druhemini_nodes <- extract_orgdata_nodes_from_graph(orgdata_graph, urad_zkratka == "MD ČR")

vn_jednomini <- visNetwork(
  orgdata_jednomini_nodes |>
    mutate(value = child_ftes, color = dpth, title = nazev,
           dpth = dpth - 1,
           analyticky = str_detect(tolower(nazev), "anal|koncep|evalu|progn|expert|výzk|hodno|monit"),
           color = recode(dpth, `1` = "blue", `2` = "purple", `3` = "orange", `4` = "red"),
           color = if_else(analyticky, "firebrick", "darkgrey"),
           size = if_else(analyticky, 50, value),
           value = if_else(dpth == 1, 0, value)),
  orgdata_jednomini_edges) |>
  visOptions(collapse = TRUE) %>%
  visEdges(arrows = "to", smooth = TRUE) %>%
  visEdges(arrows = "to", smooth = list(enabled = TRUE, type = "cubicBezier")) %>%
  visNodes() |>
  visIgraphLayout() |>
  # visLayout(hierarchical = TRUE, randomSeed = 1, improvedLayout = TRUE) |>
  # visHierarchicalLayout(sortMethod = "directed",
  #                       direction = "LR",
  #                       nodeSpacing = 100,
  #                       parentCentralization = F,
  #                       levelSeparation = 1500) |>
  visInteraction(zoomView = TRUE)

vn_jednomini

vn_druhemini <- visNetwork(orgdata_druhemini_nodes |>
                             mutate(value = child_ftes, color = dpth, title = nazev,
                                    analyticky = str_detect(tolower(nazev), "anal|koncep|evalu|progn|expert|výzk|hodno|monit"),
                                    color = recode(dpth, `1` = "blue", `2` = "purple", `3` = "orange", `4` = "red"),
                                    color = if_else(analyticky, "green", color),
                                    value = if_else(dpth == 1, 0, value)),
                           orgdata_druhemini_edges) |>
  visOptions(collapse = TRUE) %>%
  visEdges(arrows = "to", smooth = TRUE) %>%
  visEdges(arrows = "to", smooth = list(enabled = TRUE, type = "cubicBezier")) %>%
  visLayout(hierarchical = TRUE, randomSeed = 1, improvedLayout = TRUE) |>
  visHierarchicalLayout(sortMethod = "directed",
                        direction = "LR",
                        nodeSpacing = 100,
                        parentCentralization = F,
                        levelSeparation = 1500) |>
  visInteraction(zoomView = TRUE)

vn_jednomini
vn_druhemini

vn_jednomini2 <- visNetwork(orgdata_jednomini_nodes |>
                              mutate(value = child_ftes, color = dpth, title = nazev,
                                     analyticky = str_detect(tolower(nazev), "anal|koncep|evalu|progn|expert|výzk|hodno|monit|strateg"),
                                     color = recode(dpth, `1` = "blue", `2` = "purple", `3` = "orange", `4` = "red", `5` = "darkred"),
                                     color = if_else(analyticky, "green", color),
                                     value = if_else(dpth == 1, 0, value)),
                            orgdata_jednomini_edges) |>
  visOptions(collapse = TRUE) %>%
  visNodes(color = list(border = "white", selected = list(border = "blue")),
           mass = 1.4,scaling = list(min = 15, max = 50)) |>
  visEdges(arrows = "to", smooth = list(enabled = TRUE, type = "cubicBezier")) %>%
  visLayout(hierarchical = TRUE, randomSeed = 1, improvedLayout = TRUE) |>
  visHierarchicalLayout(sortMethod = "directed",
                        direction = "LR",
                        nodeSpacing = 100,
                        parentCentralization = F,
                        levelSeparation = 1500) |>
  visInteraction(zoomView = TRUE) |>
  visPhysics(enabled = TRUE)

vn_jednomini2

visNetwork::visSave(vn_jednomini, "vis_vn_jednomini.html", selfcontained = FALSE)
visNetwork::visSave(vn_druhemini, "vis_vn_druhemini.html", selfcontained = FALSE)

widget_saved <- htmlwidgets::saveWidget(vn_jednomini, "vis_vn_jednomini.html", selfcontained = FALSE, libdir = "vis-libs")
htmlwidgets::saveWidget(vn_druhemini, "vis_vn_druhemini.html", selfcontained = FALSE, libdir = "vis-libs")
htmlwidgets::saveWidget(vn_jednomini2, "vis_vn_jednomini2.html", selfcontained = FALSE, libdir = "vis-libs")

orgdata_graph |>
  activate(nodes) |>
  mutate(x = tidygraph::bfs_after(nazev == "12012281"))

visNetwork(orgdata_nodes_uv, orgdata_edges_uv)

targets::tar_load(orgdata_graph)

make_org_visnetwork(orgdata_graph, "MMR ČR", igraph_layout = "layout_with_fr")
make_org_visnetwork(orgdata_graph, "MMR ČR", igraph_layout = "layout_with_kk")
make_org_visnetwork(orgdata_graph, "GFŘ", igraph_layout = "layout_nicely")

make_org_visnetwork(orgdata_graph, urad_zkr = "ÚP ČR")
make_org_visnetwork(orgdata_graph, "ÚP ČR", igraph_layout = "layout_with_kk")
make_org_visnetwork(orgdata_graph, "GFŘ", igraph_layout = "layout_with_kk")

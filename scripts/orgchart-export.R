source("_targets_packages.R")
plan(multisession)

source("R/load-orgchart.R")

# Načtení -----------------------------------------------------------------

targets::tar_load(starts_with("orgdata"))

class(orgdata_graph)

org_gexf <- rgexf::igraph.to.gexf(orgdata_graph)
org_gexf
head(org_gexf)
write_lines(org_gexf, "org.gexf")



xml2::write_xml(org_gexf, "org.gexf")
rgexf::write.gexf()

orgdata_graph_for_export <- igraph::add_layout_(graph = orgdata_graph, igraph::as_tree())
igraph::write_graph(orgdata_graph_for_export, file = "org.graphml", format = "graphml")

plot(orgdata_graph_for_export)
plot(orgdata_graph)

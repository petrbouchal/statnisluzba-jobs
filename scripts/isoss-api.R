library(jsonlite)

dt <- fromJSON("https://portal.isoss.cz/irj/portal/anonymous/mvrest?path=/eosm-public-offer&active=&dobaUrcite=&idslm=&page=2&pageSize=10&predstaveny=&qualification=&region=&salaryClass=&sortColumn=offerId&sortOrder=-1&startDateFrom=&startDateTo=&state=CZ&uvazek=&vyberoveRizeni=")
dt0 <- fromJSON("https://portal.isoss.cz/irj/portal/anonymous/mvrest?path=/eosm-public-offer&page=1&pageSize=1000")

urady <- fromJSON("https://portal.isoss.cz/irj/portal/anonymous/mvrest?path=/eosm-enums/offices")
obory <- fromJSON("https://portal.isoss.cz/irj/portal/anonymous/mvrest?path=/eosm-enums/qualifications")
okresy <- fromJSON("https://portal.isoss.cz/irj/portal/anonymous/mvrest?path=/eosm-enums/regions")

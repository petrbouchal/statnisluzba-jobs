# Czech Civil Service salary calculator

This repository contains code and some inputs for generating

- a listing of jobs advertised by the Czech Civil Service (Státní služba) 
- a calculation of potential salary ranges for those jobs
- a basic web UI for this, available at <https://sluzstatu.netlify.app/> 

It works with three sources of data:

- job listings - [open data from the ISoSS system](https://portal.isoss.gov.cz/opendata/ISoSS_Opendata_EOSM.xml) scraped daily using [petrbouchal/statnipokladna-downloader](https://github.com/petrbouchal/statnisluzba-downloader)
- base salary tables - collected [from the ISP website](https://www.mfcr.cz/cs/ministerstvo/informacni-systemy/is-o-platech) run by the Ministry of Finance
- rules on various salary supplements - exctracted manually from documents and embedded in code

The output is

- a JSON file with all jobs in the database
- JSON files - one for each job - with calculated potential salary range
- a webpage using those inputs to display the listing and salary ranges

The code is organised as a {targets} pipeline and a Quarto webpage using ObservableHQ elements.

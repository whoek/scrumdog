# scrumdog

scrumdog is a free utility to export Jira Cloud Issues to a local SQLite database

For Windows, Linux and Mac binaries -- see <https://github.com/whoek/scrumdog-binaries>    
For notes on how to use -- see <https://scrumdog.app/>   
For background on the project -- see <https://whoek.com/b/jira-to-sqlite-with-scrumdog>

## Pre-requisites to compile the program

Install OCaml -- see <https://ocaml.org/>

Libraries used:
- `yojson` - parsing library for the JSON format 
- `sqlite3` - SQLite3 bindings for OCaml 
- `cohttp-lwt-unix` - library for HTTPS clients 
- `tls-lwt` - encryption protocal to allow secure HTTPS

To install the libraries run `opam install yojson sqlite3 cohttp-lwt-unix tls-lwt`


## To compile

`opam build`


# Scrumdog

Scrumdog is a free utility that download Jira Cloud Issues and save it to a local SQLite database.

For Windows, Linux and Mac binaries -- see <https://github.com/whoek/scrumdog-binaries>    
For notes on how to use scrumdog -- see <https://scrumdog.app/>   
For background on the project -- see <https://whoek.com/b/jira-to-sqlite-with-scrumdog>

## Pre-requisites to compile the program

Scrumdog is written in [OCaml](https://ocaml.org/).  

Libraries used:
- `dune` - build system
- `yojson` - JSON parsing library
- `sqlite3` - SQLite3 bindings
- `cohttp-lwt-unix` - HTTPS client library
- `tls-lwt` - encryption protocal to allow secure HTTPS

After installing OCaml -- you can install the above libraries with `opam install dune yojson sqlite3 cohttp-lwt-unix tls-lwt`


## To compile

`dune build`

This will create the same binaries that are available from the [download](https://github.com/whoek/scrumdog-binaries) page.

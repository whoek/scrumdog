(*                    80 width                                                *)
open Printf

open Lwt                (* to use BIND >>=  *)
module C = Cohttp
module CL =  Cohttp_lwt
module CC =  Cohttp_lwt_unix.Client

let show_status = true

let status txt =
  if show_status then begin
      let t = Unix.localtime @@ Unix.time () in
      printf "%d-%02d-%02d %02d:%02d:%02d - %s\n%!"
        (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour
        t.tm_min t.tm_sec txt
    end
  else printf "*%!"

let log txt =
  printf "%s\n%!" txt

let create_logs_folder () =
  try Unix.mkdir "logs" 0o777 with
    _ -> ()

(*  string -> string -> string -> string  *)
let replace this that s =
  Str.global_replace (Str.regexp_string this) that s


(* Same as Excel LEFT : string -> int -> string  *)
let left s n =
  let len = String.length s in
  if len >= n then String.sub s 0 n else String.sub s 0 len

(* same as Excel RIGHT : string -> int -> string  *)
let right s n =
  let len = String.length s in
  if len >= n then String.sub s (len - n) n else s

let mask txt  = String.mapi (fun n x ->
                    if n > 2 then '*' else x
                  ) txt

let option_to_string = function
    Some x -> x
  | None -> ""

let bool_option_to_int = function
    Some true -> 1
  | Some false -> 0
  | None -> -1

let field_type x =
  match x with
  | "number"  -> "NUMERIC"
  | _         -> "TEXT"

module Args = struct

  let year = 1900 + (Unix.localtime (Unix.time ())).tm_year
             |> string_of_int

  let license =
    Printf.sprintf
      "\n\
       Copyright (c) 2020-%s Willem Hoek\n\
       All rights reserved.\n\n\
       Redistribution and use in source and binary forms, with or without\n\
       modification, are permitted provided that the following conditions\n\
       are met:\n\
       1. Redistributions of source code must retain the above copyright\n\
       \   notice, this list of conditions and the following disclaimer.\n\
       2. Redistributions in binary form must reproduce the above copyright\n\
       \   notice, this list of conditions and the following disclaimer in the\n\
       \   documentation and/or other materials provided with the distribution.\n\
       3. The name of the author may not be used to endorse or promote products\n\
       \   derived from this software without specific prior written permission.\n\n\
       THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n\
       IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n\
       OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n\
       IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n\
       INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n\
       NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n\
       DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n\
       THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n\
       (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n\
       THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
      year

  let valid_file fn =
    try
      Sys.file_exists fn && not (Sys.is_directory fn)
    with
      _ -> false

  let arg_version () =

    printf "\nscrumdog %s (%i-bit)\n%!" Version.version  Sys.word_size;
    printf "Release-Date: %s\n%!" Version.release_date;
    printf "Features: issues links comments subtasks labels components\n%!"

  let arg_manual () =
    let txt = sprintf  "start https://scrumdog.app/?v=%s" Version.version  in
    let _ = Sys.command txt in
    ()

  (* string -> string -> unit *)
  let save_text_file file contents =
    let oc  = open_out file in
    fprintf oc "%s" contents;
    close_out oc

  (* string -> string *)
  let read_text_file fname =
    let txt = ref "" in
    let chan = open_in fname in
    try
      while true; do
        txt := !txt ^ input_line chan ^ "\n"
      done; ""
    with End_of_file ->
      close_in chan;
      !txt

  (* string array -> ()  *)
  let print_jql () =
    let right n str =
      let len = String.length str in
      if len > n  then String.sub str (len - n) n
      else "" in
    Sys.readdir "."
    |> Array.to_list
    |> List.filter (fun x -> String.lowercase_ascii (right 4 x) = ".jql")
    |> List.filter (fun x -> valid_file x)    (* exclude directories *)
    |> (fun x -> if List.length x > 0
                 then begin
                     printf "%s" "Queries:\n";
                     List.iter (printf " %s\n") x
                   end
                 else ()
       )

  let arg_zero () =
    Printf.printf "\nscrumdog: try 'scrumdog --help' for more information\n"

  let arg_help () =
    ["Usage:  scrumdog [Query or Option]"
    ; ""
    ; "Options:"
    ; ""
    ; " -h, --help    Get help on commands"
    ; " -m, --manual  Open 'https://scrumdog.app/' in browser"
    ; " -j, --jql     Create a sample query file named 'sample.jql'"
    ; " -v, --version Show version number and quit"
    ; " -l, --license Show license"
    ; ""] |> String.concat "\n" |> printf "\n%s%!";
    print_jql ()

  let arg_license () =
    Printf.printf "%s" license

  let arg_one arg =
    Printf.printf "Command line:  %s\n" arg

  let arg_many () =
    Printf.printf "\n%s\n%!" "scrumdog: to many options provided";
    Printf.printf "%s\n%!"   "scrumdog: try 'scrumdog --help' for more information"

  let sample_config_jql =
    [
      ""
    ; "[server]     https://yourcompany.atlassian.net/"
    ; "[email]      name@yourcompany.com"
    ; "[api_token]    xxxxYOURAPITOKENxxxxxxxxx"
    ; "[db_filename]   jira.db"
    ; "[db_table_prefix]  zz"
    ; "[jql]              project = zz"  ; "" ; ""
    ; "" ; "" ; "" ; ""
    ; "[fields]"
    ; ""
    ; "assignee   assignee.displayName"
    ; "creator    creator.displayName"
    ; "issuetype  issuetype.name"
    ; "priority   priority.name"
    ; "project    project.key"
    ; "reporter   reporter.displayName"
    ; "status     status.name"
    ; "votes      votes.votes"
    ; "watches_count        watches.watchCount"
    ; "aggregate_progress   aggregateprogress.progress"
    ; "aggregate_total      aggregateprogress.total"
    ; "parent_key           parent.key"
    ; "progress             progress.progress"
    ; "progress_total       progress.total"
    ; "comments_total       comment.total"
    ; "comments_all         comment.comments"
    ; ""
    ] |> String.concat "\n"

  let check_args arg =
    let fn =
      match (Array.length arg - 1) with
      | 0 -> arg_zero (); exit 0
      | 1 ->
         begin
           let x = String.lowercase_ascii arg.(1) in
           match x with
           | "-l" | "--license" -> arg_license ();
                                   exit 0
           | "-h" | "--help" -> arg_help ();
                                exit 0
           | "-m" | "--manual" -> arg_manual ();
                                  exit 0
           | "-j" | "--jql" -> save_text_file "sample.jql" sample_config_jql;
                               printf "\nscrumdog: file 'sample.jql' created\n";
                               exit 0
           | "-v" | "--version" -> arg_version ();
                                   exit 0
           | _ -> arg.(1)
         end
      | _ -> begin
          arg_many ();
          exit 0
        end
    in
    if not (valid_file fn) then begin
        printf "\nscrumdog: '%s' is not a valid file or option%!" fn;
        arg_zero ();   (* no need to end above with \n *)
        exit 0
      end;
    fn

end

(* ========================================================================== *)

module ParseJql = struct

  type t =
    { server    : string;
      api_token : string;
      jql       : string;
      email     : string;
      db_filename : string;
      db_table    : string;
      fields    :  string list list
    }

  let add_length lst =
    List.map (
        fun tag -> tag, String.length tag
      ) lst

  (* string -> string -> int *)
  let find sub str =
    let sub' = String.lowercase_ascii sub in
    let str' = String.lowercase_ascii str in
    try Str.search_forward (Str.regexp_string sub') str' 0
    with Not_found -> -1

  let add_find str lst =
    List.map (
        fun (tag, len) -> tag, len, (find tag str)
      ) lst


  let add_start_txt lst =
    List.map (
        fun (tag, len, s) ->
        tag, len, s, len + s
      ) lst

  (* end is lowest next number *)
  let add_end_txt str lst =
    let rec end_txt lst' st acc =
      match lst' with
      | [] -> acc
      | (_ ,_ ,s ,_) :: tl -> begin
          if s > st  && s < acc
          then end_txt tl st s
          else end_txt tl st acc
        end in
    let str_len = String.length str in
    List.map (
        fun (tag, len, s, st) ->
        tag, len, s, st, (end_txt lst st str_len)
      ) lst

  let add_tag_txt str raw lst =
    List.map (
        fun (tag, len, s, st, et) ->
        if tag = "[fields]" then
          tag, len, s, st, et, String.sub raw st (et - st)
                               |> String.trim
        else
          tag, len, s, st, et, String.sub str st (et - st)
                               |> String.trim
      ) lst

  (* string -> string list list *)
  let parse_fields s =
    String.split_on_char '\n'                   s       (* split on lines *)
    |> List.map (fun x -> Str.split (Str.regexp " +") x)    (* split words*)
    |> List.filter (fun x -> (List.length x) > 0)    (* remove blank lines*)


  let create_record lst =
    let missing_tag = ref false in
    let server = ref "" in
    let api_token = ref "" in
    let jql = ref "" in
    let email = ref "" in
    let db_filename = ref "" in
    let db_table = ref "" in
    let fields = ref [[""]] in
    List.iter (
        fun (tag, _, s, _, _, txt) ->
        if tag <> "[fields]" && s = -1 && not !missing_tag then begin
            printf "\n%s\n%!" "scrumdog: not all required tags found in configuration file";
          end;
        if tag <> "[fields]" && s = -1 then begin
            missing_tag := true;
            printf "scrumdog: missing tag: %s\n" tag;
          end;

        match tag with
        | "[server]" -> server := txt
        | "[api_token]" -> api_token:= txt
        | "[jql]" -> jql := txt
        | "[email]" -> email := txt
        | "[db_filename]" -> db_filename := txt
        | "[db_table_prefix]" -> db_table := txt
        | "[fields]" -> fields := parse_fields  txt
        | _ -> ()
      ) lst;
    if !missing_tag then exit 0;

    (* remove everything after / *)
    let trim_url url =
      let to_int = function | Some x -> x | None -> 0 in
      if String.length url > 10 then
        let pos = to_int @@ String.index_from_opt  url 9  '/'  in
        if pos = 0 then url
        else (left url pos) ^ {|/|}
      else url
    in

    { server = trim_url @@ !server;
      api_token = !api_token;
      jql = !jql;
      email = !email;
      db_filename = !db_filename;
      db_table = !db_table;
      fields = !fields
    }

  (*

    Sample usage of tuple to find TAGS

    "[server]", 8, 10,  17, 26, "https....."
    ^        ^   ^    ^   ^     ^
    |        |   |    |   |     |
    tag ----+        |   |    |   |     |
    length of tag ---+   |    |   |     |
    start of tag  -------+    |   |     |
    start of tag TEXT --------+   |     |
    end of tag TEXT --------------+     |
    tag TXT ----------------------------+


   *)

  let tags = ["[server]"; "[email]"; "[api_token]"; "[jql]";
              "[db_filename]"; "[db_table_prefix]"; "[fields]" ]

  let get_jql_tokens raw : t =
    let str = raw
              |> replace "\r" " "
              |> replace "\n" " "   in
    tags
    |> add_length            (* str not required *)
    |> add_find  str
    |> add_start_txt         (* str not required *)
    |> add_end_txt  str
    |> add_tag_txt  str raw   (* add raw to get [fields] *)
    |> create_record

end

(* ==================================================================== *)

module Db = struct

  module S = Sqlite3

  let gracefully_exit db error message =
    let () = prerr_endline (S.Rc.to_string error) in
    let () = prerr_endline (S.errmsg db) in
    let () = prerr_endline message in
    let _closed = S.db_close db in
    let () = prerr_endline "db: exiting db ..." in
    exit 1


  let exec_sql ?(msg = "db: unable to execute sql") db sql =
    match S.exec db sql with
    | S.Rc.OK -> ()
    | r ->
       let message =  msg in
       (* printf "SQL ERROR: %s\n" sql; *)
       gracefully_exit db r message


  let create_tables ~dbf ~tbl =
    let sql = sprintf "
                       DROP TABLE IF EXISTS %s_fields;

                       CREATE TABLE %s_fields (
                       id TEXT PRIMARY KEY,
                       key TEXT,
                       name TEXT,
                       untranslatedName TEXT,
                       custom INTEGER,
                       orderable INTEGER,
                       navigable INTEGER,
                       searchable INTEGER,
                       clauseNames TEXT,
                       schema TEXT,

                       schema_type TEXT,
                       schema_items TEXT,
                       schema_system TEXT,
                       schema_custom TEXT,
                       schema_customId TEXT,

                       json_field TEXT,
                       db_field TEXT,
                       field_type TEXT
                       );

                       DROP TABLE IF EXISTS %s_issues;
                       CREATE TABLE %s_issues  (
                       id TEXT PRIMARY KEY,
                       self TEXT,
                       key TEXT,
                       fields TEXT
                       );      "  tbl tbl tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql

  let insert_fields ~dbf ~tbl ~fields =
    let db = S.db_open dbf in
    let rec _add = function
      | [] -> ()
      | (id, key, nam, cus, ord, nav, sea, unt, cla, sch) :: t ->
         let sql =
           sprintf {|INSERT INTO %s_fields
                    (id, key, name, custom, orderable, navigable,
                    searchable, untranslatedName, clauseNames, schema)
                    VALUES ('%s','%s','%s', %d, %d,
                    %d, %d, '%s', '%s','%s') |}
             tbl
             id key nam cus ord nav sea unt cla sch
         in
         let () = begin match S.exec db sql with
                  | S.Rc.OK -> ()
                  | r -> prerr_endline (S.Rc.to_string r);
                         prerr_endline (S.errmsg db)
                  end
         in _add t
    in _add fields

  let insert_issues ~dbf ~tbl ~issues =
    let db = S.db_open dbf in
    let rec insert = function
      | [] -> ()
      | (id, self, key, fields) :: t ->
         let sql =
           sprintf {|INSERT INTO %s_issues
                    (id, self, key, fields)
                    VALUES('%s', '%s', '%s', '%s') |}
             tbl
             id self key fields
         in
         let () = begin match  S.exec db sql with
                  | S.Rc.OK -> ()
                  | r -> prerr_endline (S.Rc.to_string r);
                         prerr_endline (S.errmsg db)
                  end
         in insert t
    in insert issues


  (* string -> string -> string array list * string array *)
  let  select ~dbf ~sql  =
    let db = S.db_open dbf in
    let data = ref []  in
    let head = ref [||] in
    let first_row = ref true in
    let cb row headers =
      if !first_row then begin
          data := [ row ];
          head := headers;
          first_row := false;
        end
      else begin
          data := !data @ [ row ];
        end;
    in
    let code = S.exec db ~cb sql in
    ( match code with
      | S.Rc.OK -> ()
      | r ->  prerr_endline (S.Rc.to_string r);
              prerr_endline (S.errmsg db)
    ) ;
    (!data, !head)      (* data: array list,  head:  array *)

  let update_fields_table ~dbf ~tbl =
    let sql = sprintf {|-- use ID field for Jira fields
                       update %s_fields SET
                       json_field = id,
                       db_field =  replace(lower(trim(id))," ",""),
                       field_type =  json_extract(schema, '$.type'),
                       schema_type = json_extract(schema, '$.type'),
                       schema_items = json_extract(schema, '$.items'),
                       schema_system = json_extract(schema, '$.system'),
                       schema_custom = json_extract(schema, '$.custom'),
                       schema_customId = json_extract(schema, '$.customId')
                       ;
                       -- use NAME field for CUSTOMFIELDS
                       update %s_fields set
                       db_field = replace(lower(trim(name))," ","")
                       where id like 'customfield_%%';
                       |}   tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql

  let lst_to_sql lst =
    let to_txt = function | Some x -> x  | None -> "" in
    let txt = ref "" in
    List.iter (fun x->
        txt := !txt ^ sprintf  {| ("%s", "%s", "%s"), |}
                        (List.nth_opt x 0 |> to_txt)
                        (List.nth_opt x 1 |> to_txt)
                        (List.nth_opt x 2 |> to_txt)
      ) lst;
    !txt

  let create_comments_table ~dbf ~tbl =
    let sql =
      sprintf
        {|
         DROP TABLE IF EXISTS %s_comments;

         CREATE TABLE %s_comments as

         with comments as (
         select %s_issues.key, json.key as json_key, value,
         instr (fullkey, '.author') author,
         instr (fullkey, 'created') created,
         instr (fullkey, 'updated') updated,
         instr (fullkey, 'updateAuthor') update_author,
         substr(fullkey, instr(fullkey, '[') + 1,
         instr (fullkey, ']') - instr (fullkey,'[') - 1) comment_id,
         substr (
         substr (fullkey,instr(fullkey, 't[' ) + 2, 5),0,
         instr  (
         substr (fullkey,instr(fullkey, 't[' ) + 2, 5),
         ']')
         ) line_id,
         json.*
         from
         %s_issues, json_tree(json_extract(fields, '$.comment')) json
         where
         json.key in ('displayName','text','created','updated','created')
         order by parent, id
         ),
         line_concat as (
         select key, comment_id, line_id, group_concat(value, ' ') comment
         from comments where "key:1" = 'text'
         group by key, comment_id, line_id),
         comment_concat as (select key, comment_id, group_concat(comment,char(10)) comment
         from line_concat group by "key", comment_id),
         created as (select key, comment_id, value created from comments where created > 0),
         updated as (select key, comment_id, value updated from comments where updated > 0),
         created_author  as (select key, comment_id, value created_author
         from comments where author  > 0),
         updated_author  as (select key, comment_id, value updated_author
         from comments where update_author > 0)

         select distinct created.key,
         (created.comment_id + 0) comment_id,
         created.created, created_author,
         updated.updated, updated_author.updated_author,
         comment_concat.comment
         from created
         left join updated on updated.key = created.key
         and updated.comment_id = created.comment_id
         left join created_author on created_author.key = created.key
         and created_author.comment_id = created.comment_id
         left join updated_author on updated_author.key = created.key
         and updated_author.comment_id = created.comment_id
         left join comment_concat on comment_concat.key  = created.key
         and comment_concat.comment_id = created.comment_id
         order by created.key, comment_id
         ;

         -- convert date to ISO-8601 format by inserting ':'
         update %s_comments set created = substr(trim(created),0,27)
         ||':'|| substr(trim(created),27,30) where length(trim(created)) = 28;
         update %s_comments set updated = substr(trim(updated),0,27)
         ||':'|| substr(trim(updated),27,30) where length(trim(updated)) = 28;

         |} tbl tbl tbl tbl tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql


  let create_links_table ~dbf ~tbl =
    let sql =
      sprintf {|
               DROP TABLE IF EXISTS %s_links;

               create table %s_links as
               select %s_issues.key,
               json_extract(value, '$.type.name') type_name,
               json_extract(value, '$.type.inward') type_inward,
               json_extract(value, '$.type.outward') type_outward,
               json_extract(value, '$.outwardIssue.key') outward,
               json_extract(value, '$.inwardIssue.key')  inward
               from
               %s_issues, json_each(
               json_extract(fields, '$.issuelinks')
               )
               order by %s_issues.key desc, type_name
               ;

               |} tbl tbl tbl tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql


  let create_subtasks_table ~dbf ~tbl =
    let sql =
      sprintf {|
               DROP TABLE IF EXISTS %s_subtasks;

               create table %s_subtasks as
               select %s_issues.key,
               json_extract(value, '$.key') subtask
               from
               %s_issues, json_each(
               json_extract(fields, '$.subtasks')
               )
               ORDER BY %s_issues.key desc
               ;

               |} tbl tbl tbl tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql


  let create_labels_table ~dbf ~tbl =
    let sql =
      sprintf {|
               DROP TABLE IF EXISTS %s_labels;

               create table %s_labels as
               select
               %s_issues.key,
               value as labels
               from  %s_issues,
               json_each(
               json_extract(fields, '$.labels')
               )
               order by %s_issues.key desc
               ;

               |} tbl tbl tbl tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql


  let create_components_table ~dbf ~tbl =
    let sql =
      sprintf {|
               DROP TABLE IF EXISTS %s_components;

               create table %s_components as
               select %s_issues.key,
               json_extract(value, '$.name') components
               from
               %s_issues, json_each(
               json_extract(fields, '$.components')
               )
               order by %s_issues.key desc
               ;

               |} tbl tbl tbl tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql


  (* add config fields to xx_fields table *)
  let add_config_fields fields  ~dbf  ~tbl =
    let sql =
      sprintf {|
               -- insert CONFIG FIELDS in table
               INSERT INTO %s_fields
               (db_field, json_field, field_type) VALUES
               %s
               ('dummy','','') ;

               -- update EXISTING fields
               with new as (
               select db_field, json_field, field_type
               from %s_fields where id is NULL)
               update %s_fields SET
               json_field = (select json_field from new
               where db_field = %s_fields.db_field),
               field_type = (select field_type
               from new where db_field = %s_fields.db_field)
               where db_field in (select db_field from new)
               ;

               -- delete where FIELDS already in TABLE
               delete from %s_fields
               where (id is null AND db_field in
               (select db_field from %s_fields where id is not null)
               )
               -- don't touch main fields
               or db_field in  ('dummy','id','self','key','field') ;

               -- delete duplicate records ....
               with dupe as (
               select db_field, count(*) tel
               from %s_fields group by db_field
               having tel > 1)
               delete from %s_fields
               where db_field in (select db_field from dupe)
               |}
        tbl (lst_to_sql fields)
        tbl tbl tbl tbl
        tbl tbl
        tbl tbl
    in
    let db = S.db_open dbf in
    exec_sql db sql


  (** This will insert COLUMNS for every Jira Issue FIELD  *)
  let insert_fields_in_issues_table ~dbf ~tbl =
    let alter = ref "" in
    let sql = sprintf
                "select db_field, field_type
                 from %s_fields
                 where  db_field is not null
                 -- and field_type is not NULL" tbl  in
    let data, _ = select ~dbf ~sql  in
    List.iter (fun x ->
        alter := !alter ^
                   sprintf "alter table %s_issues add \"%s\" %s;\n"  tbl
                     (option_to_string @@ Array.get x 0)
                     (field_type @@ option_to_string @@ Array.get x 1)
      ) data;
    let db = S.db_open dbf in
    exec_sql db !alter


  let update_issue_fields ~dbf ~tbl =
    let sql = sprintf
                {| select
                 db_field,       -- 0
                 json_field,     -- 1
                 schema_system,  -- 2
                 field_type,     -- 3
                 id              -- 4
                 from %s_fields
                 where  db_field is not null
                 |} tbl  in
    let data, _ = select ~dbf ~sql  in
    let update = ref @@ sprintf "update %s_issues SET \n" tbl  in
    List.iter (fun x ->
        let db_field   = option_to_string @@ Array.get x 0 in
        let json_field = option_to_string @@ Array.get x 1 in
        let field_type = option_to_string @@ Array.get x 3 in
        match field_type with
        | _ -> update :=
                 !update ^ sprintf "%s = json_extract(fields,'$.%s'),\n"
                             db_field json_field
      ) data;


    (* user define functions *)
    let db = S.db_open dbf in
    S.create_fun1 db "willem" (fun _ -> S.Data.TEXT "willem");

    update := !update ^ "key = key \n";
    (* printf "SQL: \n%s\n\n" !update;  *)
    exec_sql db !update


  (* id;  key;  name;  custom;  db_field   *)
  let get_field_names ~dbf ~tbl =
    let sql = sprintf  {| select id, key, name, custom, db_field
                        from %s_fields
                        order by db_field;  |} tbl  in
    let data, _ = select ~dbf  ~sql in
    data


  (* only keep a..z   0..9   &    _ *)
  let strip_spec_char s =
    let result = ref  "" in
    String.iter (fun x ->
        if x >= 'a' && x <= 'z' || x >= '0' && x <= '9' || x = '_'
        then result := !result ^ Char.escaped x
      ) s;
    !result


  (* fix duplicate in db_field name  *)
  let fix_duplicate_field_names ~dbf ~tbl =
    let is_dupe name dta =
      let n = ref 0 in
      List.iter (fun x ->
          let name' = option_to_string  @@ Array.get x 2  in
          if name = name' then n := !n + 1
        ) dta;
      !n > 1  in
    let data = get_field_names ~dbf ~tbl  in
    let checking = ref true in
    let counter = ref 1  in

    List.iter (fun x ->
        let db_field = strip_spec_char
                       @@ option_to_string
                       @@ Array.get x 4  in    (* 5th field in array *)
        Array.set x 4 @@ Some db_field
      ) data;

    while !checking do
      checking := false;   (* loop only if a dupe is found *)
      List.iter (fun x ->
          let name = option_to_string  @@ Array.get x 2  in
          if (is_dupe name data)  (* && custom = "1"   *)
          then begin
              Array.set x 2 @@ Some (name ^ "z" ^ string_of_int !counter);
              counter := !counter + 1;
              checking := true
            end
        ) data;
    done;

    (* update database *)
    let one_update tbl db_field id =
      sprintf  {| update %s_fields SET db_field = "%s" where id = "%s";  |}
        tbl db_field id in
    let update = ref "" in
    List.iter (fun x ->
        let id = option_to_string  @@ Array.get x 0  in
        let db_field = option_to_string  @@ Array.get x 4  in
        update := !update ^ one_update tbl db_field id;
      ) data;
    let db = S.db_open dbf in
    exec_sql db !update

end

(* ==================================================================== *)

module Response = struct
  type t =
    { code: int;
      (* headers: Header.t; *)
      headers: (string * string) list;

      body: string
    }
end

(* ==================================================================== *)

let text_to_file file contents =
  let oc  = open_out file in
  fprintf oc "%s" contents;
  close_out oc


(* text -> bool *)
let valid_file fn =
  try
    Sys.file_exists fn && not (Sys.is_directory fn)
  with
    _ -> false


(* ========================================================================== *)
module Request = struct
  type t =
    { args    : string list;
      headers : (string * string) list;
      body    : string;
      url     : string
    }
end


(* ========================================================================== *)
module Issues = struct
  type t =
    { start_at     : int;
      max_results  : int;
      total        : int;
      issue_count  : int
    }
end


(* ========================================================================== *)


let header_auth ~email ~api_token =
  let h = C.Header.init () in
  let h = C.Header.add_authorization h
            (`Basic (email, api_token)) in
  h

let request_issues_fields  ~server ~email ~api_token  =
  let uri =  server ^ "/rest/api/2/field" in
  let head  = [
      ("Accept","application/json");
      ("Content-Type","application/json")
    ]
  in
  let headers = header_auth ~email ~api_token
                |> (fun h -> Cohttp.Header.add_list h head) in
  CC.get ~headers:headers
    (Uri.of_string uri) >>= fun (resp, body) ->
  let code = resp |> C.Response.status |> C.Code.code_of_status in
  let headers = resp |> C.Response.headers |> C.Header.to_list in
  body |> Cohttp_lwt.Body.to_string >|= fun body ->
  let ret : Response.t =
    { code = code;
      headers = headers;
      body = body }
  in
  ret

let request_issues  ~email ~api_token ~server ~jql ~start =
  let bb = Printf.sprintf {|{
                           "jql": "%s",
                           "maxResults": 1000,
                           "fields": ["*all"],
                           "fieldsByKeys": false,
                           "startAt": %i
                           }|} jql start in
  let uri = server ^  "/rest/api/3/search" in
  let head  = [
      ("Accept","application/json");
      ("Content-Type","application/json")
    ] in

  let headers = header_auth  ~email ~api_token
                |> (fun h -> Cohttp.Header.add_list h head)
  in
  let body = CL.Body.of_string bb  in
  CC.post ~headers  ~body
    (Uri.of_string uri) >>= fun (resp, body) ->
  let code = resp |> C.Response.status |> C.Code.code_of_status in
  let headers = resp |> C.Response.headers |> C.Header.to_list in
  body |> Cohttp_lwt.Body.to_string >|= fun body ->
  let ret : Response.t =
    { code = code;
      headers = headers;
      body = body } in
  ret

let add_records_to_fields_table  ~dbf ~tbl  (resp : Response.t) =
  let json = Yojson.Safe.from_string resp.body  in
  let open Yojson.Safe.Util in
  let fields =
    json
    |> to_list
    |> List.map (fun x ->
           let id  = member "id" x
                     |> to_string_option
                     |> option_to_string in
           let key = member "key" x
                     |> to_string_option
                     |> option_to_string in
           let nam = member "name" x
                     |> to_string_option
                     |> option_to_string in
           let cus = member "custom" x
                     |> to_bool_option
                     |> bool_option_to_int in
           let ord = member "orderable" x
                     |> to_bool_option
                     |> bool_option_to_int in
           let nav = member "navigable" x
                     |> to_bool_option
                     |> bool_option_to_int in
           let sea = member "searchable" x
                     |> to_bool_option
                     |> bool_option_to_int in
           let unt = member "untranslatedName" x
                     |> to_string_option
                     |> option_to_string in
           let cla = member "clauseNames" x
                     |> Yojson.Safe.to_string in
           let sch = member "schema" x
                     |> Yojson.Safe.to_string
                     |> replace "'" "''" in
           id, key, nam, cus, ord, nav, sea, unt, cla, sch
         )
  in
  Db.insert_fields ~dbf ~tbl ~fields


let issues_to_db ~dbf ~tbl  (resp : Response.t) =
  let json = Yojson.Safe.from_string resp.body  in
  let open Yojson.Safe.Util in
  let issues =
    json
    |> member "issues"
    |> to_list
    |> List.map (fun x ->
           member "id" x
           |> to_string_option
           |> option_to_string,
           member "self" x
           |> to_string_option
           |> option_to_string,
           member "key" x
           |> to_string_option
           |> option_to_string,
           member "fields" x
           |> Yojson.Safe.to_string
           |> replace "'" "''"
         ) in
  Db.insert_issues ~dbf ~tbl ~issues


(* int -> string *)
let text_line s n =
  let rec line' lst n =
    if n > 0 then  line' (s :: lst) (n - 1)
    else List.fold_left (fun x acc ->  x ^ acc) "" lst
  in line' [] n


let fields_db_to_file  ~dbf ~tbl =
  (* get list of fields *)
  let sql = sprintf " select db_field, json_field, field_type, name
                     from %s_fields
                     where  db_field is not null
                     order by field_type, db_field ; "   tbl in
  let  data, _ = Db.select ~dbf ~sql in

  (* write out to file *)
  let contents = ref "" in
  let max0 = ref 0 in     (* length of biggest field *)
  let max1 = ref 0 in
  let max2 = ref 0 in
  let max3 = ref 0 in

  (* calc max string width for every column *)
  List.iter (fun x ->
      let fld0 = option_to_string @@ Array.get x 0 in
      let fld1 = option_to_string @@ Array.get x 1 in
      let fld2 = option_to_string @@ Array.get x 2 in
      let fld3 = option_to_string @@ Array.get x 3 in
      if (String.length fld0) > !max0 then max0 := String.length fld0;
      if (String.length fld1) > !max1 then max1 := String.length fld1;
      if (String.length fld2) > !max2 then max2 := String.length fld2;
      if (String.length fld3) > !max3 then max3 := String.length fld3;
    ) data;

  (* file header *)
  contents := !contents ^ sprintf "%-*s %-*s %-*s # %-*s\n"
                            !max0 "db_field"
                            !max1 "json_field"
                            !max2 "field_type"
                            !max3 "name";

  contents := !contents ^ sprintf "%s %s %s %s\n%!"
                            (text_line "=" !max0)
                            (text_line "=" !max1)
                            (text_line "=" !max2)
                            (text_line "=" !max3);

  (* save to file *)
  List.iter (fun x ->
      let fld0 = option_to_string @@ Array.get x 0 in
      let fld1 = option_to_string @@ Array.get x 1 in
      let fld2 = option_to_string @@ Array.get x 2 in
      let fld3 = option_to_string @@ Array.get x 3 in
      contents := !contents ^ sprintf "%-*s %-*s %-*s # %-*s\n"
                                !max0 fld0
                                !max1 fld1
                                !max2 fld2
                                !max3 fld3
    ) data;
  text_to_file (sprintf "./logs/fields_%s.txt"   tbl) !contents


let pp_response (i : Issues.t) =
  status @@
    sprintf
      "Jira: Get Issues  %3d to %3d of %3d"
      i.start_at (i.start_at + i.issue_count) i.total


(* val string_of_header : (string * string) list -> string  *)
let pp_headers t =
  t
  |> List.map (fun (k, v) -> [Printf.sprintf "%s: %s\n" k v])
  |> List.concat
  |> List.fold_left ( ^ ) ""


let response_record = function
  | Ok (c, h, b) -> begin
      let r : Response.t = {code = c; headers = h; body = b} in r
    end
  | Error _ -> (exit 1)


let fields_to_log_files  ~tbl (r : Response.t) =
  text_to_file (sprintf "./logs/response_%s_fields_code.txt"   tbl)
    (string_of_int r.code);
  text_to_file (sprintf "./logs/response_%s_fields_header.txt" tbl)
    (pp_headers r.headers);
  text_to_file (sprintf "./logs/response_%s_fields_body.json"  tbl)
    r.body

let issues_to_log_files ~tbl  (r : Response.t) =
  text_to_file (sprintf "./logs/response_%s_issues_code.txt"   tbl)
    (string_of_int r.code);
  text_to_file (sprintf "./logs/response_%s_issues_header.txt" tbl)
    (pp_headers r.headers);
  text_to_file (sprintf "./logs/response_%s_issues_body.json"  tbl)
    r.body


let get_counts (r : Response.t) : Issues.t =
  let json = Yojson.Safe.from_string r.body  in
  let open Yojson.Safe.Util in
  let start_at =
    json
    |> member "startAt"
    |> to_int in
  let max_results =
    json
    |> member "maxResults"
    |> to_int in
  let total =
    json
    |> member "total"
    |> to_int in
  let issues =
    json
    |> member "issues"
    |> to_list in
  { start_at = start_at;
    max_results = max_results;
    total = total;
    issue_count = (List.length issues)
  }


let response_code_validation (r : Response.t) =
  match r.code with
  | 200 -> ()
  | 400 -> status "Error in config file";
           exit 1
  | 401 -> status "Error in config file";
           status "Email and/or token incorrect or missing";
           exit 1
  | _ ->   exit 1


let jql_validation (r : Response.t) =
  let () =  match r.code with
    | 200 -> ()   (* all good, proceed *)
    | 400 -> status "Error in config file"
    | 401 -> status "Error in config file";
             status "Email and/or token incorrect or missing"
    | _ -> ()
  in
  let json = Yojson.Safe.from_string r.body  in
  let open Yojson.Safe.Util in

  (* TODO  change to pattern match   *)
  let warning = json |> member "warningMessages" in
  if warning = `Null then ()   (* proceed *)
  else begin
      let warning_lst = warning |> to_list in
      if List.length warning_lst > 0 then begin
          status  "Warning message:";
          warning_lst |> List.iter (fun x ->
                             status @@ sprintf "%s " (x |> to_string))
        end
      else ()
    end;

  (* TODO  change to pattern match   *)
  let error = json |> member "errorMessages" in
  if error = `Null then ()    (* proceed *)
  else begin
      let error_lst = error |> to_list in
      if List.length error_lst > 0 then begin
          status "Error message:";
          error_lst
          |> List.iter (fun x ->
                 status @@ sprintf "%s " (x |> to_string));
          exit 1
        end
      else ()
    end

let pp_ini (ini : ParseJql.t)  =
  (* validate url *)
  let validate_url url =
    let x = String.lowercase_ascii @@ left url 8 in
    if x <> "https://" then begin
        status "ERROR: URL should start with https://";
        exit 0
      end in
  log "";
  log @@ sprintf " Configuration file:";
  log @@ sprintf " Jira server:   %s" ini.server;
  (* log @@ sprintf " Jira server:   %s" "*************************";  *)

  validate_url ini.server;

  log @@ sprintf " Jira token:    %s" (mask @@ ini.api_token);
  (* log @@ sprintf " Jira token:    %s" "*************************"; *)
  log @@ sprintf " Email:         %s" ini.email;
  (* log @@ sprintf " Email:         %s" "****************"; *)
  log @@ sprintf " Database file: %s" ini.db_filename;
  log @@ sprintf " Table prefix:  %s" ini.db_table;
  log @@ sprintf " JQL:           %s" ini.jql;
  log "";

exception E of string

let main () =
  printf "\n%s%!"    "**********************************************************************";
  printf "\n%s%!"    "* Thanks for using scrumdog.                                         *";
  printf "\n%s%!"    "* If you need help or have any comments, email:  willem@matimba.com  *";
  printf "\n%s\n%!"  "**********************************************************************";

  create_logs_folder ();
  let fn = Args.check_args Sys.argv in
  let setup_file_raw = Args.read_text_file fn in
  let ini : ParseJql.t = ParseJql.get_jql_tokens  setup_file_raw in
  pp_ini ini;

  let dbf = ini.db_filename in
  let tbl = ini.db_table in
  Db.create_tables    ~dbf  ~tbl;
  status @@ sprintf "SQLite: Create table '%s_fields'"  tbl;
  status @@ sprintf "SQLite: Create table '%s_issues'"  tbl;

  status "Jira: Get Issue Fields";
  let fields  = Lwt_main.run @@ request_issues_fields
                                  ~server:ini.server
                                  ~email:ini.email
                                  ~api_token:ini.api_token
                                  (* |>  response_record in *)
  in
  fields_to_log_files         ~tbl   fields;
  response_code_validation           fields;
  add_records_to_fields_table        ~dbf ~tbl  fields;
  Db.update_fields_table             ~dbf ~tbl;   (* extract SCHEMA fields *)
  Db.fix_duplicate_field_names      ~dbf ~tbl;
  fields_db_to_file                  ~dbf ~tbl;

  Db.add_config_fields  ini.fields   ~dbf ~tbl;
  Db.insert_fields_in_issues_table   ~dbf ~tbl;

  let start = ref 0 in
  let not_last = ref true in
  while !not_last do
    let resp =  Lwt_main.run @@ request_issues
                                  ~jql:ini.jql
                                  ~server:ini.server
                                  ~email:ini.email
                                  ~api_token:ini.api_token
                                  ~start:!start
                                  (* |>  response_record  *)
    in

    issues_to_log_files ~tbl  resp;
    jql_validation            resp;
    issues_to_db              ~dbf  ~tbl resp;
    Db.update_issue_fields    ~dbf  ~tbl;

    let counts : Issues.t =
      get_counts resp in
    pp_response counts;
    not_last := not (counts.issue_count < counts.max_results);
    start := !start + counts.max_results
  done;

  Db.create_links_table     ~dbf ~tbl;
  status @@ sprintf "SQLite: Create table '%s_links'" tbl;
  Db.create_subtasks_table  ~dbf ~tbl;
  status @@ sprintf "SQLite: Create table '%s_subtasks'" tbl;
  Db.create_comments_table  ~dbf ~tbl;
  status @@ sprintf "SQLite: Create table '%s_comments'" tbl;
  Db.create_labels_table  ~dbf ~tbl;
  status @@ sprintf "SQLite: Create table '%s_labels'" tbl;
  Db.create_components_table  ~dbf ~tbl;
  status @@ sprintf "SQLite: Create table '%s_components'" tbl;
  status "Done!\n"

let () =
  main ()

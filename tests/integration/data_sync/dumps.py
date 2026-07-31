"""Shared dump-fixture pieces for the data-sync integration tests."""

# The preamble a real `pg_dump` emits ahead of the COPY header, verbatim.
#
# It is reproduced in full — rather than trimmed to "a couple of SET statements"
# — because of the `set_config('search_path', '')` line. pg_dump blanks the
# search_path for the rest of the session and the restore streams the preamble
# straight into psql, so any SQL the restore emits afterwards resolves nothing
# unqualified. A fixture without this line lets bare function calls (the QC
# block's ST_IsValid and friends) resolve via the default search_path and pass a
# test that fails against a real dump in production.
PG_DUMP_PREAMBLE = (
    "SET statement_timeout = 0;\n"
    "SET lock_timeout = 0;\n"
    "SET idle_in_transaction_session_timeout = 0;\n"
    "SET client_encoding = 'UTF8';\n"
    "SET standard_conforming_strings = on;\n"
    "SELECT pg_catalog.set_config('search_path', '', false);\n"
    "SET check_function_bodies = false;\n"
    "SET xmloption = content;\n"
    "SET client_min_messages = warning;\n"
    "SET row_security = off;\n"
)

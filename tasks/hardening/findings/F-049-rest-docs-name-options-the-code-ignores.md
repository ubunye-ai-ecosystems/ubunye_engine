# F-049: the REST docs name options the code does not read; the api_key example sends no key

**Status:** fixed on fix/f015-rest-on-pandas
**Severity:** major (a documented auth config silently sends no credentials)
**Source:** engine-fixer while fixing F-015, confirmed by the skeptic (2026-09-30)
**Promise:** 3 (the docs are true)

## What happens
`docs/connectors/rest_api.md`, `docs/patterns/rest_api.md`, `docs/patterns/rag.md` and
`docs/config/io.md` printed option names the connector never read:

| The docs said | The code read |
|---|---|
| `auth: {type: api_key, header: ...}` | `type: api_key_header` |
| `auth: {type: api_key, param: ...}` | `type: api_key_query` |
| `pagination.cursor_field` | `pagination.cursor_response_key` |
| `pagination.link_field` | `pagination.next_key` |

An unknown auth type was ignored, so the documented API key example sent a request
with no key at all, and the pagination names were ignored (the default key was used,
so a differently named cursor or link stopped after one page). The docs also said an
offset read stops at a short page (it stops at an empty one), that `page_size` is sent
to the API (it is not), that `link_field` takes a dotted path and the `Link` header
(neither is read), and used `type: timestamp` for text, which both backends refuse.

## Repro
`auth: {type: api_key, header: X-Token, key: k}` against any server: the request has
no `X-Token` header. `pagination: {type: cursor, cursor_field: token}` against an API
whose cursor key is `token`: one page.

## Fix
The docs now print the names the code reads, and describe what offset, cursor and
next-link pagination really do. The old names are also accepted, with a warning,
because configs were written from these docs and a silent "no key" is the worst
outcome: `type: api_key` means `api_key_header` with `header` and `api_key_query`
with `param` (unambiguous: both or neither is refused), `cursor_field` means
`cursor_response_key`, `link_field` means `next_key`. An auth type nobody knows is
now refused by `ubunye validate` (the connectors' `validate_config`) and before any
request, instead of sending nothing (`ubunye/plugins/rest_http.py`, `normalized`,
`config_problems`).

## Evidence
`tests/unit/connectors/test_rest_api_options.py`: 8 tests (the key sent by header and
by query for the old name, on the reader and the writer; `cursor_field` and
`link_field` read every page; unknown, neither and both refused before any request
and by `validate_config`). All 8 fail on the code before the fix and pass after.

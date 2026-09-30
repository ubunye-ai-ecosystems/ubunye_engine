# F-050: a REST API key (and other secrets) in log lines and error messages

**Status:** fixed on fix/f015-rest-on-pandas
**Severity:** major (security: a credential written out in clear)
**Source:** skeptic review of F-015 (2026-09-30), `runtime_attacks.py` case F
**Promise:** 4 (secrets stay secret)

## What happens
With `auth: {type: api_key_query}` the key is a query parameter of every request.
When the API answers with an error:

- the writer logs `RestApiWriter: batch of 2 rows failed — 500 Server Error: ...
  for url: http://host/sink?api_key=SUPERSECRET` at ERROR;
- the reader raises `requests.HTTPError` whose message carries the same URL;
- urllib3 logs every request line at DEBUG: `"POST /sink?api_key=SUPERSECRET HTTP/1.1" 500`;
- an unreachable API raises `ConnectionError` with `url: /x?api_key=SUPERSECRET`.

The same holds for any secret-named parameter under `params` (`access_token`).
E-04 did not see it because its API never failed.

## Repro
`runtime_attacks.py` case F (skeptic scripts): `SUPERSECRET in log output: True`.

## Fix
`ubunye/plugins/rest_http.py`: `secrets_of(cfg)` lists every secret a config sends
(the auth token, key and password and basic auth's encoded pair, an `Authorization`
header or a secret-named header, a secret-named query parameter); `redact` masks
them, and secret-named URL parameters, as `***`. Every request exception raised by
`send` (HTTPError, ConnectionError, any RequestException) is raised again, same
class, same `response`, with its message masked. While the reader or writer runs,
a logging filter on this module's logger, the connector's own logger and
`urllib3.connectionpool` masks the same secrets in every record, and is removed
after. The writer's summary line and error carry the masked URL.

## Evidence
- `tests/unit/connectors/test_rest_api_secrets.py`: 14 tests. Every auth type, on
  the pandas route and on a Spark frame (writer), the reader against a failing API,
  and an unreachable API; each checks the exception text and every log line at
  DEBUG. 7 fail before the fix (the api_key_query writes on both routes, every read,
  since secret-named params leaked there, and the unreachable API) and all 14 pass
  after.
- `runtime_attacks.py` case F after the fix: `SUPERSECRET in log output: False`.

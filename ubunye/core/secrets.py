"""Secrets in config: ``secret://<provider>/<reference>``, fetched only when used.

A config names a secret instead of holding it::

    password: "secret://aws-sm/prod/warehouse#password"
    token:    "secret://env/API_TOKEN"

The reference stays a reference everywhere the config is shown or remembered: the
config hash, ``ubunye plan``, ``ubunye config``, run records and logs. It is
swapped for the value only in the copy handed to a connector, at the moment the
connector reads or writes. So a secret cannot leak through a printed plan or a run
record, and rotating a password does not change the config hash.

Providers are plugins (entry point group ``ubunye.secrets``); the built-in ones are
in :mod:`ubunye.plugins.secrets`. The whole value must be the reference: a secret is
not spliced into a longer string.
"""

from __future__ import annotations

import difflib
import importlib.metadata as md
import importlib.util
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import SecretError

SCHEME = "secret://"
GROUP = "ubunye.secrets"


def is_reference(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(SCHEME)


def parse(value: str) -> Tuple[str, str]:
    """``secret://aws-sm/prod/db#password`` -> (``aws-sm``, ``prod/db#password``)."""
    body = value[len(SCHEME) :]
    provider, _, ref = body.partition("/")
    if not provider or not ref:
        raise SecretError(
            f"'{value}' is not a secret reference.",
            hint="Write secret://<provider>/<reference>, e.g. secret://env/API_TOKEN.",
        )
    return provider, ref


@lru_cache(maxsize=1)
def providers() -> Dict[str, str]:
    """Installed providers: name -> entry point target (nothing is imported)."""
    return {ep.name: ep.value for ep in md.entry_points(group=GROUP)}


def _load(name: str) -> Any:
    for ep in md.entry_points(group=GROUP):
        if ep.name == name:
            return ep.load()
    raise SecretError(f"No secret provider named '{name}'.", hint=_known_hint(name))


def _known_hint(name: str) -> str:
    known = sorted(providers())
    near = difflib.get_close_matches(name, known, n=1)
    hint = f"Installed providers: {', '.join(known)}."
    return f"Did you mean '{near[0]}'? " + hint if near else hint


def references(node: Any, where: str = "") -> List[Tuple[str, str]]:
    """Every (place, reference) in a config tree, for validation and doctor."""
    found: List[Tuple[str, str]] = []
    if isinstance(node, dict):
        for k, v in node.items():
            found += references(v, f"{where}.{k}" if where else str(k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            found += references(v, f"{where}[{i}]")
    elif is_reference(node):
        found.append((where, node))
    return found


def problems(node: Any, where: str = "") -> List[str]:
    """What is wrong with the references in a config, without fetching anything."""
    out: List[str] = []
    installed = providers()
    for place, ref in references(node, where):
        try:
            name, _ = parse(ref)
        except SecretError as exc:
            out.append(f"{place}: {exc.args[0]}")
            continue
        if name not in installed:
            out.append(f"{place}: no secret provider named '{name}'. {_known_hint(name)}")
    return out


def missing_packages(name: str) -> List[str]:
    """Python packages the named provider needs that are not installed."""
    cls = _load(name)
    missing = []
    for module, package in getattr(cls, "REQUIRES_MODULES", ()):
        try:
            found = importlib.util.find_spec(module) is not None
        except (ImportError, ValueError):
            found = False
        if not found:
            missing.append(package)
    return missing


class SecretResolver:
    """Swaps references for values in the copy of a config a connector gets.

    One per run: each secret is fetched once, then reused.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, str] = {}
        self._providers: Dict[str, Any] = {}

    def get(self, value: str) -> str:
        if value in self._cache:
            return self._cache[value]
        name, ref = parse(value)
        provider = self._providers.get(name)
        if provider is None:
            provider = self._providers[name] = _load(name)()
        try:
            secret = provider.get(ref)
        except SecretError:
            raise
        except Exception as exc:  # the provider's own error, with the value never shown
            raise SecretError(
                f"Could not read secret {value}: {type(exc).__name__}: {exc}",
                context={"Provider": name, "Reference": ref},
                hint=getattr(provider, "HINT", None),
            ) from exc
        if not isinstance(secret, str):
            secret = str(secret)
        self._cache[value] = secret
        _remember(secret)
        return secret

    def resolve(self, node: Any) -> Any:
        """A copy of ``node`` with every reference replaced; the input is untouched."""
        if isinstance(node, dict):
            return {k: self.resolve(v) for k, v in node.items()}
        if isinstance(node, list):
            return [self.resolve(v) for v in node]
        if is_reference(node):
            return self.get(node)
        return node


def resolve(node: Any, resolver: Optional[SecretResolver] = None) -> Any:
    """Resolve a config tree once, e.g. in a notebook that calls a connector itself."""
    return (resolver or SecretResolver()).resolve(node)


# A variable whose name says it holds a secret. The value is used by the run as
# given; only what is recorded, sent or printed about it is masked. The same idea as
# Spline's default password filter (AbsaOSS), which exists because lineage leaked them.
#
# Whole words of the name count, not substrings: `db_password`, `apiKey` and
# `GITHUB_TOKEN` are masked; `tokenizer`, `author` and `partition_key` are not.
_SECRET_WORDS = frozenset(
    {
        "password",
        "passwords",
        "passwd",
        "pwd",
        "passphrase",
        "secret",
        "secrets",
        "token",
        "tokens",
        "credential",
        "credentials",
        "auth",
        "apikey",
        "accesskey",
        "secretkey",
        "privatekey",
        "signingkey",
        "sas",
        "signature",
        "cookie",
        "bearer",
        "jwt",
    }
)
_SECRET_PAIRS = frozenset(
    {
        ("api", "key"),
        ("access", "key"),
        ("private", "key"),
        ("secret", "key"),
        ("client", "key"),
        ("session", "key"),
        ("account", "key"),
        ("master", "key"),
        ("signing", "key"),
        ("encryption", "key"),
        ("ssh", "key"),
        ("connection", "string"),
    }
)
# Names about a secret, not holding one: `max_token`, `token_limit`, `auth_mode`,
# `secret_scope`, `password_file`. Counts and limits matter in an engine that calls
# models.
_NOT_SECRET_FIRST = frozenset({"max", "min", "num", "n"})
_NOT_SECRET_LAST = frozenset(
    {
        "limit",
        "limits",
        "count",
        "counts",
        "max",
        "min",
        "mode",
        "type",
        "name",
        "scope",
        "version",
        "domain",
        "expiry",
        "length",
        "path",
        "file",
        "url",
        "id",
        "header",
        "endpoint",
        "enabled",
        "provider",
        "method",
        "format",
    }
)
_WORD = re.compile(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+")
REDACTED = "***"


def looks_secret(name: str) -> bool:
    """True when a variable's name says its value is a secret."""
    words = [w.lower() for w in _WORD.findall(str(name))]
    if not words:
        return False
    if len(words) > 1 and (words[0] in _NOT_SECRET_FIRST or words[-1] in _NOT_SECRET_LAST):
        return False
    if any(w in _SECRET_WORDS for w in words):
        return True
    return any(pair in _SECRET_PAIRS for pair in zip(words, words[1:]))


# `scheme://user:password@host`, also inside `jdbc:postgresql://...`.
_URL_PASSWORD = re.compile(r"(//[^/:@\s]*):([^@/\s]+)@")
# `?password=x&...`, `;password=x;...` (JDBC), a key that says it is a secret.
_URL_PARAM = re.compile(r"([?&;])([^=&;#\s]+)=([^&;#\s]*)")


def redact_url(url: str) -> str:
    """A URL safe to record: the password in `user:password@` and the values of
    secret-named parameters masked. What Spline's default filter learned to do."""
    if not isinstance(url, str) or not url:
        return url
    out = _URL_PASSWORD.sub(lambda m: f"{m.group(1)}:{REDACTED}@", url)
    return _URL_PARAM.sub(
        lambda m: (
            f"{m.group(1)}{m.group(2)}={REDACTED}" if looks_secret(m.group(2)) else m.group(0)
        ),
        out,
    )


def secret_values(variables: Optional[Dict[str, Any]]) -> List[str]:
    """The real values of the secret-looking variables, longest first, to scrub
    wherever they were templated in (a URL, a path, an error message)."""
    found = {
        str(v)
        for k, v in (variables or {}).items()
        if v is not None and looks_secret(k) and len(str(v)) >= 4
    }
    return sorted(found, key=len, reverse=True)


def scrub(node: Any, values: List[str]) -> Any:
    """A copy of ``node`` with every occurrence of ``values`` in its strings masked."""
    if not values:
        return node
    if isinstance(node, str):
        for value in values:
            node = node.replace(value, REDACTED)
        return node
    if isinstance(node, dict):
        return {k: scrub(v, values) for k, v in node.items()}
    if isinstance(node, (list, tuple)):
        return type(node)(scrub(v, values) for v in node)
    return node


def redact_variables(variables: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The variables to record, send or print: secret-looking values masked, and
    ``None`` values left out. The run itself always uses the real values."""
    return {
        k: (REDACTED if looks_secret(k) and v not in ("", None) else v)
        for k, v in (variables or {}).items()
        if v is not None
    }


# --------------------------------------------------------------------------- #
# Masking secrets in free text: error messages, log lines, run records
# --------------------------------------------------------------------------- #

#: Every value a SecretResolver fetched in this process. An error raised later in
#: the run may quote one; the run record must not keep it (F-054 review).
_SEEN: set = set()

#: Shorter values are not masked word for word: "abc" would shred the text.
MIN_SECRET_LENGTH = 4


def _remember(value: str) -> None:
    if isinstance(value, str) and len(value) >= MIN_SECRET_LENGTH:
        _SEEN.add(value)


#: Header names whose value is a credential, however the text quotes them.
_SECRET_HEADERS = frozenset({"authorization", "proxy-authorization", "x-api-key"})
#: URL and JDBC parameter names that hold a secret but that `looks_secret` does not
#: call secret on their own (a variable named `key` is usually not one).
_SECRET_PARAMS = frozenset({"key", "sig"})
# `Authorization: Bearer x`, `'authorization': 'Basic x'`, `X-Api-Key=x`.
_HEADER_VALUE = re.compile(
    r"(?i)(\b(?:proxy-)?authorization|\bx-api-key)(['\"]?\s*[:=]\s*['\"]?)"
    r"((?:bearer|basic|token|digest|negotiate)\s+)?([^'\"\s,;}]+)"
)


def _secret_param(name: str) -> bool:
    return looks_secret(name) or name.lower() in _SECRET_PARAMS


def _base64_cores(value: str) -> List[str]:
    """The part of ``value``'s base64 form that stays the same wherever it sits.

    Base64 codes 3 bytes as 4 characters, so the characters that code the value
    depend on where it starts (``user:password`` is coded differently from
    ``password``) and on what follows. For each of the 3 starts, the characters
    coded from the value alone are kept; standard and URL-safe alphabets.
    """
    import base64

    raw = value.encode("utf-8")
    cores = []
    for lead in range(3):
        coded = base64.b64encode(b"\0" * lead + raw).decode()
        total = lead + len(raw)
        start = (0, 2, 3)[lead]
        end = (total // 3) * 4 + (total % 3)
        core = coded[start:end]
        if len(core) >= 6:
            cores += [core, core.replace("+", "-").replace("/", "_")]
    return cores


def _forms(value: str) -> List[str]:
    """``value`` as it may be written: itself, URL-encoded, and in base64."""
    from urllib.parse import quote, quote_plus

    return [value, quote(value, safe=""), quote(value), quote_plus(value), *_base64_cores(value)]


def mask_text(text: Any, values: Optional[List[str]] = None) -> str:
    """``text`` with secrets masked (``***``), for anything recorded, sent or logged.

    - Every value in ``values`` of 4 characters or more, and its URL-encoded and
      base64 forms, and the value broken across lines or spaces. A shorter value is
      not masked word for word: it would shred the text.
    - The password in ``scheme://user:password@host``.
    - A secret-named parameter in a URL or a JDBC string (``?api_key=x``,
      ``;password=x``, ``&sig=x``).
    - The value of an ``Authorization``, ``Proxy-Authorization`` or ``X-Api-Key``
      header, with or without ``Bearer`` / ``Basic``.

    One implementation for the run record, OpenLineage, the REST connector's logs
    and errors (F-050, F-054).
    """
    out = str(text)
    wanted = sorted(
        {str(v) for v in (values or []) if v is not None and len(str(v)) >= MIN_SECRET_LENGTH},
        key=len,
        reverse=True,
    )
    for value in wanted:
        for form in _forms(value):
            if form and form in out:
                out = out.replace(form, REDACTED)
        if len(value) >= 8 and value not in out:
            # The value broken across a line or a space ("VarSec\nret-123!").
            spread = r"\s*".join(re.escape(c) for c in value)
            out = re.sub(spread, REDACTED, out)
    out = _URL_PASSWORD.sub(lambda m: f"{m.group(1)}:{REDACTED}@", out)
    out = _URL_PARAM.sub(
        lambda m: (
            f"{m.group(1)}{m.group(2)}={REDACTED}" if _secret_param(m.group(2)) else m.group(0)
        ),
        out,
    )
    return _HEADER_VALUE.sub(lambda m: f"{m.group(1)}{m.group(2)}{m.group(3) or ''}{REDACTED}", out)


def _config_secrets(node: Any, parent: str = "") -> List[str]:
    """Literal secret values in a config: under a secret-looking key, a header that
    carries a credential, or an ``auth`` block's ``key`` / ``token`` / ``password``."""
    found: List[str] = []
    if isinstance(node, dict):
        for k, v in node.items():
            key = str(k)
            if isinstance(v, (dict, list)):
                found += _config_secrets(v, key)
            elif isinstance(v, str) and not is_reference(v):
                secret = (
                    looks_secret(key)
                    or key.lower() in _SECRET_HEADERS
                    or (parent.lower() == "auth" and key.lower() in {"key", "token", "password"})
                )
                if secret:
                    found.append(v)
                    found += v.split()[1:]  # "Bearer <token>": the token alone too
    elif isinstance(node, list):
        for v in node:
            found += _config_secrets(v, parent)
    return found


def known_secrets(
    variables: Optional[Dict[str, Any]] = None, config: Optional[Dict[str, Any]] = None
) -> List[str]:
    """Every secret value this process knows of, longest first, for :func:`mask_text`.

    The secret-looking variables (``--var db_password=...``), every value a
    ``secret://`` reference resolved to, the literal secrets in the config, and the
    environment variables with secret-looking names (a config may template one in
    with ``{{ env.DB_PASSWORD }}``).
    """
    import os

    found = set(secret_values(variables)) | set(_SEEN) | set(_config_secrets(config or {}))
    found |= {v for k, v in os.environ.items() if looks_secret(k)}
    return sorted((v for v in found if len(v) >= MIN_SECRET_LENGTH), key=len, reverse=True)


def error_text(
    exc: BaseException,
    variables: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """Why a run failed, safe to record: the error's type and message, secrets masked,
    at most :data:`MAX_ERROR_CHARS` characters and then ``... (N more characters)``.

    Masked first and cut after, so the cut cannot leave half a secret showing. Only
    a head of the text is masked (16 times what is kept): a 5 MB message costs no
    more than a 64 KB one.
    """
    text = f"{type(exc).__name__}: {exc}"
    window = text[: MAX_ERROR_CHARS * 16]
    masked = mask_text(window, known_secrets(variables, config))
    more = len(masked) - MAX_ERROR_CHARS + (len(text) - len(window))
    if more > 0:
        return masked[:MAX_ERROR_CHARS] + f" ... ({more} more characters)"
    return masked


#: How much of a failed run's error the record keeps and OpenLineage sends.
MAX_ERROR_CHARS = 4096

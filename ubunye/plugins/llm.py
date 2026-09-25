"""LLM backends that ship with the engine: three wire formats, standard library only.

Registered in the ``ubunye.llm_backends`` entry-point group; a package adds its
own by subclassing :class:`ubunye.llm.LLMBackend`.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from ubunye.core.errors import LLMError
from ubunye.llm import LLMBackend, LLMRequest, LLMResponse


class AnthropicBackend(LLMBackend):
    """Anthropic's Messages API (``POST /v1/messages``)."""

    KEY_ENV = "ANTHROPIC_API_KEY"
    DEFAULT_BASE_URL = "https://api.anthropic.com"
    PROVIDER = "Anthropic"
    SERVICE = "Anthropic API"
    VERSION = "2023-06-01"

    def build(self, request: LLMRequest) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        body: Dict[str, Any] = {"model": request.model, "messages": request.messages}
        if request.system is not None:
            body["system"] = request.system
        body["max_tokens"] = request.max_tokens
        if request.temperature is not None:
            body["temperature"] = request.temperature
        if request.stop:
            body["stop_sequences"] = list(request.stop)
        headers = {"x-api-key": self.api_key or "", "anthropic-version": self.VERSION}
        return f"{self.base_url}/v1/messages", headers, body

    def parse(self, answer: Dict[str, Any], request: LLMRequest) -> LLMResponse:
        text = "".join(
            part.get("text", "")
            for part in answer.get("content") or []
            if part.get("type") == "text"
        )
        usage = answer.get("usage") or {}
        return LLMResponse(
            text=text,
            model=answer.get("model") or request.model,
            input_tokens=int(usage.get("input_tokens") or 0),
            output_tokens=int(usage.get("output_tokens") or 0),
            stop_reason=answer.get("stop_reason"),
            raw=answer,
        )


class OpenAICompatibleBackend(LLMBackend):
    """Any server with OpenAI's ``POST {base_url}/chat/completions``.

    OpenAI, Azure OpenAI, vLLM, Ollama, LiteLLM and most local servers. The key is
    optional, for servers that need none.
    """

    KEY_ENV = "OPENAI_API_KEY"
    KEY_REQUIRED = False
    DEFAULT_BASE_URL = "https://api.openai.com/v1"

    def provider(self) -> Tuple[Optional[str], Optional[str]]:
        """Told from the server's address; a local or unknown server is its host name."""
        host = urlparse(self.base_url).hostname or ""
        if host == "api.openai.com":
            return "OpenAI", "OpenAI API"
        if host.endswith(".openai.azure.com"):
            return "Microsoft", "Azure OpenAI"
        return (host or None), "OpenAI-compatible API"

    def _body(self, request: LLMRequest) -> Dict[str, Any]:
        messages = list(request.messages)
        if request.system is not None:
            messages = [{"role": "system", "content": request.system}] + messages
        body: Dict[str, Any] = {"model": request.model, "messages": messages}
        body["max_tokens"] = request.max_tokens
        if request.temperature is not None:
            body["temperature"] = request.temperature
        if request.stop:
            body["stop"] = list(request.stop)
        return body

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

    def build(self, request: LLMRequest) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        return f"{self.base_url}/chat/completions", self._headers(), self._body(request)

    def parse(self, answer: Dict[str, Any], request: LLMRequest) -> LLMResponse:
        choices = answer.get("choices") or [{}]
        message = choices[0].get("message") or {}
        usage = answer.get("usage") or {}
        return LLMResponse(
            text=message.get("content") or "",
            model=answer.get("model") or request.model,
            input_tokens=int(usage.get("prompt_tokens") or 0),
            output_tokens=int(usage.get("completion_tokens") or 0),
            stop_reason=choices[0].get("finish_reason"),
            raw=answer,
        )


class DatabricksServingBackend(OpenAICompatibleBackend):
    """A Databricks Model Serving endpoint; ``model`` is the endpoint name.

    The workspace is ``base_url`` or ``DATABRICKS_HOST``; the token is ``api_key``
    or ``DATABRICKS_TOKEN``.
    """

    KEY_ENV = "DATABRICKS_TOKEN"
    KEY_REQUIRED = True
    DEFAULT_BASE_URL = ""

    def provider(self) -> Tuple[Optional[str], Optional[str]]:
        return "Databricks", "Databricks Model Serving"

    def __init__(
        self, *, model: str, api_key: Optional[str] = None, base_url: Optional[str] = None
    ) -> None:
        host = base_url or os.environ.get("DATABRICKS_HOST") or ""
        if host and not host.startswith("http"):
            host = "https://" + host
        super().__init__(model=model, api_key=api_key, base_url=host)

    def build(self, request: LLMRequest) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        if not self.base_url:
            raise LLMError(
                "No Databricks workspace for databricks_serving",
                context={"Endpoint": request.model},
                hint="Set DATABRICKS_HOST, or pass base_url=.",
            )
        body = self._body(request)
        body.pop("model")  # the endpoint is the model
        url = f"{self.base_url}/serving-endpoints/{request.model}/invocations"
        return url, self._headers(), body

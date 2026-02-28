"""
LLM Provider Abstraction Layer.

Supports two backends:
  1. OpenAI API (GPT-4o / configurable model)
  2. Local HuggingFace model via the existing text-completion-llm-service

Usage:
    provider = create_llm_provider()  # reads LLM_PROVIDER env var
    response = provider.generate("Build me an ETL pipeline for HR data", system_prompt="...")
"""

import json
import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger("ai_agent.llm_provider")


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    def generate(self, prompt: str, system_prompt: str = "", temperature: float = 0.3, max_tokens: int = 2048) -> str:
        """Generate text from a prompt."""
        ...

    @abstractmethod
    def name(self) -> str:
        """Return the provider name."""
        ...


class OpenAIProvider(LLMProvider):
    """OpenAI API provider (GPT-4o by default)."""

    def __init__(self, model: str = None, api_key: str = None):
        try:
            import openai
        except ImportError:
            raise ImportError("openai package not installed. Run: pip install openai")

        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required for OpenAI provider")

        self.client = openai.OpenAI(api_key=api_key)
        logger.info(f"OpenAI provider initialized with model: {self.model}")

    def generate(self, prompt: str, system_prompt: str = "", temperature: float = 0.3, max_tokens: int = 2048) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content

    def name(self) -> str:
        return f"OpenAI ({self.model})"


class LocalProvider(LLMProvider):
    """Local HuggingFace provider via the text-completion-llm-service."""

    def __init__(self, service_url: str = None):
        import requests
        self.session = requests.Session()
        self.service_url = service_url or os.getenv(
            "LOCAL_LLM_URL", "http://text-completion-llm-service:5012"
        )
        logger.info(f"Local LLM provider initialized with URL: {self.service_url}")

    def generate(self, prompt: str, system_prompt: str = "", temperature: float = 0.3, max_tokens: int = 2048) -> str:
        """
        Use the local LLM service for text generation.
        Note: This wraps the existing service which is designed for text completion,
        so we adapt it for general-purpose generation.
        """
        import pandas as pd
        import pyarrow as pa

        # Build a single-row DataFrame with the prompt as the text
        full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        df = pd.DataFrame({"text": [f"[GENERATE]{full_prompt}"]})
        table = pa.Table.from_pandas(df)

        # Serialize to IPC
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        ipc_bytes = sink.getvalue().to_pybytes()

        # Call the service
        headers = {
            "Content-Type": "application/vnd.apache.arrow.stream",
            "X-Params": json.dumps({
                "dataset_name": "_agent_request",
                "text_column": "text",
                "max_tokens": max_tokens,
                "missing_placeholder": "[GENERATE]",
                "max_rows": 1,
            })
        }
        resp = self.session.post(
            f"{self.service_url}/text-completion-llm",
            data=ipc_bytes,
            headers=headers,
            timeout=(5, 300),
        )
        resp.raise_for_status()

        # Parse response
        reader = pa.ipc.open_stream(pa.BufferReader(resp.content))
        result_table = reader.read_all()
        result_df = result_table.to_pandas()
        generated_text = result_df["text"].iloc[0]

        # Remove the original prompt prefix
        return generated_text.replace(full_prompt, "").strip()

    def name(self) -> str:
        return f"Local HuggingFace ({self.service_url})"


def create_llm_provider(provider: str = None, **kwargs) -> LLMProvider:
    """
    Factory function to create an LLM provider.

    Args:
        provider: "openai" or "local". Defaults to LLM_PROVIDER env var, then "openai".
        **kwargs: Additional arguments passed to the provider constructor.
    """
    provider = provider or os.getenv("LLM_PROVIDER", "openai")

    if provider == "openai":
        return OpenAIProvider(**kwargs)
    elif provider == "local":
        return LocalProvider(**kwargs)
    else:
        raise ValueError(f"Unknown LLM provider: '{provider}'. Supported: 'openai', 'local'")

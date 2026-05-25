"""
Thin async/sync wrapper around the local Ollama REST API.

Used by bench tasks to produce real AI workloads so the benchmark
reflects genuine CPU + I/O overhead, not just sleep() calls.
"""

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any

from bench.config import EMBED_MODEL, GEN_MODEL, OLLAMA_URL


def _post(endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
    """Synchronous HTTP POST to Ollama (no external deps beyond stdlib)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}{endpoint}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


async def embed(text: str) -> list[float]:
    """Return an embedding vector for *text* using nomic-embed-text."""
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: _post("/api/embeddings", {"model": EMBED_MODEL, "prompt": text}),
    )
    return result["embedding"]


async def generate(prompt: str, max_tokens: int = 60) -> str:
    """Generate text from *prompt* using gemma3:4b."""
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: _post(
            "/api/generate",
            {
                "model": GEN_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens},
            },
        ),
    )
    return result["response"]


def embed_sync(text: str) -> list[float]:
    return _post("/api/embeddings", {"model": EMBED_MODEL, "prompt": text})["embedding"]


def generate_sync(prompt: str, max_tokens: int = 60) -> str:
    return _post(
        "/api/generate",
        {
            "model": GEN_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"num_predict": max_tokens},
        },
    )["response"]


def check_connectivity() -> tuple[bool, str]:
    """Return (ok, message). Called during preflight."""
    try:
        result = _post("/api/embeddings", {"model": EMBED_MODEL, "prompt": "ping"})
        if "embedding" not in result:
            return False, "Ollama returned unexpected response"
        return True, f"nomic-embed-text OK  ({len(result['embedding'])}d vector)"
    except Exception as exc:
        return False, f"Ollama unreachable: {exc}"

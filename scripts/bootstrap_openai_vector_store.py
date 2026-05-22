#!/usr/bin/env python3
"""Create an OpenAI vector store for H.I.R.A semantic memory."""

from __future__ import annotations

import os
import sys

from openai import OpenAI


def main() -> int:
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        print("OPENAI_API_KEY is required to create the vector store.", file=sys.stderr)
        return 2

    name = " ".join(sys.argv[1:]).strip() or "H.I.R.A semantic memory"
    client = OpenAI()
    vector_store = client.vector_stores.create(
        name=name,
        description="Semantic memory, uploaded-file summaries, and source-backed context for H.I.R.A.",
        metadata={"app": "hira", "kind": "semantic_memory"},
    )
    vector_store_id = getattr(vector_store, "id", "")
    if not vector_store_id:
        print("Vector store was created but no id was returned.", file=sys.stderr)
        return 1

    print("Created OpenAI vector store:")
    print(vector_store_id)
    print()
    print("Set these in Railway / local env:")
    print(f"HIRA_OPENAI_MEMORY_VECTOR_STORE_ID={vector_store_id}")
    print(f"HIRA_OPENAI_FILE_SEARCH_VECTOR_STORE_IDS={vector_store_id}")
    print("HIRA_OPENAI_VECTOR_SYNC_ENABLED=1")
    print("HIRA_OPENAI_VECTOR_SYNC_MEMORY=1")
    print("HIRA_OPENAI_VECTOR_SYNC_UPLOADS=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

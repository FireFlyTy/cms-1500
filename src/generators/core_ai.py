import os
import json
import asyncio
from typing import AsyncGenerator
from dotenv import load_dotenv

# --- IMPORTS FOR GOOGLE ---
from google import genai
from google.genai import types

# --- IMPORTS FOR OPENAI ---
from openai import AsyncOpenAI

load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
AI_PROVIDER = os.getenv("AI_PROVIDER", "google").lower()

# Google Config
GOOGLE_API_KEY = os.getenv("GEMINI_API_KEY")
GOOGLE_MODEL_NAME = os.getenv("GOOGLE_MODEL", "gemini-2.5-flash")

# OpenAI Config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-5.1")
REASONING_EFFORT = os.getenv("REASONING_EFFORT", "low")

# Initialize Clients
google_client = None
openai_client = None

if GOOGLE_API_KEY:
    google_client = genai.Client(api_key=GOOGLE_API_KEY)

if OPENAI_API_KEY:
    openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


# ==========================================
# IMPLEMENTATION: GOOGLE GEMINI
# ==========================================
async def _stream_google(prompt: str, thinking_budget: int) -> AsyncGenerator[str, None]:
    print(f"--- [DEBUG] Strategy: GOOGLE ({GOOGLE_MODEL_NAME}) ---")

    if not google_client:
        yield json.dumps({"type": "error", "content": "GOOGLE_API_KEY is missing"}) + "\n\n"
        return

    yield json.dumps({"type": "status", "content": f"Initializing {GOOGLE_MODEL_NAME}..."}) + "\n\n"

    try:
        contents = [types.Content(role='user', parts=[types.Part.from_text(text=prompt)])]

        # Build config - no thinking_config when thinking_budget is None
        if thinking_budget is not None:
            config = types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(
                    include_thoughts=True,
                    thinking_budget=thinking_budget
                ),
                temperature=0.7
            )
        else:
            config = types.GenerateContentConfig(
                temperature=0.7
            )

        response_stream = await google_client.aio.models.generate_content_stream(
            model=GOOGLE_MODEL_NAME,
            contents=contents,
            config=config
        )

        full_response_text = ''
        think_text = ''

        async for chunk in response_stream:
            if not chunk.candidates or not chunk.candidates[0].content or not chunk.candidates[0].content.parts:
                continue

            for part in chunk.candidates[0].content.parts:
                is_thought = False
                if hasattr(part, 'thought') and part.thought:
                    is_thought = True
                    raw_thought = part.text if part.text else str(part.thought)
                    if raw_thought and raw_thought.strip() != "True":
                        think_text += raw_thought
                        yield json.dumps({"type": "thought", "content": raw_thought}) + "\n\n"

                if not is_thought and part.text:
                    full_response_text += part.text
                    yield json.dumps({"type": "content", "content": part.text}) + "\n\n"

        yield json.dumps({"type": "done", "full_text": full_response_text, "think_text": think_text}) + "\n\n"

    except Exception as e:
        error_str = str(e)
        print(f"!!! GOOGLE API ERROR: {error_str}")
        if "Expecting property name" in error_str or "JSONDecodeError" in error_str:
            yield json.dumps({"type": "error",
                              "content": f"Google API overloaded (HTML response). Try switching to OpenAI in .env"}) + "\n\n"
        else:
            yield json.dumps({"type": "error", "content": error_str}) + "\n\n"


# ==========================================
# IMPLEMENTATION: OPENAI
# ==========================================
async def _stream_openai(prompt: str) -> AsyncGenerator[str, None]:
    print(f"--- [DEBUG] Strategy: OPENAI ({OPENAI_MODEL_NAME}, Effort: {REASONING_EFFORT}) ---")

    if not openai_client:
        yield json.dumps({"type": "error", "content": "OPENAI_API_KEY is missing"}) + "\n\n"
        return

    yield json.dumps({"type": "status", "content": f"Initializing {OPENAI_MODEL_NAME}..."}) + "\n\n"

    try:
        # Build request params
        params = {
            "model": OPENAI_MODEL_NAME,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True
        }

        # Add reasoning_effort only for models that support it
        if OPENAI_MODEL_NAME in ["gpt-5.1", "o1", "o1-mini", "o3", "o3-mini"]:
            params["reasoning_effort"] = REASONING_EFFORT

        stream = await openai_client.chat.completions.create(**params)

        full_response_text = ''

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                content = chunk.choices[0].delta.content
                full_response_text += content
                yield json.dumps({"type": "content", "content": content}) + "\n\n"

        yield json.dumps({"type": "done", "full_text": full_response_text, "think_text": ""}) + "\n\n"

    except Exception as e:
        print(f"!!! OPENAI API ERROR: {str(e)}")
        yield json.dumps({"type": "error", "content": f"OpenAI Error: {str(e)}"}) + "\n\n"


async def call_openai_model(
    prompt: str,
    model: str = "gpt-4.1",
    reasoning_effort: str = None,
    stream: bool = False
) -> str:
    """
    Call a specific OpenAI model with optional reasoning.

    Args:
        prompt: The prompt to send
        model: Model name (gpt-5.1, gpt-4.1, etc.)
        reasoning_effort: For reasoning models - "low", "medium", "high"
        stream: Whether to stream (not implemented yet, returns full response)

    Returns:
        The model's response text
    """
    if not openai_client:
        raise ValueError("OPENAI_API_KEY is missing")

    print(f"--- [DEBUG] OpenAI call: {model}, reasoning: {reasoning_effort} ---")

    try:
        # Build request params
        params = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
        }

        # Add reasoning_effort only for models that support it (gpt-5.1, o1, o3, etc.)
        if reasoning_effort and model in ["gpt-5.1", "o1", "o1-mini", "o3", "o3-mini"]:
            params["reasoning_effort"] = reasoning_effort

        response = await openai_client.chat.completions.create(**params)

        return response.choices[0].message.content

    except Exception as e:
        print(f"!!! OPENAI API ERROR ({model}): {str(e)}")
        raise


async def stream_openai_model(
    prompt: str,
    model: str = "gpt-4.1",
    reasoning_effort: str = None
) -> AsyncGenerator[str, None]:
    """
    Stream response from a specific OpenAI model.

    Args:
        prompt: The prompt to send
        model: Model name (gpt-5.1, gpt-4.1, etc.)
        reasoning_effort: For reasoning models - "low", "medium", "high"

    Yields:
        JSON chunks with type and content
    """
    if not openai_client:
        yield json.dumps({"type": "error", "content": "OPENAI_API_KEY is missing"}) + "\n\n"
        return

    yield json.dumps({"type": "status", "content": f"Calling {model}..."}) + "\n\n"

    try:
        # Build request params
        params = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True
        }

        # Add reasoning_effort only for models that support it
        if reasoning_effort and model in ["gpt-5.1", "o1", "o1-mini", "o3", "o3-mini"]:
            params["reasoning_effort"] = reasoning_effort

        stream = await openai_client.chat.completions.create(**params)

        full_response_text = ''

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                content = chunk.choices[0].delta.content
                full_response_text += content
                yield json.dumps({"type": "content", "content": content}) + "\n\n"

        yield json.dumps({"type": "done", "full_text": full_response_text}) + "\n\n"

    except Exception as e:
        print(f"!!! OPENAI API ERROR ({model}): {str(e)}")
        yield json.dumps({"type": "error", "content": f"OpenAI Error: {str(e)}"}) + "\n\n"


async def call_gemini_model(
    prompt: str,
    model: str = None,
    thinking_budget: int = None
) -> str:
    """
    Call Gemini model and return full response (non-streaming).

    Args:
        prompt: The prompt to send
        model: Model name (defaults to GOOGLE_MODEL_NAME)
        thinking_budget: Optional thinking budget

    Returns:
        The model's response text
    """
    if not google_client:
        raise ValueError("GOOGLE_API_KEY is missing")

    model = model or GOOGLE_MODEL_NAME
    print(f"--- [DEBUG] Gemini call: {model}, thinking: {thinking_budget} ---")

    try:
        contents = [types.Content(role='user', parts=[types.Part.from_text(text=prompt)])]

        # Build config
        if thinking_budget is not None:
            config = types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(
                    include_thoughts=False,
                    thinking_budget=thinking_budget
                ),
                temperature=0.7
            )
        else:
            config = types.GenerateContentConfig(temperature=0.7)

        response = await google_client.aio.models.generate_content(
            model=model,
            contents=contents,
            config=config
        )

        # Extract text from response
        if response.candidates and response.candidates[0].content:
            parts = response.candidates[0].content.parts
            return ''.join(p.text for p in parts if p.text and not getattr(p, 'thought', False))

        return ""

    except Exception as e:
        print(f"!!! GEMINI API ERROR ({model}): {str(e)}")
        raise


# ==========================================
# MAIN ROUTER (Public Function)
# ==========================================
async def stream_gemini_generator(prompt: str, thinking_budget=2048) -> AsyncGenerator[str, None]:
    if AI_PROVIDER == 'openai':
        async for chunk in _stream_openai(prompt):
            yield chunk
    else:
        async for chunk in _stream_google(prompt, thinking_budget):
            yield chunk
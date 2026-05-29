const API_BASE = process.env.API_BASE || "https://api.getnadir.com/v1";

/**
 * Get API keys from environment variable or fallback to hardcoded keys.
 * Environment variable format: API_KEYS=key1,key2,key3
 */
function getApiKeys() {
  if (process.env.API_KEYS) {
    return process.env.API_KEYS.split(",")
      .map((k) => k.trim())
      .filter(Boolean);
  }
  // Fallback keys — works without environment variables
  return [
    "ndr_4e8895f9d1ff46f9951ed9077cc99be4",
  ];
}

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

/**
 * Sanitize messages — convert multi-part content (images, etc.) to plain text
 * since the upstream API may not support all content types.
 */
function sanitizeMessages(messages) {
  return messages.map((msg) => {
    let content = msg.content;
    if (Array.isArray(content)) {
      const textParts = [];
      for (const part of content) {
        if (part.type === "text") textParts.push(part.text);
        else if (part.type === "image_url")
          textParts.push("[User attached an image]");
      }
      content = textParts.join("\n");
    }
    return { role: msg.role, content };
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Helper to create an SSE error response.
 */
function sseErrorResponse(errorMessage) {
  const data = `data: ${JSON.stringify({ error: errorMessage })}\n\ndata: [DONE]\n\n`;
  return new Response(data, {
    status: 200,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
    },
  });
}

/**
 * Try each API key in sequence. Retry on auth/rate-limit errors (401, 403, 429)
 * and on timeouts. Stop on other errors.
 */
async function callApiWithRetry(payload, keys) {
  let lastError = null;

  for (let i = 0; i < keys.length; i++) {
    const apiKey = keys[i];
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 25000);

      const resp = await fetch(`${API_BASE}/chat/completions`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (resp.ok) {
        return { response: resp };
      }

      let errorText = "";
      try {
        errorText = await resp.text();
      } catch (_) {}
      lastError = `API error ${resp.status}: ${errorText}`;

      // Retry on auth or rate-limit errors
      if (resp.status === 401 || resp.status === 403 || resp.status === 429) {
        continue;
      }

      // Don't retry on other errors (400, 500, etc.)
      break;
    } catch (err) {
      lastError =
        err.name === "AbortError"
          ? "API request timed out after 25s"
          : err.message;
      continue;
    }
  }

  return { error: lastError || "All API keys failed" };
}

export const config = {
  runtime: "edge",
};

export default async function handler(req) {
  // Handle CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  try {
    const body = await req.json();
    const messages = body.messages || [];
    const model = body.model;

    if (!messages.length) {
      return new Response(
        JSON.stringify({ error: "No messages provided" }),
        {
          status: 400,
          headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
        }
      );
    }

    const keys = getApiKeys();
    if (!keys.length) {
      return sseErrorResponse(
        "No API keys configured. Add API_KEYS in Vercel Environment Variables."
      );
    }

    const cleanMessages = sanitizeMessages(messages);
    const payload = { messages: cleanMessages, stream: false };
    if (model) payload.model = model;

    const result = await callApiWithRetry(payload, keys);

    if (result.error) {
      return sseErrorResponse(result.error);
    }

    let data;
    try {
      data = await result.response.json();
    } catch (e) {
      return sseErrorResponse("Failed to parse API response");
    }

    const content = data.choices?.[0]?.message?.content || "";
    const usedModel = data.model || "claude";

    if (!content) {
      return sseErrorResponse("API returned empty response. Please try again.");
    }

    // Simulate SSE streaming by chunking the complete response
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();

    (async () => {
      try {
        const chars = [...content];
        const chunkSize = 4;
        const delayMs = 10;

        for (let i = 0; i < chars.length; i += chunkSize) {
          const chunk = chars.slice(i, i + chunkSize).join("");
          const sseChunk = {
            choices: [
              { delta: { content: chunk }, finish_reason: null },
            ],
            model: usedModel,
          };
          await writer.write(
            encoder.encode(`data: ${JSON.stringify(sseChunk)}\n\n`)
          );
          if (i + chunkSize < chars.length) {
            await delay(delayMs);
          }
        }

        await writer.write(encoder.encode("data: [DONE]\n\n"));
      } catch (e) {
        const errChunk = `data: ${JSON.stringify({ error: e.message })}\n\ndata: [DONE]\n\n`;
        await writer.write(encoder.encode(errChunk));
      } finally {
        await writer.close();
      }
    })();

    return new Response(readable, {
      status: 200,
      headers: {
        ...CORS_HEADERS,
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
      },
    });
  } catch (e) {
    return sseErrorResponse(e.message || "Internal server error");
  }
}

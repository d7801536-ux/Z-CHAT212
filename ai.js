const BOT_CONFIG = {
  coding: {
    label: "Coding AI",
    avatar: "Code",
    starter: "Describe a coding problem, paste code, or ask for debugging help."
  },
  gaming: {
    label: "Gaming AI",
    avatar: "Game",
    starter: "Ask for strategies, squad ideas, or game recommendations."
  },
  study: {
    label: "Study AI",
    avatar: "Study",
    starter: "Ask for a concept explanation, study plan, or quiz support."
  },
  assistant: {
    label: "General Assistant",
    avatar: "Assist",
    starter: "Ask anything practical, from planning to summarizing."
  }
};

function createTimeoutSignal(timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    clear() {
      clearTimeout(timer);
    }
  };
}

async function safeJsonFetch(url, options = {}, fallbackMessage = "Request failed.") {
  const timeout = createTimeoutSignal(options.timeoutMs || 8000);
  try {
    const response = await fetch(url, {
      ...options,
      signal: timeout.signal
    });
    const contentType = response.headers.get("content-type") || "";
    const isJson = contentType.includes("application/json");
    const payload = isJson ? await response.json().catch(() => ({})) : null;

    if (!response.ok) {
      throw new Error(payload?.message || fallbackMessage);
    }

    if (!isJson) {
      throw new Error("Received an unexpected response format.");
    }

    return payload;
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("The request timed out.");
    }
    throw error;
  } finally {
    timeout.clear();
  }
}

function resolveAiBackendBase(explicitBase = "") {
  if (explicitBase) {
    return explicitBase.replace(/\/$/, "");
  }

  const runtimeBase =
    typeof window !== "undefined" && typeof window.__ZCHAT_API_BASE__ === "string"
      ? window.__ZCHAT_API_BASE__.trim()
      : "";

  if (runtimeBase) {
    return runtimeBase.replace(/\/$/, "");
  }

  const host = typeof window !== "undefined" ? window.location.hostname : "";
  if (!host || host === "localhost" || host === "127.0.0.1") {
    return "";
  }

  if (/\.github\.io$/i.test(host)) {
    throw new Error("Z AI needs a deployed backend API on static hosting. Set window.__ZCHAT_API_BASE__ to your server URL.");
  }

  return "";
}

export function getBotConfig(botId) {
  return BOT_CONFIG[botId] || BOT_CONFIG.assistant;
}

export async function scanClaimWithWikipedia(claimText) {
  const query = String(claimText || "").trim();
  if (!query) {
    throw new Error("Enter a claim before scanning.");
  }

  const searchUrl = new URL("https://en.wikipedia.org/w/api.php");
  searchUrl.searchParams.set("action", "query");
  searchUrl.searchParams.set("list", "search");
  searchUrl.searchParams.set("srsearch", query);
  searchUrl.searchParams.set("format", "json");
  searchUrl.searchParams.set("origin", "*");
  searchUrl.searchParams.set("srlimit", "3");

  const searchJson = await safeJsonFetch(searchUrl, {}, "Public source search failed.");
  const hits = searchJson?.query?.search || [];
  if (!hits.length) {
    return {
      verdict: "unverified",
      label: "Unverified",
      summary: "No matching public sources were found on Wikipedia for this claim.",
      sources: []
    };
  }

  const sourceIds = hits.map((entry) => entry.pageid).join("|");
  const detailUrl = new URL("https://en.wikipedia.org/w/api.php");
  detailUrl.searchParams.set("action", "query");
  detailUrl.searchParams.set("prop", "extracts|info");
  detailUrl.searchParams.set("pageids", sourceIds);
  detailUrl.searchParams.set("inprop", "url");
  detailUrl.searchParams.set("exintro", "true");
  detailUrl.searchParams.set("explaintext", "true");
  detailUrl.searchParams.set("format", "json");
  detailUrl.searchParams.set("origin", "*");

  const detailJson = await safeJsonFetch(detailUrl, {}, "Source details could not be loaded.");
  const pages = Object.values(detailJson?.query?.pages || {});
  const sources = pages
    .filter((page) => page?.extract)
    .map((page) => ({
      title: page.title,
      url: page.fullurl,
      extract: String(page.extract).slice(0, 320)
    }));

  const queryTokens = query.toLowerCase().split(/\s+/).filter((token) => token.length > 3);
  const topExtract = (sources[0]?.extract || "").toLowerCase();
  const matches = queryTokens.filter((token) => topExtract.includes(token)).length;
  const ratio = queryTokens.length ? matches / queryTokens.length : 0;

  let verdict = "unverified";
  let label = "Unverified";
  let summary = "The sources are related, but the claim still needs human review.";

  if (ratio >= 0.55) {
    verdict = "true";
    label = "Likely True";
    summary = "Public reference material appears strongly related to the claim.";
  } else if (ratio >= 0.25) {
    verdict = "false";
    label = "Possibly False";
    summary = "The claim only partially matches public reference material and may be misleading.";
  }

  return {
    verdict,
    label,
    summary,
    sources
  };
}

export async function requestAiReply({ botId, prompt, authToken, backendBase = "" }) {
  const cleanPrompt = String(prompt || "").trim();
  if (!cleanPrompt) {
    throw new Error("Enter a prompt before sending it to Z AI.");
  }

  const base = resolveAiBackendBase(backendBase);
  const payload = await safeJsonFetch(`${base}/api/ai/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(authToken ? { Authorization: `Bearer ${authToken}` } : {})
    },
    body: JSON.stringify({
      bot: botId,
      prompt: cleanPrompt
    }),
    timeoutMs: 10000
  }, "AI service is unavailable right now.");

  return {
    reply: payload.reply || "No reply received.",
    plan: payload.plan || null,
    usageToday: payload.usageToday ?? null,
    limit: payload.limit ?? null,
    usage: payload.usage || null,
    provider: payload.provider || "configured-backend"
  };
}

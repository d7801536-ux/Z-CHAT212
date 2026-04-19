const PLAN_MAP = {
  moderate: {
    code: "49",
    label: "Moderate Plan",
    amount: 49
  },
  premium: {
    code: "99",
    label: "Premium Plan",
    amount: 99
  }
};

export function getPlanMeta(planId) {
  return PLAN_MAP[planId] || null;
}

async function parseJsonSafe(response) {
  try {
    return await response.json();
  } catch (_error) {
    return {};
  }
}

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

async function requestJson(url, options = {}, fallbackMessage = "Request failed.") {
  const timeout = createTimeoutSignal(options.timeoutMs || 8000);
  try {
    const response = await fetch(url, {
      ...options,
      signal: timeout.signal
    });
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("application/json")
      ? await parseJsonSafe(response)
      : {};

    if (!response.ok) {
      throw new Error(payload?.message || fallbackMessage);
    }

    if (!contentType.includes("application/json")) {
      throw new Error("Received an unexpected non-JSON response.");
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

function resolveBackendBase(explicitBase = "") {
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
    throw new Error("Payments need a deployed backend API. Set window.__ZCHAT_API_BASE__ to your server URL.");
  }

  return "";
}

export async function fetchPaymentHistory(authToken, backendBase = "") {
  const base = resolveBackendBase(backendBase);
  const payload = await requestJson(`${base}/api/payments/history`, {
    headers: authToken ? { Authorization: `Bearer ${authToken}` } : {}
  }, "Payment history could not be loaded.");
  return payload.items || [];
}

export async function createPaymentOrder({ planId, authToken, backendBase = "" }) {
  const meta = getPlanMeta(planId);
  if (!meta) {
    throw new Error("Unsupported plan selected.");
  }

  const base = resolveBackendBase(backendBase);
  const payload = await requestJson(`${base}/api/payments/create-order`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(authToken ? { Authorization: `Bearer ${authToken}` } : {})
    },
    body: JSON.stringify({ plan: meta.code }),
    timeoutMs: 10000
  }, "Order could not be created.");

  return {
    ...payload,
    planMeta: meta
  };
}

export async function verifyPayment({
  authToken,
  backendBase = "",
  paymentPayload,
  planId
}) {
  const meta = getPlanMeta(planId);
  const base = resolveBackendBase(backendBase);
  const payload = await requestJson(`${base}/api/payments/verify`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(authToken ? { Authorization: `Bearer ${authToken}` } : {})
    },
    body: JSON.stringify({
      ...paymentPayload,
      plan: meta?.code || ""
    }),
    timeoutMs: 10000
  }, "Payment verification failed.");
  return payload;
}

export async function openRazorpayCheckout({
  planId,
  order,
  user,
  authToken,
  backendBase = "",
  onVerified
}) {
  if (!window.Razorpay) {
    throw new Error("Razorpay checkout is not loaded. Add the checkout script and try again.");
  }

  return new Promise((resolve, reject) => {
    const instance = new window.Razorpay({
      key: order.keyId,
      amount: order.amount,
      currency: order.currency,
      name: "Z Chat",
      description: order.planMeta?.label || "Z Chat subscription",
      order_id: order.orderId,
      prefill: {
        name: user?.displayName || user?.fullName || "",
        email: user?.email || "",
        contact: user?.phone || ""
      },
      theme: {
        color: "#33d6ff"
      },
      handler: async (response) => {
        try {
          const verification = await verifyPayment({
            authToken,
            backendBase: resolveBackendBase(backendBase),
            paymentPayload: response,
            planId
          });
          if (typeof onVerified === "function") {
            await onVerified(verification);
          }
          resolve(verification);
        } catch (error) {
          reject(error);
        }
      },
      modal: {
        ondismiss: () => reject(new Error("Payment checkout was closed."))
      }
    });

    instance.on("payment.failed", (event) => {
      reject(new Error(event?.error?.description || "Payment failed."));
    });

    instance.open();
  });
}

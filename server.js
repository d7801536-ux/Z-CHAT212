import path from "path";
import { fileURLToPath } from "url";
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import paymentRoutes, { handlePaymentWebhook } from "./routes/payment.routes.js";
import aiRoutes from "./routes/ai.routes.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = __dirname;
const port = Number(process.env.PORT || 3000);

const app = express();
app.disable("x-powered-by");
app.use(cors({
  origin: true,
  credentials: true,
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"]
}));
app.options("*", cors());

app.get("/health", (_req, res) => {
  res.json({ ok: true, app: "Z Chat" });
});

app.post("/api/payments/webhook", express.raw({ type: "application/json" }), handlePaymentWebhook);
app.post("/razorpay-webhook", express.raw({ type: "application/json" }), handlePaymentWebhook);

app.use(express.json({ limit: "2mb" }));
app.use("/api/payments", paymentRoutes);
app.use("/api/ai", aiRoutes);

app.use(express.static(root, {
  extensions: ["html"],
  setHeaders(res, filePath) {
    if (filePath.endsWith(".js")) {
      res.setHeader("Content-Type", "application/javascript; charset=utf-8");
    }
    if (filePath.endsWith(".json") && filePath.toLowerCase().endsWith("manifest.json")) {
      res.setHeader("Content-Type", "application/manifest+json; charset=utf-8");
    }
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-store");
    }
  }
}));

app.get("*", (_req, res) => {
  res.sendFile(path.join(root, "index.html"));
});

app.listen(port, () => {
  console.log(`Z Chat running at http://localhost:${port}`);
  console.log("Open the app through localhost, not file://");
});

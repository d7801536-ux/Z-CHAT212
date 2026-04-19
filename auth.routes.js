const express = require("express");

module.exports = function authRoutes(state) {
    const router = express.Router();

    router.post("/register", (req, res) => {
        if (state.users.some((user) => user.email.toLowerCase() === String(req.body.email).toLowerCase())) {
            return res.status(409).json({ ok: false, error: "Email already exists" });
        }
        if (state.users.some((user) => user.username === req.body.username)) {
            return res.status(409).json({ ok: false, error: "Username already taken" });
        }
        state.otpStore[req.body.phone] = "123456";
        return res.status(201).json({ ok: true, otpSent: true, otp: "123456" });
    });

    router.post("/login", (req, res) => {
        const user = state.users.find((item) => item.email.toLowerCase() === String(req.body.email).toLowerCase());
        if (!user) {
            return res.status(404).json({ ok: false, error: "No account found with this email" });
        }
        if (user.password !== req.body.password) {
            return res.status(401).json({ ok: false, error: "Wrong password" });
        }
        return res.json({ ok: true, token: `token-${Date.now()}`, user });
    });

    router.post("/logout", (_req, res) => res.json({ ok: true }));
    router.post("/refresh-token", (_req, res) => res.json({ ok: true, token: `token-${Date.now()}` }));

    router.post("/forgot-password", (req, res) => {
        const user = state.users.find((item) => item.email.toLowerCase() === String(req.body.email).toLowerCase());
        if (!user) {
            return res.status(404).json({ ok: false, error: "No account found with this email" });
        }
        state.resetTokens[user.email] = `reset-${Date.now()}`;
        return res.json({ ok: true, sent: true });
    });

    router.post("/reset-password", (req, res) => {
        const user = state.users.find((item) => item.email.toLowerCase() === String(req.body.email).toLowerCase());
        if (!user) {
            return res.status(404).json({ ok: false, error: "No account found with this email" });
        }
        user.password = req.body.password;
        return res.json({ ok: true });
    });

    router.post("/send-otp", (req, res) => {
        state.otpStore[req.body.phone] = "123456";
        return res.json({ ok: true, otp: "123456" });
    });

    router.post("/verify-otp", (req, res) => {
        const otp = state.otpStore[req.body.phone];
        if (otp !== req.body.otp) {
            return res.status(400).json({ ok: false, error: "Incorrect OTP" });
        }
        return res.json({ ok: true });
    });

    router.get("/check-username/:username", (req, res) => {
        const exists = state.users.some((user) => user.username === req.params.username);
        res.json({ ok: true, available: !exists });
    });

    router.get("/check-email/:email", (req, res) => {
        const exists = state.users.some((user) => user.email.toLowerCase() === req.params.email.toLowerCase());
        res.json({ ok: true, available: !exists });
    });

    return router;
};

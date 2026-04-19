# Z Chat

Z Chat is a responsive realtime communication platform built with:

- HTML
- CSS
- Vanilla JavaScript
- Firebase Authentication
- Firestore
- Firebase Storage
- WebRTC
- Node.js + Express
- Razorpay

## Current Product Surface

- Email/password auth
- Google sign-in
- Profile editing with username, bio, theme, and photo upload
- Realtime direct chat
- Image messages
- Friend requests and friend list
- Group, gaming, and study rooms
- Voice calls, video calls, and screen sharing with Firestore signaling
- Z Scanner using Wikipedia public search
- Gemini-backed AI route integration point
- Daily challenges and streak-oriented dashboard widgets
- Mini games
- Razorpay order creation, verification, webhook handling, and payment history
- Direct UPI QR support card for manual transfers that does not auto-unlock plans

## Local Run

1. Copy `.env.example` to `.env`
2. Fill in the backend values:
   - `FIREBASE_SERVICE_ACCOUNT_JSON`
   - `FIREBASE_PROJECT_ID`
   - `RAZORPAY_KEY_ID`
   - `RAZORPAY_KEY_SECRET`
   - `RAZORPAY_WEBHOOK_SECRET`
   - `GEMINI_API_KEY`
3. Use Node.js 18 or newer. The Express backend uses native `fetch` for Gemini requests.
4. Install dependencies:

```bash
npm install
```

5. Start the app:

```bash
npm start
```

6. Open:

```text
http://localhost:3000
```

Do not run the app with `file://`.

## Frontend Firebase Config

The browser app uses [firebase.js](./firebase.js) with the modular CDN SDK.

It supports either:

1. The default config currently committed in the file, or
2. A runtime override through:

```html
<script>
  window.__ZCHAT_FIREBASE_CONFIG__ = {
    apiKey: "YOUR_API_KEY",
    authDomain: "YOUR_PROJECT.firebaseapp.com",
    projectId: "YOUR_PROJECT_ID",
    storageBucket: "YOUR_PROJECT.firebasestorage.app",
    messagingSenderId: "YOUR_MESSAGING_SENDER_ID",
    appId: "YOUR_APP_ID"
  };
</script>
```

If you use the override approach, place that script before loading [script.js](./script.js).

For hosted frontends, you can also point payments and AI to a deployed backend:

```html
<script>
  window.__ZCHAT_API_BASE__ = "https://your-backend.example.com";
</script>
```

If `window.__ZCHAT_API_BASE__` is empty on a static host like GitHub Pages, payment requests will fail safely with a clear configuration error instead of a broken checkout flow.

## Firebase Console Setup

Enable:

- Authentication
  - Email/Password
  - Google
- Firestore
- Storage

Publish:

- `firestore.rules`
- `storage.rules`

Add these auth domains:

- `localhost`
- `127.0.0.1`

Recommended collections used by the app:

- `users/{userId}`
- `usernames/{username}`
- `conversations/{conversationId}`
- `conversations/{conversationId}/messages/{messageId}`
- `calls/{callId}`
- `calls/{callId}/offerCandidates/{candidateId}`
- `calls/{callId}/answerCandidates/{candidateId}`
- `friendships/{friendshipId}`
- `rooms/{roomId}`
- `rooms/{roomId}/messages/{messageId}`
- `userDailyChallenges/{challengeId}`

## Payments

Frontend uses Razorpay Checkout.

Backend endpoints:

- `POST /api/payments/create-order`
- `POST /api/payments/verify`
- `GET /api/payments/history`
- `POST /api/payments/webhook`

The premium screen also includes a direct UPI QR and UPI ID for manual transfers, but plan unlocks are still gated behind verified Razorpay checkout only.

For GitHub Pages or any static-only frontend deployment, you must deploy the Express backend separately and set `window.__ZCHAT_API_BASE__` to that backend origin.

## AI

Backend endpoint:

- `POST /api/ai/chat`

If `GEMINI_API_KEY` is missing, the AI panel will show a real configuration error instead of a fake success state.

## Browser Notes

- `setSinkId()` support depends on the browser
- screen share requires a supported Chromium or modern desktop browser
- camera/mic require localhost or HTTPS
- WebRTC calling requires two signed-in users in the same Firebase project
- Test voice/video/screen share in current Chromium, Edge, or Firefox
- For deployed environments, use HTTPS or camera/mic/screen share will fail

## Files

- `index.html`
- `style.css`
- `script.js`
- `firebase.js`
- `ai.js`
- `payment.js`
- `games.js`
- `server.js`
- `routes/payment.routes.js`
- `routes/ai.routes.js`
- `firestore.rules`
- `storage.rules`

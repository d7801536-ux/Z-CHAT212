import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js";
import {
  getAuth,
  GoogleAuthProvider,
  browserLocalPersistence,
  setPersistence
} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js";
import { getFirestore } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-firestore.js";
import { getStorage } from "https://www.gstatic.com/firebasejs/10.12.5/firebase-storage.js";

const DEFAULT_FIREBASE_CONFIG = {
  apiKey: "AIzaSyAhZXEXJmLaxcnz7nTKuwhDhSuYQSKtBQs",
  authDomain: "z-chat11.firebaseapp.com",
  projectId: "z-chat11",
  storageBucket: "z-chat11.firebasestorage.app",
  messagingSenderId: "373379915510",
  appId: "1:373379915510:web:8b4b4502f6facb40c6d6d7"
};

function getRuntimeFirebaseConfig() {
  const runtimeConfig =
    typeof globalThis !== "undefined" && globalThis.__ZCHAT_FIREBASE_CONFIG__
      ? globalThis.__ZCHAT_FIREBASE_CONFIG__
      : null;

  if (!runtimeConfig || typeof runtimeConfig !== "object") {
    return DEFAULT_FIREBASE_CONFIG;
  }

  return {
    ...DEFAULT_FIREBASE_CONFIG,
    ...runtimeConfig
  };
}

function validateFirebaseConfig(config) {
  const requiredKeys = ["apiKey", "authDomain", "projectId", "storageBucket", "messagingSenderId", "appId"];
  const missing = requiredKeys.filter((key) => !config[key] || String(config[key]).includes("YOUR_"));
  if (missing.length && typeof console !== "undefined" && console.warn) {
    console.warn(`[Z Chat] Firebase config is missing: ${missing.join(", ")}.`);
  }
}

export const firebaseConfig = getRuntimeFirebaseConfig();
validateFirebaseConfig(firebaseConfig);

let firebaseApp = null;
let auth = null;
let db = null;
let storage = null;
let firebaseInitError = null;

try {
  firebaseApp = initializeApp(firebaseConfig);
  auth = getAuth(firebaseApp);
  db = getFirestore(firebaseApp);
  storage = getStorage(firebaseApp);
  console.info("[Z Chat] Firebase initialized");
} catch (error) {
  firebaseInitError = error;
  console.error("[Z Chat] Firebase initialization failed", error);
}

export { firebaseApp, auth, db, storage, firebaseInitError };
export const googleProvider = new GoogleAuthProvider();

googleProvider.setCustomParameters({
  prompt: "select_account"
});

if (auth) {
  setPersistence(auth, browserLocalPersistence).catch((error) => {
    console.warn("[Z Chat] Firebase persistence setup failed", error);
  });
}

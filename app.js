import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
    browserLocalPersistence,
    browserSessionPersistence,
    createUserWithEmailAndPassword,
    getAuth,
    onAuthStateChanged,
    sendEmailVerification,
    setPersistence,
    signInWithEmailAndPassword,
    signOut,
    updateProfile
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import {
    addDoc,
    collection,
    doc,
    getFirestore,
    getDoc,
    onSnapshot,
    orderBy,
    query,
    serverTimestamp,
    setDoc,
    updateDoc,
    where,
    getDocs,
    limit
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";

const firebaseConfig = {
    apiKey: "AIzaSyAhZXEXJmLaxcnz7nTKuwhDhSuYQSKtBQs",
    authDomain: "z-chat11.firebaseapp.com",
    databaseURL: "https://z-chat11-default-rtdb.asia-southeast1.firebasedatabase.app",
    projectId: "z-chat11",
    storageBucket: "z-chat11.firebasestorage.app",
    messagingSenderId: "373379915510",
    appId: "1:373379915510:web:8b4b4502f6facb40c6d6d7"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const paymentApiBase = window.location.hostname === "localhost" ? "http://localhost:3001" : "http://localhost:3001";

const rtcConfig = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" }
    ]
};

const profiles = [
    { id: "pilot", name: "Pilot Zero", avatar: "PZ", tag: "Product Lead" },
    { id: "alya", name: "Alya", avatar: "AL", tag: "AI Coder" },
    { id: "nova", name: "Nova Coach", avatar: "NC", tag: "AI Gamer" },
    { id: "rhea", name: "Rhea", avatar: "RH", tag: "AI Study" },
    { id: "kai", name: "Kai", avatar: "KA", tag: "Night Squad" },
    { id: "mina", name: "Mina", avatar: "MI", tag: "Pixel Raiders" }
];

const sections = [
    { id: "chats", label: "Chats", icon: "CH" },
    { id: "friends", label: "Friends", icon: "FR" },
    { id: "groups", label: "Groups", icon: "GR" },
    { id: "bots", label: "AI Bots", icon: "AI" },
    { id: "gaming", label: "Gaming", icon: "GM" },
    { id: "study", label: "Study", icon: "SD" },
    { id: "settings", label: "Settings", icon: "ST" }
];

const aiModes = {
    coder: {
        title: "AI Coder",
        description: "Builds bug triage notes, responsive layout fixes, and code-review ready summaries.",
        actions: ["Refactor layout", "Explain bug", "Generate theme"]
    },
    gamer: {
        title: "AI Gamer",
        description: "Creates match callouts, round plans, and quick squad recap prompts.",
        actions: ["Build strategy", "Draft recap", "Clip highlights"]
    },
    study: {
        title: "AI Study",
        description: "Turns conversations into flashcards, outlines, and short revision plans.",
        actions: ["Make flashcards", "Summarize notes", "Plan revision"]
    }
};

const conversationSeed = {
    alya: [
        { author: "Alya", avatar: "AL", text: "Realtime call layer is mapped and ready for signaling.", time: "10:34", self: false },
        { author: "You", avatar: "ME", text: "Perfect. Keep mobile full-screen and desktop clean.", time: "10:36", self: true }
    ],
    nova: [
        { author: "Nova Coach", avatar: "NC", text: "Want a voice room before the ranked run?", time: "09:15", self: false }
    ],
    rhea: [
        { author: "Rhea", avatar: "RH", text: "I can review your notes over video later tonight.", time: "07:58", self: false }
    ],
    kai: [
        { author: "Kai", avatar: "KA", text: "Open a quick voice call when you are free.", time: "08:45", self: false }
    ],
    mina: [
        { author: "Mina", avatar: "MI", text: "Screen share the bracket once you join.", time: "Yesterday", self: false }
    ]
};

const notifications = [
    { id: 1, title: "Calling tips", body: "Open this app in two browsers with different profiles to test calls.", unread: true },
    { id: 2, title: "Permissions", body: "Grant microphone and camera access for voice and video calling.", unread: true },
    { id: 3, title: "Bluetooth", body: "Pair a headset and refresh devices to route output and mic input.", unread: true }
];

const storageKeys = {
    profileId: "zchat_profile_id",
    registerDraft: "zchat_register_draft",
    queuedActions: "zchat_queued_actions"
};

const state = {
    authUser: null,
    authReady: false,
    userProfile: null,
    activeProfileId: localStorage.getItem(storageKeys.profileId) || "pilot",
    activeSection: "chats",
    activeConversationId: "alya",
    activeAiTab: "coder",
    drawerOpen: false,
    notificationsOpen: false,
    scannerOpen: false,
    searchTerm: "",
    mobile: window.innerWidth <= 768,
    profilesPresence: {},
    conversations: {},
    localDevices: {
        audioInputs: [],
        audioOutputs: [],
        videoInputs: []
    },
    selectedAudioInputId: "",
    selectedAudioOutputId: "default",
    selectedVideoInputId: "",
    currentCall: null,
    incomingCall: null,
    peerConnection: null,
    localStream: null,
    remoteStream: null,
    screenStream: null,
    remoteCandidatesUnsub: null,
    localCandidatesCollection: null,
    callDocUnsub: null,
    callsListUnsub: null,
    presenceUnsub: null,
    durationTimer: null,
    speakerEnabled: true,
    cameraEnabled: true,
    micEnabled: true,
    notificationsMuted: false,
    ringtoneTimer: null,
    paymentPollTimer: null,
    paymentOrderId: null,
    registerStep: 1,
    registerOtpCode: "",
    retryQueue: [],
    heartbeatTimer: null,
    keepaliveTimer: null,
    swRegistration: null
};

const elements = {
    appRoot: document.getElementById("appRoot"),
    authScreen: document.getElementById("authScreen"),
    authTabs: document.getElementById("authTabs"),
    loginForm: document.getElementById("loginForm"),
    registerForm: document.getElementById("registerForm"),
    loginEmail: document.getElementById("loginEmail"),
    loginPassword: document.getElementById("loginPassword"),
    rememberMe: document.getElementById("rememberMe"),
    toggleLoginPasswordBtn: document.getElementById("toggleLoginPasswordBtn"),
    loginSubmitBtn: document.getElementById("loginSubmitBtn"),
    loginFeedback: document.getElementById("loginFeedback"),
    registerName: document.getElementById("registerName"),
    registerEmail: document.getElementById("registerEmail"),
    registerPhone: document.getElementById("registerPhone"),
    registerPassword: document.getElementById("registerPassword"),
    registerConfirmPassword: document.getElementById("registerConfirmPassword"),
    toggleRegisterPasswordBtn: document.getElementById("toggleRegisterPasswordBtn"),
    registerProfileUpload: document.getElementById("registerProfileUpload"),
    registerOtp: document.getElementById("registerOtp"),
    termsCheckbox: document.getElementById("termsCheckbox"),
    registerBackBtn: document.getElementById("registerBackBtn"),
    registerNextBtn: document.getElementById("registerNextBtn"),
    registerSubmitBtn: document.getElementById("registerSubmitBtn"),
    registerFeedback: document.getElementById("registerFeedback"),
    registerProgressBar: document.getElementById("registerProgressBar"),
    registerProgressText: document.getElementById("registerProgressText"),
    passwordStrengthBar: document.getElementById("passwordStrengthBar"),
    passwordStrengthText: document.getElementById("passwordStrengthText"),
    otpHint: document.getElementById("otpHint"),
    offlineBanner: document.getElementById("offlineBanner"),
    globalErrorBanner: document.getElementById("globalErrorBanner"),
    sectionNav: document.getElementById("sectionNav"),
    drawerNav: document.getElementById("drawerNav"),
    profileSelect: document.getElementById("profileSelect"),
    mobileProfileSelect: document.getElementById("mobileProfileSelect"),
    currentUserAvatar: document.getElementById("currentUserAvatar"),
    currentUserName: document.getElementById("currentUserName"),
    firebaseStatus: document.getElementById("firebaseStatus"),
    currentPlanBadge: document.getElementById("currentPlanBadge"),
    searchInput: document.getElementById("searchInput"),
    mobileSearchMirror: document.getElementById("mobileSearchMirror"),
    searchCount: document.getElementById("searchCount"),
    conversationList: document.getElementById("conversationList"),
    notificationPreview: document.getElementById("notificationPreview"),
    notificationList: document.getElementById("notificationList"),
    notificationPanel: document.getElementById("notificationPanel"),
    mobileNotificationBtn: document.getElementById("mobileNotificationBtn"),
    desktopNotificationBtn: document.getElementById("desktopNotificationBtn"),
    mobileNotificationBadge: document.getElementById("mobileNotificationBadge"),
    desktopNotificationBadge: document.getElementById("desktopNotificationBadge"),
    closeNotificationBtn: document.getElementById("closeNotificationBtn"),
    markAllReadBtn: document.getElementById("markAllReadBtn"),
    activeAvatar: document.getElementById("activeAvatar"),
    activeMeta: document.getElementById("activeMeta"),
    activeTitle: document.getElementById("activeTitle"),
    activeStatus: document.getElementById("activeStatus"),
    callBannerTitle: document.getElementById("callBannerTitle"),
    callBannerText: document.getElementById("callBannerText"),
    openCallPanelBtn: document.getElementById("openCallPanelBtn"),
    messageFeed: document.getElementById("messageFeed"),
    typingIndicator: document.getElementById("typingIndicator"),
    composerForm: document.getElementById("composerForm"),
    messageInput: document.getElementById("messageInput"),
    voiceCallBtn: document.getElementById("voiceCallBtn"),
    videoCallBtn: document.getElementById("videoCallBtn"),
    aiTabs: document.getElementById("aiTabs"),
    aiPanel: document.getElementById("aiPanel"),
    connectionStateLabel: document.getElementById("connectionStateLabel"),
    networkLabel: document.getElementById("networkLabel"),
    callDetailLabel: document.getElementById("callDetailLabel"),
    audioInputSelect: document.getElementById("audioInputSelect"),
    audioOutputSelect: document.getElementById("audioOutputSelect"),
    videoInputSelect: document.getElementById("videoInputSelect"),
    callAudioInputSelect: document.getElementById("callAudioInputSelect"),
    callAudioOutputSelect: document.getElementById("callAudioOutputSelect"),
    callVideoInputSelect: document.getElementById("callVideoInputSelect"),
    refreshDevicesBtn: document.getElementById("refreshDevicesBtn"),
    logoutBtn: document.getElementById("logoutBtn"),
    focusModeBtn: document.getElementById("focusModeBtn"),
    newChatBtn: document.getElementById("newChatBtn"),
    openDrawerBtn: document.getElementById("openDrawerBtn"),
    closeDrawerBtn: document.getElementById("closeDrawerBtn"),
    mobileDrawer: document.getElementById("mobileDrawer"),
    drawerBackdrop: document.getElementById("drawerBackdrop"),
    scannerModal: document.getElementById("scannerModal"),
    closeScannerBtn: document.getElementById("closeScannerBtn"),
    captureScanBtn: document.getElementById("captureScanBtn"),
    flipCameraBtn: document.getElementById("flipCameraBtn"),
    scannerStatus: document.getElementById("scannerStatus"),
    incomingCallSheet: document.getElementById("incomingCallSheet"),
    incomingCallerName: document.getElementById("incomingCallerName"),
    incomingCallType: document.getElementById("incomingCallType"),
    acceptCallBtn: document.getElementById("acceptCallBtn"),
    declineCallBtn: document.getElementById("declineCallBtn"),
    callOverlay: document.getElementById("callOverlay"),
    callPeerName: document.getElementById("callPeerName"),
    callStatusText: document.getElementById("callStatusText"),
    callModeLabel: document.getElementById("callModeLabel"),
    callDuration: document.getElementById("callDuration"),
    remoteStage: document.getElementById("remoteStage"),
    remoteVideo: document.getElementById("remoteVideo"),
    remoteAudio: document.getElementById("remoteAudio"),
    remotePlaceholder: document.getElementById("remotePlaceholder"),
    remoteAvatarLarge: document.getElementById("remoteAvatarLarge"),
    remotePlaceholderName: document.getElementById("remotePlaceholderName"),
    remotePlaceholderText: document.getElementById("remotePlaceholderText"),
    localVideo: document.getElementById("localVideo"),
    localPreviewFallback: document.getElementById("localPreviewFallback"),
    muteToggleBtn: document.getElementById("muteToggleBtn"),
    cameraToggleBtn: document.getElementById("cameraToggleBtn"),
    speakerToggleBtn: document.getElementById("speakerToggleBtn"),
    switchCameraBtn: document.getElementById("switchCameraBtn"),
    shareScreenBtn: document.getElementById("shareScreenBtn"),
    endCallBtn: document.getElementById("endCallBtn"),
    paymentPlanSelect: document.getElementById("paymentPlanSelect"),
    paymentAmountLabel: document.getElementById("paymentAmountLabel"),
    paymentStateBadge: document.getElementById("paymentStateBadge"),
    paymentStatusText: document.getElementById("paymentStatusText"),
    paymentProgress: document.getElementById("paymentProgress"),
    paymentProgressText: document.getElementById("paymentProgressText"),
    paymentResult: document.getElementById("paymentResult"),
    paymentHistory: document.getElementById("paymentHistory"),
    startPaymentBtn: document.getElementById("startPaymentBtn"),
    toastStack: document.getElementById("toastStack")
};

function buildInitialConversations() {
    const conversations = {};
    profiles
        .filter((profile) => profile.id !== state.activeProfileId)
        .forEach((profile) => {
            conversations[profile.id] = {
                id: profile.id,
                name: profile.name,
                avatar: profile.avatar,
                tag: profile.tag,
                category: resolveCategory(profile.id),
                preview: "Ready for realtime calling.",
                time: "Now",
                unread: 0,
                messages: [...(conversationSeed[profile.id] || [])]
            };
        });
    state.conversations = conversations;
}

function resolveCategory(profileId) {
    if (profileId === "nova" || profileId === "kai") {
        return "gaming";
    }
    if (profileId === "rhea") {
        return "study";
    }
    if (profileId === "alya") {
        return "bots";
    }
    if (profileId === "mina") {
        return "groups";
    }
    return "friends";
}

async function init() {
    buildInitialConversations();
    state.activeConversationId = Object.keys(state.conversations)[0];
    renderNav();
    renderProfileSelectors();
    renderConversations();
    renderNotifications();
    renderChat();
    renderAiPanel();
    syncNotificationBadges();
    updatePaymentPlanLabel();
    applyInitialPaymentState();
    restoreRegisterDraft();
    renderRegisterStep();
    bindEvents();
    registerGlobalHandlers();
    await registerServiceWorker();
    state.retryQueue = loadQueuedActions();
}

function debounce(fn, delay = 180) {
    let timeoutId = null;
    return (...args) => {
        clearTimeout(timeoutId);
        timeoutId = window.setTimeout(() => fn(...args), delay);
    };
}

function bindEvents() {
    const debouncedSearch = debounce((value) => handleSearch(value), 140);
    elements.authTabs.addEventListener("click", (event) => {
        const button = event.target.closest("[data-auth-view]");
        if (!button) {
            return;
        }
        switchAuthView(button.dataset.authView);
    });

    elements.loginForm.addEventListener("submit", handleLoginSubmit);
    elements.registerForm.addEventListener("submit", handleRegisterSubmit);
    elements.toggleLoginPasswordBtn.addEventListener("click", () => togglePasswordField(elements.loginPassword, elements.toggleLoginPasswordBtn));
    elements.toggleRegisterPasswordBtn.addEventListener("click", () => togglePasswordField(elements.registerPassword, elements.toggleRegisterPasswordBtn));
    elements.registerBackBtn.addEventListener("click", goToPreviousRegisterStep);
    elements.registerNextBtn.addEventListener("click", goToNextRegisterStep);
    elements.registerPassword.addEventListener("input", updatePasswordStrength);
    [
        elements.registerName,
        elements.registerEmail,
        elements.registerPhone,
        elements.registerPassword,
        elements.registerConfirmPassword,
        elements.registerOtp
    ].forEach((input) => {
        input.addEventListener("input", persistRegisterDraft);
    });

    [elements.searchInput, elements.mobileSearchMirror].forEach((input) => {
        input.addEventListener("input", (event) => debouncedSearch(event.target.value));
    });

    [elements.profileSelect, elements.mobileProfileSelect].forEach((select) => {
        select.addEventListener("change", async (event) => {
            await switchProfile(event.target.value);
        });
    });

    elements.composerForm.addEventListener("submit", (event) => {
        event.preventDefault();
        sendMessage();
    });
    elements.messageInput.addEventListener("input", autoResizeInput);

    document.querySelectorAll(".tool-btn").forEach((button) => {
        button.addEventListener("click", () => handleTool(button.dataset.action));
    });

    elements.aiTabs.addEventListener("click", (event) => {
        const button = event.target.closest(".ai-tab");
        if (!button) {
            return;
        }
        state.activeAiTab = button.dataset.tab;
        renderAiPanel();
    });

    [elements.mobileNotificationBtn, elements.desktopNotificationBtn].forEach((button) => {
        button.addEventListener("click", toggleNotifications);
    });
    elements.closeNotificationBtn.addEventListener("click", closeNotifications);
    elements.markAllReadBtn.addEventListener("click", markAllRead);

    elements.voiceCallBtn.addEventListener("click", () => startOutgoingCall("voice"));
    elements.videoCallBtn.addEventListener("click", () => startOutgoingCall("video"));
    elements.acceptCallBtn.addEventListener("click", acceptIncomingCall);
    elements.declineCallBtn.addEventListener("click", () => declineIncomingCall("declined"));
    elements.endCallBtn.addEventListener("click", () => endCurrentCall("ended"));
    elements.openCallPanelBtn.addEventListener("click", () => {
        if (!state.currentCall) {
            toast("Start or accept a call first");
            return;
        }
        openCallOverlay();
    });

    elements.muteToggleBtn.addEventListener("click", toggleMute);
    elements.cameraToggleBtn.addEventListener("click", toggleCamera);
    elements.speakerToggleBtn.addEventListener("click", toggleSpeaker);
    elements.switchCameraBtn.addEventListener("click", switchCameraInput);
    elements.shareScreenBtn.addEventListener("click", toggleScreenShare);

    [elements.audioInputSelect, elements.callAudioInputSelect].forEach((select) => {
        select.addEventListener("change", async (event) => {
            state.selectedAudioInputId = event.target.value;
            syncDeviceSelections();
            if (state.currentCall) {
                await replaceMediaTracks({
                    audio: true,
                    video: Boolean(getLocalVideoTrack()) && !state.screenStream
                });
            }
        });
    });

    [elements.videoInputSelect, elements.callVideoInputSelect].forEach((select) => {
        select.addEventListener("change", async (event) => {
            state.selectedVideoInputId = event.target.value;
            syncDeviceSelections();
            if (state.currentCall && getLocalVideoTrack() && !state.screenStream) {
                await replaceMediaTracks({ audio: false, video: true });
            }
        });
    });

    [elements.audioOutputSelect, elements.callAudioOutputSelect].forEach((select) => {
        select.addEventListener("change", async (event) => {
            state.selectedAudioOutputId = event.target.value;
            syncDeviceSelections();
            await applyAudioOutputDevice();
        });
    });

    elements.refreshDevicesBtn.addEventListener("click", async () => {
        await refreshDeviceLists();
        toast("Device list refreshed");
    });
    elements.logoutBtn.addEventListener("click", handleLogout);

    elements.paymentPlanSelect.addEventListener("change", updatePaymentPlanLabel);
    elements.startPaymentBtn.addEventListener("click", startPaymentFlow);

    elements.focusModeBtn.addEventListener("click", () => pulsePanel(".call-health-card", "Focus mode activated"));
    elements.newChatBtn.addEventListener("click", createChat);

    elements.openDrawerBtn.addEventListener("click", openDrawer);
    elements.closeDrawerBtn.addEventListener("click", closeDrawer);
    elements.drawerBackdrop.addEventListener("click", closeDrawer);

    elements.closeScannerBtn.addEventListener("click", closeScanner);
    elements.captureScanBtn.addEventListener("click", captureScan);
    elements.flipCameraBtn.addEventListener("click", () => toast("Scanner flipped"));
    elements.scannerModal.addEventListener("click", (event) => {
        if (event.target === elements.scannerModal) {
            closeScanner();
        }
    });

    document.addEventListener("click", (event) => {
        if (state.notificationsOpen) {
            const insidePanel = event.target.closest("#notificationPanel");
            const onTrigger = event.target.closest("#mobileNotificationBtn, #desktopNotificationBtn");
            if (!insidePanel && !onTrigger) {
                closeNotifications();
            }
        }
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeDrawer();
            closeNotifications();
            closeScanner();
            if (state.currentCall) {
                openCallOverlay();
            }
        }
    });

    window.addEventListener("resize", handleResize);
    window.addEventListener("beforeunload", () => {
        void shutdownPresence();
    });
    window.addEventListener("online", handleOnline);
    window.addEventListener("offline", handleOffline);

    navigator.mediaDevices?.addEventListener?.("devicechange", async () => {
        await refreshDeviceLists();
    });
}

onAuthStateChanged(auth, async (user) => {
    state.authReady = true;
    state.authUser = user;

    if (!user) {
        elements.appRoot.hidden = true;
        elements.authScreen.hidden = false;
        elements.firebaseStatus.textContent = "Sign in required";
        stopSessionTimers();
        return;
    }

    await ensureUserProfile();
    elements.appRoot.hidden = false;
    elements.authScreen.hidden = true;
    elements.firebaseStatus.textContent = "Firebase connected";
    updateNetworkLabel(navigator.onLine);
    renderCurrentTier();
    updateDocumentTitle();
    await refreshPresenceProfile();
    subscribeToPresence();
    subscribeToCalls();
    await refreshDeviceLists();
    await loadPaymentHistory();
    startSessionTimers();
});

function switchAuthView(view) {
    const loginActive = view === "login";
    document.querySelectorAll("[data-auth-view]").forEach((button) => {
        button.classList.toggle("active", button.dataset.authView === view);
    });
    elements.loginForm.classList.toggle("hidden-auth", !loginActive);
    elements.registerForm.classList.toggle("hidden-auth", loginActive);
}

function setAuthFeedback(target, message, type = "") {
    target.textContent = message;
    target.className = `auth-feedback${type ? ` ${type}` : ""}`;
}

function togglePasswordField(input, trigger) {
    const nextType = input.type === "password" ? "text" : "password";
    input.type = nextType;
    trigger.textContent = nextType === "password" ? "Show" : "Hide";
}

function normalizePhone(value) {
    return value.replace(/[^\d+]/g, "").replace(/^(\d{10})$/, "+91$1");
}

function validateEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function calculatePasswordStrength(password) {
    let score = 0;
    if (password.length >= 8) score += 25;
    if (/[A-Z]/.test(password) && /[a-z]/.test(password)) score += 25;
    if (/\d/.test(password)) score += 25;
    if (/[^A-Za-z0-9]/.test(password)) score += 25;
    return score;
}

function updatePasswordStrength() {
    const score = calculatePasswordStrength(elements.registerPassword.value);
    elements.passwordStrengthBar.style.width = `${score}%`;
    elements.passwordStrengthText.textContent = score >= 100
        ? "Strong password"
        : score >= 75
            ? "Good password"
            : score >= 50
                ? "Needs one more improvement"
                : "Use at least 8 characters with mixed case, a number, and a symbol.";
}

function persistRegisterDraft() {
    const draft = {
        name: elements.registerName.value,
        email: elements.registerEmail.value,
        phone: elements.registerPhone.value,
        password: elements.registerPassword.value,
        confirmPassword: elements.registerConfirmPassword.value,
        otp: elements.registerOtp.value,
        step: state.registerStep
    };
    localStorage.setItem(storageKeys.registerDraft, JSON.stringify(draft));
}

function restoreRegisterDraft() {
    const raw = localStorage.getItem(storageKeys.registerDraft);
    if (!raw) {
        return;
    }
    try {
        const draft = JSON.parse(raw);
        elements.registerName.value = draft.name || "";
        elements.registerEmail.value = draft.email || "";
        elements.registerPhone.value = draft.phone || "";
        elements.registerPassword.value = draft.password || "";
        elements.registerConfirmPassword.value = draft.confirmPassword || "";
        elements.registerOtp.value = draft.otp || "";
        state.registerStep = Math.min(Math.max(Number(draft.step) || 1, 1), 3);
        updatePasswordStrength();
    } catch (error) {
        localStorage.removeItem(storageKeys.registerDraft);
    }
}

function clearRegisterDraft() {
    localStorage.removeItem(storageKeys.registerDraft);
}

function renderRegisterStep() {
    const step = state.registerStep;
    document.querySelectorAll(".register-step").forEach((node) => {
        node.classList.toggle("hidden-auth", Number(node.dataset.step) !== step);
    });
    elements.registerBackBtn.classList.toggle("hidden-auth", step === 1);
    elements.registerNextBtn.classList.toggle("hidden-auth", step === 3);
    elements.registerSubmitBtn.classList.toggle("hidden-auth", step !== 3);
    elements.registerProgressBar.style.width = `${(step / 3) * 100}%`;
    elements.registerProgressText.textContent = `Step ${step} of 3`;
}

function goToPreviousRegisterStep() {
    state.registerStep = Math.max(1, state.registerStep - 1);
    persistRegisterDraft();
    renderRegisterStep();
}

async function goToNextRegisterStep() {
    const valid = await validateRegisterStep(state.registerStep);
    if (!valid) {
        return;
    }
    if (state.registerStep === 2) {
        state.registerOtpCode = String(Math.floor(100000 + Math.random() * 900000));
        elements.otpHint.textContent = `Verification code: ${state.registerOtpCode}`;
    }
    state.registerStep = Math.min(3, state.registerStep + 1);
    persistRegisterDraft();
    renderRegisterStep();
}

async function validateRegisterStep(step) {
    if (step === 1) {
        if (!elements.registerName.value.trim()) {
            setAuthFeedback(elements.registerFeedback, "Full name is required.", "error");
            return false;
        }
        if (!validateEmail(elements.registerEmail.value.trim())) {
            setAuthFeedback(elements.registerFeedback, "Use a valid email address.", "error");
            return false;
        }
        const phone = normalizePhone(elements.registerPhone.value.trim());
        elements.registerPhone.value = phone;
        if (!/^\+91\d{10}$/.test(phone)) {
            setAuthFeedback(elements.registerFeedback, "Use a valid +91 phone number.", "error");
            return false;
        }
        const duplicateEmail = await getDocs(query(collection(db, "users"), where("email", "==", elements.registerEmail.value.trim().toLowerCase()), limit(1)));
        if (!duplicateEmail.empty) {
            setAuthFeedback(elements.registerFeedback, "Email already exists.", "error");
            return false;
        }
        const duplicatePhone = await getDocs(query(collection(db, "users"), where("phone", "==", phone), limit(1)));
        if (!duplicatePhone.empty) {
            setAuthFeedback(elements.registerFeedback, "Phone number already exists.", "error");
            return false;
        }
    }

    if (step === 2) {
        if (calculatePasswordStrength(elements.registerPassword.value) < 100) {
            setAuthFeedback(elements.registerFeedback, "Password must be stronger.", "error");
            return false;
        }
        if (elements.registerPassword.value !== elements.registerConfirmPassword.value) {
            setAuthFeedback(elements.registerFeedback, "Passwords do not match.", "error");
            return false;
        }
    }

    setAuthFeedback(elements.registerFeedback, "");
    return true;
}

async function handleLoginSubmit(event) {
    event.preventDefault();
    setAuthFeedback(elements.loginFeedback, "");
    const email = elements.loginEmail.value.trim().toLowerCase();
    const password = elements.loginPassword.value;
    if (!validateEmail(email) || password.length < 8) {
        setAuthFeedback(elements.loginFeedback, "Enter a valid email and password.", "error");
        return;
    }

    try {
        elements.loginSubmitBtn.disabled = true;
        elements.loginSubmitBtn.textContent = "Logging in...";
        await setPersistence(auth, elements.rememberMe.checked ? browserLocalPersistence : browserSessionPersistence);
        const credential = await signInWithEmailAndPassword(auth, email, password);
        setAuthFeedback(
            elements.loginFeedback,
            credential.user.emailVerified ? "Login successful." : "Login successful. Please verify your email soon.",
            credential.user.emailVerified ? "success" : ""
        );
    } catch (error) {
        setAuthFeedback(elements.loginFeedback, error.message || "Login failed.", "error");
    } finally {
        elements.loginSubmitBtn.disabled = false;
        elements.loginSubmitBtn.textContent = "Login";
    }
}

async function handleRegisterSubmit(event) {
    event.preventDefault();
    const valid = await validateRegisterStep(3);
    if (!valid) {
        return;
    }
    if (elements.registerOtp.value.trim() !== state.registerOtpCode) {
        setAuthFeedback(elements.registerFeedback, "Invalid verification code.", "error");
        return;
    }
    if (!elements.termsCheckbox.checked) {
        setAuthFeedback(elements.registerFeedback, "You must accept the terms.", "error");
        return;
    }

    try {
        elements.registerSubmitBtn.disabled = true;
        elements.registerSubmitBtn.textContent = "Creating...";
        await setPersistence(auth, browserLocalPersistence);
        const credential = await createUserWithEmailAndPassword(auth, elements.registerEmail.value.trim().toLowerCase(), elements.registerPassword.value);
        await updateProfile(credential.user, {
            displayName: elements.registerName.value.trim()
        });
        await sendEmailVerification(credential.user);
        await setDoc(doc(db, "users", credential.user.uid), {
            fullName: elements.registerName.value.trim(),
            email: elements.registerEmail.value.trim().toLowerCase(),
            phone: normalizePhone(elements.registerPhone.value.trim()),
            plan: "free",
            premium: false,
            profilePhotoName: elements.registerProfileUpload.files?.[0]?.name || "",
            createdAt: serverTimestamp()
        }, { merge: true });
        clearRegisterDraft();
        setAuthFeedback(elements.registerFeedback, "Account created. Verification email sent.", "success");
    } catch (error) {
        setAuthFeedback(elements.registerFeedback, error.message || "Registration failed.", "error");
    } finally {
        elements.registerSubmitBtn.disabled = false;
        elements.registerSubmitBtn.textContent = "Create account";
    }
}

async function ensureUserProfile() {
    if (!state.authUser) {
        return;
    }
    const userRef = doc(db, "users", state.authUser.uid);
    const snapshot = await getDoc(userRef);
    if (!snapshot.exists()) {
        await setDoc(userRef, {
            fullName: state.authUser.displayName || state.authUser.email?.split("@")[0] || "Z Chat User",
            email: state.authUser.email || "",
            phone: "",
            plan: "free",
            premium: false,
            createdAt: serverTimestamp()
        }, { merge: true });
    }
    const freshSnapshot = await getDoc(userRef);
    state.userProfile = { id: freshSnapshot.id, ...freshSnapshot.data() };
}

function renderCurrentTier() {
    const plan = getCurrentPlan().toUpperCase();
    elements.currentPlanBadge.textContent = plan;
    renderAiPanel();
}

function startSessionTimers() {
    stopSessionTimers();
    state.heartbeatTimer = window.setInterval(() => {
        if (navigator.onLine) {
            void refreshPresenceProfile().catch(() => {});
        }
    }, 30000);
    state.keepaliveTimer = window.setInterval(async () => {
        if (state.authUser) {
            await state.authUser.getIdToken(true).catch(() => {});
            await refreshPresenceProfile().catch(() => {});
        }
    }, 300000);
}

function stopSessionTimers() {
    clearInterval(state.heartbeatTimer);
    clearInterval(state.keepaliveTimer);
    state.heartbeatTimer = null;
    state.keepaliveTimer = null;
}

async function handleLogout() {
    stopSessionTimers();
    await shutdownPresence().catch(() => {});
    await signOut(auth);
    toast("Logged out");
}

function updateDocumentTitle() {
    const active = getActiveConversation();
    const plan = String(state.userProfile?.plan || state.userProfile?.premiumPlan || "free").toUpperCase();
    document.title = active ? `${active.name} | Z Chat ${plan}` : `Z Chat ${plan}`;
}

function registerGlobalHandlers() {
    window.addEventListener("error", (event) => {
        showGlobalError(event.message || "Unexpected error");
    });
    window.addEventListener("unhandledrejection", (event) => {
        showGlobalError(event.reason?.message || "Unhandled async error");
    });
}

function showGlobalError(message) {
    elements.globalErrorBanner.hidden = false;
    elements.globalErrorBanner.textContent = message;
    window.setTimeout(() => {
        elements.globalErrorBanner.hidden = true;
    }, 4000);
}

function renderProfileSelectors() {
    const options = profiles.map((profile) => `
        <option value="${profile.id}" ${profile.id === state.activeProfileId ? "selected" : ""}>
            ${profile.name}
        </option>
    `).join("");

    elements.profileSelect.innerHTML = options;
    elements.mobileProfileSelect.innerHTML = options;
    const me = getActiveProfile();
    elements.currentUserAvatar.textContent = me.avatar;
    elements.currentUserName.textContent = me.name;
}

function renderNav() {
    const markup = sections.map((section) => `
        <button class="nav-item ${section.id === state.activeSection ? "active" : ""}" data-section="${section.id}" type="button">
            <span>${section.icon}</span>
            <span>${section.label}</span>
        </button>
    `).join("");

    elements.sectionNav.innerHTML = markup;
    elements.drawerNav.innerHTML = sections.map((section) => `
        <button class="conversation-item ${section.id === state.activeSection ? "active" : ""}" data-section="${section.id}" type="button">
            <div class="conversation-avatar">${section.icon}</div>
            <div class="conversation-content">
                <div class="conversation-line">
                    <strong>${section.label}</strong>
                </div>
                <span class="conversation-preview">Open ${section.label.toLowerCase()} workspace</span>
            </div>
        </button>
    `).join("");

    document.querySelectorAll("[data-section]").forEach((button) => {
        button.addEventListener("click", () => {
            state.activeSection = button.dataset.section;
            renderNav();
            renderConversations();
            closeDrawer();
        });
    });
}

function getFilteredConversations() {
    const list = Object.values(state.conversations).filter((conversation) => {
        const matchesSection = state.activeSection === "chats" || conversation.category === state.activeSection;
        const matchesSearch = !state.searchTerm
            || `${conversation.name} ${conversation.tag} ${conversation.preview}`.toLowerCase().includes(state.searchTerm);
        return matchesSection && matchesSearch;
    });

    if (!list.length) {
        return [];
    }

    return list.sort((left, right) => right.unread - left.unread);
}

function renderConversations() {
    const filtered = getFilteredConversations();
    elements.searchCount.textContent = String(filtered.length);

    elements.conversationList.innerHTML = filtered.map((conversation) => {
        const presence = state.profilesPresence[conversation.id] || {};
        const statusClass = presence.currentCallId ? "busy" : presence.online ? "online" : "";
        const statusText = presence.currentCallId ? "Busy on another call" : presence.online ? "Online now" : "Offline";
        return `
            <button class="conversation-item ${conversation.id === state.activeConversationId ? "active" : ""}" data-chat="${conversation.id}" type="button">
                <div class="conversation-avatar">${conversation.avatar}</div>
                <div class="conversation-content">
                    <div class="conversation-line">
                        <strong>${conversation.name}</strong>
                        <span class="status-dot ${statusClass}"></span>
                    </div>
                    <div class="conversation-line">
                        <span class="conversation-preview">${conversation.preview}</span>
                        <span class="pill">${statusText}</span>
                    </div>
                </div>
            </button>
        `;
    }).join("") || `<div class="card"><strong>No matches</strong><p>Try another search term or section.</p></div>`;

    document.querySelectorAll("[data-chat]").forEach((button) => {
        button.addEventListener("click", () => switchConversation(button.dataset.chat));
    });
}

function renderChat() {
    const active = getActiveConversation();
    if (!active) {
        return;
    }
    elements.activeAvatar.textContent = active.avatar;
    elements.activeMeta.textContent = active.tag;
    elements.activeTitle.textContent = active.name;
    elements.activeStatus.textContent = active.preview;
    elements.messageFeed.innerHTML = active.messages.map((message) => `
        <article class="message-row ${message.self ? "self" : ""}">
            <div class="message-body">
                <div class="conversation-avatar">${message.avatar}</div>
                <div class="bubble">
                    <div class="bubble-head">
                        <strong>${message.author}</strong>
                        <span>${message.time}</span>
                    </div>
                    <p>${message.text}</p>
                </div>
            </div>
        </article>
    `).join("");
    elements.messageFeed.scrollTop = elements.messageFeed.scrollHeight;
}

function renderNotifications() {
    const markup = notifications.map((item) => `
        <article class="notification-item ${item.unread ? "unread" : ""}">
            <strong>${item.title}</strong>
            <p>${item.body}</p>
        </article>
    `).join("");
    elements.notificationPreview.innerHTML = markup;
    elements.notificationList.innerHTML = markup;
}

function renderAiPanel() {
    const activeMode = aiModes[state.activeAiTab];
    document.querySelectorAll(".ai-tab").forEach((button) => {
        button.classList.toggle("active", button.dataset.tab === state.activeAiTab);
        const requiredPlan = button.dataset.tab === "study" ? "premium" : button.dataset.tab === "gamer" ? "moderate" : "free";
        button.disabled = !hasTier(requiredPlan);
    });

    if (!hasTier(state.activeAiTab === "study" ? "premium" : state.activeAiTab === "gamer" ? "moderate" : "free")) {
        state.activeAiTab = "coder";
        return renderAiPanel();
    }

    elements.aiPanel.innerHTML = `
        <section class="card">
            <div class="card-head">
                <div>
                    <p class="kicker">Active mode</p>
                    <h3>${activeMode.title}</h3>
                </div>
            </div>
            <p>${activeMode.description}</p>
        </section>
        <section class="card">
            <div class="card-head">
                <div>
                    <p class="kicker">Actions</p>
                    <h3>Tap to run</h3>
                </div>
            </div>
            <div class="ai-tabs">
                ${activeMode.actions.map((action) => `<button class="quick-btn ai-action-btn" type="button">${action}</button>`).join("")}
            </div>
        </section>
    `;

    document.querySelectorAll(".ai-action-btn").forEach((button) => {
        button.addEventListener("click", () => toast(`${button.textContent} queued in ${activeMode.title}`));
    });
}

function syncNotificationBadges() {
    const count = notifications.filter((item) => item.unread).length;
    elements.mobileNotificationBadge.textContent = String(count);
    elements.desktopNotificationBadge.textContent = String(count);
}

function getActiveConversation() {
    return state.conversations[state.activeConversationId];
}

function getActiveProfile() {
    return profiles.find((profile) => profile.id === state.activeProfileId) || profiles[0];
}

function getCurrentPlan() {
    return String(state.userProfile?.plan || state.userProfile?.premiumPlan || "free").toLowerCase();
}

function hasTier(requiredPlan) {
    const ranking = {
        free: 0,
        moderate: 1,
        premium: 2
    };
    return ranking[getCurrentPlan()] >= ranking[requiredPlan];
}

async function switchProfile(profileId) {
    if (state.currentCall) {
        toast("End the current call before switching profiles");
        renderProfileSelectors();
        return;
    }

    state.activeProfileId = profileId;
    localStorage.setItem(storageKeys.profileId, profileId);
    buildInitialConversations();
    state.activeConversationId = Object.keys(state.conversations)[0];
    renderProfileSelectors();
    renderConversations();
    renderChat();
    await refreshPresenceProfile();
    toast(`Switched to ${getActiveProfile().name}`);
}

async function refreshPresenceProfile() {
    if (!state.authUser) {
        return;
    }

    const me = getActiveProfile();
    await setDoc(doc(db, "presence", me.id), {
        authUid: state.authUser.uid,
        profileId: me.id,
        displayName: me.name,
        avatar: me.avatar,
        tag: me.tag,
        online: true,
        currentCallId: state.currentCall?.id || null,
        updatedAt: serverTimestamp()
    }, { merge: true });
}

async function shutdownPresence() {
    const me = getActiveProfile();
    if (!state.authUser) {
        return;
    }
    await setDoc(doc(db, "presence", me.id), {
        online: false,
        currentCallId: null,
        updatedAt: serverTimestamp()
    }, { merge: true });
}

function subscribeToPresence() {
    state.presenceUnsub?.();
    state.presenceUnsub = onSnapshot(collection(db, "presence"), (snapshot) => {
        const next = {};
        snapshot.forEach((entry) => {
            next[entry.id] = entry.data();
        });
        state.profilesPresence = next;
        renderConversations();
        updateNetworkLabel(navigator.onLine);
    });
}

function subscribeToCalls() {
    state.callsListUnsub?.();
    const callsQuery = query(collection(db, "calls"), orderBy("createdAt", "desc"));
    state.callsListUnsub = onSnapshot(callsQuery, (snapshot) => {
        snapshot.docChanges().forEach((change) => {
            const call = { id: change.doc.id, ...change.doc.data() };
            const isIncoming = call.calleeId === state.activeProfileId;
            const isMine = call.callerId === state.activeProfileId || call.calleeId === state.activeProfileId;

            if (isIncoming && call.status === "ringing" && !state.currentCall) {
                state.incomingCall = call;
                showIncomingCall(call);
            }

            if (state.incomingCall && state.incomingCall.id === call.id && ["cancelled", "ended", "declined", "failed"].includes(call.status)) {
                state.incomingCall = null;
                hideIncomingCall();
                toast(`Incoming call ${call.status}`);
            }

            if (state.currentCall && state.currentCall.id === call.id && isMine) {
                handleCallDocUpdate(call);
            }
        });
    }, (error) => {
        toast(error.message || "Failed to subscribe to calls");
    });
}

async function startOutgoingCall(mode) {
    if (mode === "video" && !hasTier("moderate")) {
        toast("Video calls unlock on the Moderate plan.");
        return;
    }
    if (!navigator.onLine) {
        toast("No internet connection");
        return;
    }
    if (state.currentCall) {
        toast("A call is already active");
        return;
    }

    const target = getActiveConversation();
    const presence = state.profilesPresence[target.id];
    if (!presence?.online) {
        toast(`${target.name} is offline`);
        return;
    }
    if (presence.currentCallId) {
        toast(`${target.name} is busy`);
        return;
    }

    try {
        const localStream = await getLocalMedia({
            audio: true,
            video: mode === "video"
        });
        const me = getActiveProfile();
        const callDocRef = await addDoc(collection(db, "calls"), {
            callerId: me.id,
            callerName: me.name,
            callerAvatar: me.avatar,
            calleeId: target.id,
            calleeName: target.name,
            calleeAvatar: target.avatar,
            mode,
            status: "ringing",
            createdAt: serverTimestamp()
        });

        state.currentCall = {
            id: callDocRef.id,
            role: "caller",
            mode,
            peerId: target.id,
            peerName: target.name,
            peerAvatar: target.avatar,
            startedAt: null
        };

        await refreshPresenceProfile();
        await setupPeerConnection(callDocRef.id, "caller", mode, localStream);
        await bindCallDocument(callDocRef.id);

        const offer = await state.peerConnection.createOffer();
        await state.peerConnection.setLocalDescription(offer);
        await updateDoc(doc(db, "calls", callDocRef.id), {
            offer: {
                type: offer.type,
                sdp: offer.sdp
            },
            status: "ringing"
        });

        updateCallUi({
            title: target.name,
            status: "Calling...",
            mode
        });
        openCallOverlay();
    } catch (error) {
        handleMediaError(error);
    }
}

async function acceptIncomingCall() {
    if (!state.incomingCall) {
        return;
    }

    try {
        const call = state.incomingCall;
        if (!call.offer) {
            toast("Incoming call offer is still arriving. Try again in a moment.");
            return;
        }
        const localStream = await getLocalMedia({
            audio: true,
            video: call.mode === "video"
        });

        state.currentCall = {
            id: call.id,
            role: "callee",
            mode: call.mode,
            peerId: call.callerId,
            peerName: call.callerName,
            peerAvatar: call.callerAvatar,
            startedAt: null
        };
        state.incomingCall = null;
        hideIncomingCall();
        await refreshPresenceProfile();
        await setupPeerConnection(call.id, "callee", call.mode, localStream);
        await bindCallDocument(call.id);

        const offerDescription = new RTCSessionDescription(call.offer);
        await state.peerConnection.setRemoteDescription(offerDescription);
        const answer = await state.peerConnection.createAnswer();
        await state.peerConnection.setLocalDescription(answer);

        await updateDoc(doc(db, "calls", call.id), {
            answer: {
                type: answer.type,
                sdp: answer.sdp
            },
            status: "connecting",
            acceptedAt: serverTimestamp()
        });

        updateCallUi({
            title: call.callerName,
            status: "Connecting...",
            mode: call.mode
        });
        openCallOverlay();
    } catch (error) {
        handleMediaError(error);
        await declineIncomingCall("failed");
    }
}

async function declineIncomingCall(status = "declined") {
    if (!state.incomingCall) {
        return;
    }

    const callId = state.incomingCall.id;
    state.incomingCall = null;
    hideIncomingCall();
    await updateDoc(doc(db, "calls", callId), {
        status,
        endedAt: serverTimestamp()
    }).catch(() => {});
}

async function bindCallDocument(callId) {
    state.callDocUnsub?.();
    state.callDocUnsub = onSnapshot(doc(db, "calls", callId), (snapshot) => {
        if (!snapshot.exists()) {
            return;
        }
        handleCallDocUpdate({ id: snapshot.id, ...snapshot.data() });
    });
}

async function setupPeerConnection(callId, role, mode, localStream) {
    cleanupPeerConnection();

    state.peerConnection = new RTCPeerConnection(rtcConfig);
    state.localStream = localStream;
    state.remoteStream = new MediaStream();
    elements.remoteVideo.srcObject = state.remoteStream;
    elements.remoteAudio.srcObject = state.remoteStream;
    elements.localVideo.srcObject = localStream;
    elements.localPreviewFallback.hidden = hasEnabledVideoTrack(localStream);
    elements.remotePlaceholder.hidden = false;

    state.localCandidatesCollection = collection(db, "calls", callId, role === "caller" ? "offerCandidates" : "answerCandidates");
    const remoteCandidatesCollection = collection(db, "calls", callId, role === "caller" ? "answerCandidates" : "offerCandidates");

    localStream.getTracks().forEach((track) => {
        state.peerConnection.addTrack(track, localStream);
    });

    state.peerConnection.ontrack = (event) => {
        event.streams[0].getTracks().forEach((track) => {
            state.remoteStream.addTrack(track);
        });
        elements.remotePlaceholder.hidden = hasRemoteVideo();
        elements.remotePlaceholderText.textContent = hasRemoteVideo()
            ? "Remote media connected"
            : "Remote audio connected";
        void applyAudioOutputDevice();
    };

    state.peerConnection.onicecandidate = async (event) => {
        if (!event.candidate || !state.localCandidatesCollection) {
            return;
        }
        await addDoc(state.localCandidatesCollection, event.candidate.toJSON());
    };

    state.peerConnection.onconnectionstatechange = async () => {
        const connectionState = state.peerConnection?.connectionState || "new";
        elements.connectionStateLabel.textContent = connectionState;

        if (connectionState === "connected" && state.currentCall) {
            await updateDoc(doc(db, "calls", state.currentCall.id), {
                status: "active"
            }).catch(() => {});
            onCallConnected();
        }

        if (["failed", "disconnected", "closed"].includes(connectionState) && state.currentCall) {
            updateCallUi({
                title: state.currentCall.peerName,
                status: `Connection ${connectionState}`,
                mode: state.currentCall.mode
            });
            if (connectionState === "failed") {
                toast("Call failed");
            }
        }
    };

    state.remoteCandidatesUnsub?.();
    state.remoteCandidatesUnsub = onSnapshot(remoteCandidatesCollection, (snapshot) => {
        snapshot.docChanges().forEach(async (change) => {
            if (change.type !== "added") {
                return;
            }
            const candidate = new RTCIceCandidate(change.doc.data());
            try {
                await state.peerConnection.addIceCandidate(candidate);
            } catch (error) {
                console.error("Failed to add ICE candidate", error);
            }
        });
    });

    syncCallControlLabels();
}

function handleCallDocUpdate(call) {
    if (!state.currentCall || state.currentCall.id !== call.id) {
        return;
    }

    state.currentCall.status = call.status;
    updateCallUi({
        title: state.currentCall.peerName,
        status: resolveCallStatusText(call.status),
        mode: state.currentCall.mode
    });

    if (call.answer && state.currentCall.role === "caller" && !state.peerConnection.currentRemoteDescription) {
        void state.peerConnection.setRemoteDescription(new RTCSessionDescription(call.answer));
    }

    if (call.status === "active") {
        onCallConnected();
    }

    if (["ended", "declined", "cancelled", "failed"].includes(call.status)) {
        toast(`Call ${call.status}`);
        void tearDownCallUi(call.status);
    }
}

function resolveCallStatusText(status) {
    const labels = {
        ringing: "Calling...",
        connecting: "Connecting...",
        active: "Connected",
        declined: "Declined",
        ended: "Call ended",
        cancelled: "Cancelled",
        failed: "Failed"
    };
    return labels[status] || "Waiting...";
}

function onCallConnected() {
    if (!state.currentCall || state.currentCall.startedAt) {
        return;
    }

    state.currentCall.startedAt = Date.now();
    elements.remoteStage.classList.remove("ringing");
    startCallDuration();
    updateCallUi({
        title: state.currentCall.peerName,
        status: "Connected",
        mode: state.currentCall.mode
    });
}

function startCallDuration() {
    clearInterval(state.durationTimer);
    state.durationTimer = window.setInterval(() => {
        if (!state.currentCall?.startedAt) {
            elements.callDuration.textContent = "00:00";
            return;
        }
        const elapsedMs = Date.now() - state.currentCall.startedAt;
        const totalSeconds = Math.floor(elapsedMs / 1000);
        const minutes = String(Math.floor(totalSeconds / 60)).padStart(2, "0");
        const seconds = String(totalSeconds % 60).padStart(2, "0");
        elements.callDuration.textContent = `${minutes}:${seconds}`;
    }, 1000);
}

function updateCallUi({ title, status, mode }) {
    elements.callPeerName.textContent = title;
    elements.callStatusText.textContent = status;
    elements.callModeLabel.textContent = mode.toUpperCase();
    elements.callBannerTitle.textContent = title;
    elements.callBannerText.textContent = status;
    elements.remotePlaceholderName.textContent = title;
    elements.remotePlaceholderText.textContent = status;
    elements.remoteAvatarLarge.textContent = getPeerAvatar();
    elements.connectionStateLabel.textContent = status;
    elements.callDetailLabel.textContent = `${mode} call with ${title}`;
    elements.remoteStage.classList.toggle("ringing", ["Calling...", "Connecting..."].includes(status));
}

function openCallOverlay() {
    elements.callOverlay.hidden = false;
}

function closeCallOverlay() {
    elements.callOverlay.hidden = true;
}

function showIncomingCall(call) {
    elements.incomingCallerName.textContent = call.callerName;
    elements.incomingCallType.textContent = `${call.mode === "video" ? "Video" : "Voice"} call`;
    elements.incomingCallSheet.hidden = false;
    playRingtone();
}

function hideIncomingCall() {
    elements.incomingCallSheet.hidden = true;
    stopRingtone();
}

async function endCurrentCall(status = "ended") {
    if (!state.currentCall) {
        return;
    }

    const callId = state.currentCall.id;
    await updateDoc(doc(db, "calls", callId), {
        status,
        endedAt: serverTimestamp()
    }).catch(() => {});
    await tearDownCallUi(status);
}

async function tearDownCallUi(status) {
    clearInterval(state.durationTimer);
    elements.callDuration.textContent = "00:00";
    closeCallOverlay();
    hideIncomingCall();
    cleanupPeerConnection();
    stopStream(state.localStream);
    stopStream(state.screenStream);
    state.localStream = null;
    state.screenStream = null;
    state.currentCall = null;
    state.micEnabled = true;
    state.cameraEnabled = true;
    state.speakerEnabled = true;
    syncCallControlLabels();
    await refreshPresenceProfile();
    updateCallUi({
        title: "No active call",
        status: status === "ended" ? "Call ended" : status,
        mode: "voice"
    });
    elements.callBannerTitle.textContent = "No active call";
    elements.callBannerText.textContent = "Choose a contact to place a voice or video call.";
    elements.remoteVideo.srcObject = null;
    elements.remoteAudio.srcObject = null;
    elements.localVideo.srcObject = null;
    elements.remotePlaceholder.hidden = false;
    elements.localPreviewFallback.hidden = false;
}

function cleanupPeerConnection() {
    state.callDocUnsub?.();
    state.callDocUnsub = null;
    state.remoteCandidatesUnsub?.();
    state.remoteCandidatesUnsub = null;
    if (state.peerConnection) {
        state.peerConnection.ontrack = null;
        state.peerConnection.onicecandidate = null;
        state.peerConnection.onconnectionstatechange = null;
        state.peerConnection.close();
    }
    state.peerConnection = null;
}

async function getLocalMedia({ audio, video }) {
    const constraints = {
        audio: audio ? buildAudioConstraints() : false,
        video: video ? buildVideoConstraints() : false
    };

    const stream = await navigator.mediaDevices.getUserMedia(constraints);
    state.localStream = stream;
    elements.localVideo.srcObject = stream;
    elements.localPreviewFallback.hidden = hasEnabledVideoTrack(stream);
    await refreshDeviceLists();
    return stream;
}

function buildAudioConstraints() {
    if (!state.selectedAudioInputId) {
        return true;
    }
    return {
        deviceId: { exact: state.selectedAudioInputId },
        echoCancellation: true,
        noiseSuppression: true
    };
}

function buildVideoConstraints() {
    if (!state.selectedVideoInputId) {
        return {
            width: { ideal: 1280 },
            height: { ideal: 720 },
            facingMode: state.mobile ? "user" : undefined
        };
    }
    return {
        deviceId: { exact: state.selectedVideoInputId },
        width: { ideal: 1280 },
        height: { ideal: 720 }
    };
}

async function replaceMediaTracks({ audio, video }) {
    if (!state.peerConnection) {
        return;
    }

    const nextStream = await navigator.mediaDevices.getUserMedia({
        audio: audio ? buildAudioConstraints() : false,
        video: video ? buildVideoConstraints() : false
    });

    const audioTrack = nextStream.getAudioTracks()[0];
    const videoTrack = nextStream.getVideoTracks()[0];
    const audioSender = state.peerConnection.getSenders().find((sender) => sender.track?.kind === "audio");
    const videoSender = state.peerConnection.getSenders().find((sender) => sender.track?.kind === "video");

    if (audio && audioSender && audioTrack) {
        await audioSender.replaceTrack(audioTrack);
        const currentTrack = getLocalAudioTrack();
        currentTrack?.stop();
        replaceTrackInLocalStream(currentTrack, audioTrack);
    }

    if (video) {
        if (videoSender && videoTrack) {
            await videoSender.replaceTrack(videoTrack);
        } else if (!videoSender && videoTrack) {
            state.peerConnection.addTrack(videoTrack, nextStream);
        }
        const currentTrack = getLocalVideoTrack();
        currentTrack?.stop();
        replaceTrackInLocalStream(currentTrack, videoTrack);
    }

    nextStream.getTracks().forEach((track) => {
        if (!state.localStream.getTracks().includes(track)) {
            track.stop();
        }
    });

    elements.localVideo.srcObject = state.localStream;
    elements.localPreviewFallback.hidden = hasEnabledVideoTrack(state.localStream);
}

function replaceTrackInLocalStream(oldTrack, newTrack) {
    if (!state.localStream) {
        state.localStream = new MediaStream();
    }
    if (oldTrack) {
        state.localStream.removeTrack(oldTrack);
    }
    if (newTrack) {
        state.localStream.addTrack(newTrack);
    }
}

function getLocalAudioTrack() {
    return state.localStream?.getAudioTracks()?.[0] || null;
}

function getLocalVideoTrack() {
    return state.localStream?.getVideoTracks()?.[0] || null;
}

function hasEnabledVideoTrack(stream) {
    return Boolean(stream?.getVideoTracks()?.some((track) => track.enabled));
}

function hasRemoteVideo() {
    return Boolean(state.remoteStream?.getVideoTracks()?.length);
}

function hasRemoteAudio() {
    return Boolean(state.remoteStream?.getAudioTracks()?.length);
}

function getPeerAvatar() {
    if (!state.currentCall) {
        return "--";
    }
    return state.currentCall.peerAvatar || state.currentCall.peerName.slice(0, 2).toUpperCase();
}

function toggleMute() {
    const audioTrack = getLocalAudioTrack();
    if (!audioTrack) {
        toast("No microphone track available");
        return;
    }
    state.micEnabled = !state.micEnabled;
    audioTrack.enabled = state.micEnabled;
    syncCallControlLabels();
}

async function toggleCamera() {
    if (!state.currentCall) {
        return;
    }

    const videoTrack = getLocalVideoTrack();
    if (!videoTrack && state.currentCall.mode === "voice") {
        toast("Video is only available in a video call");
        return;
    }

    if (!videoTrack && state.currentCall.mode === "video") {
        await replaceMediaTracks({ audio: false, video: true });
        state.cameraEnabled = true;
        syncCallControlLabels();
        return;
    }

    state.cameraEnabled = !state.cameraEnabled;
    videoTrack.enabled = state.cameraEnabled;
    elements.localPreviewFallback.hidden = state.cameraEnabled;
    syncCallControlLabels();
}

async function toggleSpeaker() {
    state.speakerEnabled = !state.speakerEnabled;
    await applyAudioOutputDevice();
    syncCallControlLabels();
}

async function applyAudioOutputDevice() {
    const targetDevice = state.speakerEnabled ? state.selectedAudioOutputId || "default" : "default";
    const mediaTarget = hasRemoteVideo() ? elements.remoteVideo : elements.remoteAudio;

    if (typeof mediaTarget.setSinkId !== "function") {
        toast("Audio output switching is not supported in this browser");
        return;
    }

    try {
        await mediaTarget.setSinkId(targetDevice);
    } catch (error) {
        toast(error.message || "Unable to switch speaker output");
    }
}

async function switchCameraInput() {
    const devices = state.localDevices.videoInputs;
    if (!devices.length) {
        toast("No camera devices found");
        return;
    }

    const currentIndex = Math.max(devices.findIndex((device) => device.deviceId === state.selectedVideoInputId), 0);
    const nextIndex = (currentIndex + 1) % devices.length;
    state.selectedVideoInputId = devices[nextIndex].deviceId;
    syncDeviceSelections();
    if (state.currentCall && !state.screenStream) {
        await replaceMediaTracks({ audio: false, video: true });
    }
}

async function toggleScreenShare() {
    if (!hasTier("premium")) {
        toast("Screen sharing is available on the Premium plan.");
        return;
    }
    if (!state.currentCall || state.currentCall.mode !== "video") {
        toast("Screen sharing is available during a video call");
        return;
    }

    if (state.screenStream) {
        await stopScreenShare();
        return;
    }

    try {
        state.screenStream = await navigator.mediaDevices.getDisplayMedia({
            video: true,
            audio: true
        });
        const screenTrack = state.screenStream.getVideoTracks()[0];
        const videoSender = state.peerConnection.getSenders().find((sender) => sender.track?.kind === "video");
        await videoSender.replaceTrack(screenTrack);
        const currentVideoTrack = getLocalVideoTrack();
        replaceTrackInLocalStream(currentVideoTrack, screenTrack);
        currentVideoTrack?.stop();
        elements.localVideo.srcObject = state.screenStream;
        elements.shareScreenBtn.textContent = "Stop Share";
        screenTrack.onended = () => {
            void stopScreenShare();
        };
        toast("Screen sharing started");
    } catch (error) {
        handleMediaError(error);
    }
}

async function stopScreenShare() {
    if (!state.screenStream) {
        return;
    }

    stopStream(state.screenStream);
    state.screenStream = null;
    elements.shareScreenBtn.textContent = "Share Screen";
    if (state.currentCall && state.currentCall.mode === "video") {
        await replaceMediaTracks({ audio: false, video: true });
    }
}

async function refreshDeviceLists() {
    if (!navigator.mediaDevices?.enumerateDevices) {
        return;
    }

    const devices = await navigator.mediaDevices.enumerateDevices();
    state.localDevices.audioInputs = devices.filter((device) => device.kind === "audioinput");
    state.localDevices.audioOutputs = devices.filter((device) => device.kind === "audiooutput");
    state.localDevices.videoInputs = devices.filter((device) => device.kind === "videoinput");

    if (!state.selectedAudioInputId && state.localDevices.audioInputs[0]) {
        state.selectedAudioInputId = state.localDevices.audioInputs[0].deviceId;
    }
    if (!state.selectedAudioOutputId && state.localDevices.audioOutputs[0]) {
        state.selectedAudioOutputId = state.localDevices.audioOutputs[0].deviceId;
    }
    if (!state.selectedVideoInputId && state.localDevices.videoInputs[0]) {
        state.selectedVideoInputId = state.localDevices.videoInputs[0].deviceId;
    }

    populateDeviceSelect(elements.audioInputSelect, state.localDevices.audioInputs, state.selectedAudioInputId, "Default microphone");
    populateDeviceSelect(elements.audioOutputSelect, state.localDevices.audioOutputs, state.selectedAudioOutputId, "System default");
    populateDeviceSelect(elements.videoInputSelect, state.localDevices.videoInputs, state.selectedVideoInputId, "Default camera");
    populateDeviceSelect(elements.callAudioInputSelect, state.localDevices.audioInputs, state.selectedAudioInputId, "Default microphone");
    populateDeviceSelect(elements.callAudioOutputSelect, state.localDevices.audioOutputs, state.selectedAudioOutputId, "System default");
    populateDeviceSelect(elements.callVideoInputSelect, state.localDevices.videoInputs, state.selectedVideoInputId, "Default camera");
}

function populateDeviceSelect(select, devices, selectedId, fallbackLabel) {
    if (!devices.length) {
        select.innerHTML = `<option value="">No devices found</option>`;
        return;
    }

    select.innerHTML = devices.map((device, index) => {
        const label = device.label || `${fallbackLabel} ${index + 1}`;
        return `<option value="${device.deviceId}" ${device.deviceId === selectedId ? "selected" : ""}>${label}</option>`;
    }).join("");
}

function syncDeviceSelections() {
    [
        [elements.audioInputSelect, state.selectedAudioInputId],
        [elements.callAudioInputSelect, state.selectedAudioInputId],
        [elements.audioOutputSelect, state.selectedAudioOutputId],
        [elements.callAudioOutputSelect, state.selectedAudioOutputId],
        [elements.videoInputSelect, state.selectedVideoInputId],
        [elements.callVideoInputSelect, state.selectedVideoInputId]
    ].forEach(([select, value]) => {
        if (select) {
            select.value = value;
        }
    });
}

function toggleNotifications() {
    state.notificationsOpen = !state.notificationsOpen;
    elements.notificationPanel.hidden = !state.notificationsOpen;
}

function closeNotifications() {
    state.notificationsOpen = false;
    elements.notificationPanel.hidden = true;
}

function markAllRead() {
    notifications.forEach((item) => {
        item.unread = false;
    });
    renderNotifications();
    syncNotificationBadges();
    toast("Notifications cleared");
}

function handleSearch(value) {
    const normalized = String(value || "");
    state.searchTerm = normalized.trim().toLowerCase();
    elements.searchInput.value = normalized;
    elements.mobileSearchMirror.value = normalized;
    renderConversations();
}

function switchConversation(chatId) {
    state.activeConversationId = chatId;
    const active = getActiveConversation();
    active.unread = 0;
    renderConversations();
    renderChat();
    updateDocumentTitle();
}

function updateNetworkLabel(isOnline) {
    elements.networkLabel.textContent = isOnline ? "Online" : "Offline";
    elements.networkLabel.className = `pill live-pill ${isOnline ? "live" : "danger"}`;
}

function handleOnline() {
    updateNetworkLabel(true);
    elements.offlineBanner.hidden = true;
    toast("Back online. Syncing queued actions.");
    void flushQueuedActions();
}

function handleOffline() {
    updateNetworkLabel(false);
    elements.offlineBanner.hidden = false;
}

function saveQueuedActions() {
    localStorage.setItem(storageKeys.queuedActions, JSON.stringify(state.retryQueue));
}

function loadQueuedActions() {
    try {
        return JSON.parse(localStorage.getItem(storageKeys.queuedActions) || "[]");
    } catch (error) {
        return [];
    }
}

function queueAction(action) {
    state.retryQueue.push(action);
    saveQueuedActions();
}

async function flushQueuedActions() {
    if (!state.retryQueue.length || !navigator.onLine) {
        return;
    }

    const remaining = [];
    for (const action of state.retryQueue) {
        try {
            if (action.type === "message") {
                deliverQueuedMessage(action.payload);
            }
        } catch (error) {
            remaining.push(action);
        }
    }
    state.retryQueue = remaining;
    saveQueuedActions();
}

function deliverQueuedMessage(payload) {
    const conversation = state.conversations[payload.conversationId];
    if (!conversation) {
        return;
    }
    conversation.messages.push({
        author: payload.author,
        avatar: payload.avatar,
        text: payload.text,
        time: payload.time,
        self: true
    });
    conversation.preview = payload.text;
    renderChat();
    renderConversations();
}

async function registerServiceWorker() {
    if (!("serviceWorker" in navigator)) {
        return;
    }
    try {
        state.swRegistration = await navigator.serviceWorker.register("./sw.js");
    } catch (error) {
        console.error("Service worker registration failed", error);
    }
}

async function loadPaymentHistory() {
    if (!state.authUser) {
        return;
    }

    try {
        const token = await getAuthToken();
        const response = await fetch(`${paymentApiBase}/payment-history`, {
            headers: {
                Authorization: `Bearer ${token}`
            }
        });
        const payload = await response.json();
        if (!response.ok) {
            throw new Error(payload.message || "Failed to load payment history");
        }

        elements.paymentHistory.innerHTML = payload.items?.length
            ? payload.items.map((item) => `
                <article class="payment-history-item">
                    <strong>${String(item.plan || "").toUpperCase()} - INR ${item.amount || 0}</strong>
                    <p>Status: ${item.status || "created"}</p>
                </article>
            `).join("")
            : `<article class="payment-history-item"><strong>No payments yet</strong><p>Your plan history will appear here.</p></article>`;
    } catch (error) {
        elements.paymentHistory.innerHTML = `<article class="payment-history-item"><strong>Payment history unavailable</strong><p>${error.message}</p></article>`;
    }
}

function updatePaymentPlanLabel() {
    const plan = elements.paymentPlanSelect.value;
    elements.paymentAmountLabel.textContent = plan === "99" ? "INR 99" : "INR 49";
}

function applyInitialPaymentState() {
    const url = new URL(window.location.href);
    if (url.searchParams.get("payment") === "success") {
        setPaymentState("success", "Payment already verified for this session.", "success");
        url.searchParams.delete("payment");
        window.history.replaceState({}, "", url.toString());
        return;
    }
    setPaymentState("idle", "Create an order and complete checkout. Verification runs automatically.");
}

function setPaymentState(stateName, message, resultClass = "", progressText = "Verifying payment...") {
    elements.paymentStateBadge.textContent = stateName.toUpperCase();
    elements.paymentStatusText.textContent = message;
    elements.paymentProgressText.textContent = progressText;
    const showProgress = stateName === "loading" || stateName === "pending";
    elements.paymentProgress.hidden = !showProgress;

    if (resultClass) {
        elements.paymentResult.hidden = false;
        elements.paymentResult.className = `payment-result ${resultClass}`;
        elements.paymentResult.textContent = message;
    } else {
        elements.paymentResult.hidden = true;
        elements.paymentResult.className = "payment-result";
        elements.paymentResult.textContent = "";
    }
}

async function getAuthToken() {
    if (!state.authUser) {
        throw new Error("Authentication not ready yet.");
    }
    return state.authUser.getIdToken();
}

async function startPaymentFlow() {
    if (!window.Razorpay) {
        toast("Razorpay checkout failed to load");
        return;
    }

    try {
        const token = await getAuthToken();
        const plan = elements.paymentPlanSelect.value;
        setPaymentState("loading", "Creating secure order...", "", "Creating order...");

        const response = await fetch(`${paymentApiBase}/create-order`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${token}`
            },
            body: JSON.stringify({ plan })
        });

        const payload = await response.json();
        if (!response.ok) {
            throw new Error(payload.message || "Unable to create order");
        }

        state.paymentOrderId = payload.orderId;
        setPaymentState("pending", "Checkout opened. Waiting for payment confirmation...", "pending", "Waiting for payment confirmation...");
        startPaymentStatusPolling(payload.orderId, token);

        const razorpay = new window.Razorpay({
            key: payload.keyId,
            amount: payload.amount,
            currency: payload.currency,
            name: "Z Chat",
            description: `Premium plan INR ${plan}`,
            order_id: payload.orderId,
            handler: async (checkoutResponse) => {
                await verifyPaymentOnServer(checkoutResponse, plan, token);
            },
            modal: {
                ondismiss: () => {
                    setPaymentState("pending", "Checkout closed. Waiting for webhook or retry.", "pending", "Waiting for payment confirmation...");
                }
            },
            theme: {
                color: "#00E5FF"
            }
        });

        razorpay.on("payment.failed", (event) => {
            stopPaymentStatusPolling();
            const message = event.error?.description || "Payment failed";
            setPaymentState("failed", message, "failed");
        });

        razorpay.open();
    } catch (error) {
        stopPaymentStatusPolling();
        setPaymentState("failed", error.message || "Payment initialization failed", "failed");
    }
}

async function verifyPaymentOnServer(checkoutResponse, plan, token) {
    setPaymentState("loading", "Verifying payment securely on the server...", "", "Verifying payment...");

    const response = await fetch(`${paymentApiBase}/verify-payment`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`
        },
        body: JSON.stringify({
            razorpay_payment_id: checkoutResponse.razorpay_payment_id,
            razorpay_order_id: checkoutResponse.razorpay_order_id,
            razorpay_signature: checkoutResponse.razorpay_signature,
            plan
        })
    });

    const payload = await response.json();
    if (!response.ok) {
        throw new Error(payload.message || "Payment verification failed");
    }

    stopPaymentStatusPolling();
    await ensureUserProfile();
    await loadPaymentHistory();
    renderCurrentTier();
    setPaymentState("success", "Payment verified. Redirecting...", "success");
    schedulePaymentRedirect();
}

function startPaymentStatusPolling(orderId, token) {
    stopPaymentStatusPolling();
    state.paymentPollTimer = window.setInterval(async () => {
        try {
            const response = await fetch(`${paymentApiBase}/payment-status/${orderId}`, {
                headers: {
                    Authorization: `Bearer ${token}`
                }
            });
            const payload = await response.json();
            if (!response.ok) {
                throw new Error(payload.message || "Failed to fetch payment status");
            }

            const paymentStatus = String(payload.status || "").toLowerCase();
            if (["verified", "captured", "paid"].includes(paymentStatus)) {
                stopPaymentStatusPolling();
                await ensureUserProfile();
                await loadPaymentHistory();
                renderCurrentTier();
                setPaymentState("success", "Payment verified by webhook. Redirecting...", "success");
                schedulePaymentRedirect();
                return;
            }

            if (["failed", "refunded"].includes(paymentStatus)) {
                stopPaymentStatusPolling();
                setPaymentState("failed", `Payment ${paymentStatus}`, "failed");
                return;
            }

            setPaymentState("pending", `Payment status: ${payload.status || "pending"}`, "pending", "Waiting for payment confirmation...");
        } catch (error) {
            stopPaymentStatusPolling();
            setPaymentState("failed", error.message || "Unable to confirm payment status", "failed");
        }
    }, 3000);
}

function stopPaymentStatusPolling() {
    if (state.paymentPollTimer) {
        clearInterval(state.paymentPollTimer);
        state.paymentPollTimer = null;
    }
}

function schedulePaymentRedirect() {
    window.setTimeout(() => {
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.set("payment", "success");
        window.location.href = nextUrl.toString();
    }, 1800);
}

function playRingtone() {
    stopRingtone();
    if (state.notificationsMuted) {
        return;
    }
    state.ringtoneTimer = window.setInterval(() => {
        const audioContext = new window.AudioContext();
        const oscillator = audioContext.createOscillator();
        const gain = audioContext.createGain();
        oscillator.type = "sine";
        oscillator.frequency.setValueAtTime(660, audioContext.currentTime);
        gain.gain.setValueAtTime(0.02, audioContext.currentTime);
        oscillator.connect(gain);
        gain.connect(audioContext.destination);
        oscillator.start();
        oscillator.stop(audioContext.currentTime + 0.18);
        oscillator.onended = () => {
            audioContext.close().catch(() => {});
        };
    }, 1400);
}

function stopRingtone() {
    clearInterval(state.ringtoneTimer);
    state.ringtoneTimer = null;
}

function sendMessage() {
    const value = elements.messageInput.value.trim();
    if (!value) {
        toast("Write a message first");
        return;
    }

    const conversation = getActiveConversation();
    const me = getActiveProfile();
    const payload = {
        author: me.name,
        avatar: me.avatar,
        text: value,
        time: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        self: true,
        conversationId: conversation.id
    };

    if (!navigator.onLine) {
        queueAction({
            type: "message",
            payload
        });
        conversation.preview = `${value} (queued)`;
        elements.messageInput.value = "";
        autoResizeInput();
        renderConversations();
        renderChat();
        toast("Message queued for sync");
        return;
    }

    conversation.messages.push(payload);
    conversation.preview = value;
    elements.messageInput.value = "";
    autoResizeInput();
    renderChat();
    renderConversations();
    updateDocumentTitle();
}

function autoResizeInput() {
    elements.messageInput.style.height = "auto";
    elements.messageInput.style.height = `${elements.messageInput.scrollHeight}px`;
}

function handleTool(action) {
    const actions = {
        emoji: () => appendToComposer(":) "),
        gif: () => toast("GIF tray opened"),
        media: () => toast("Media tray opened"),
        scanner: openScanner,
        voice: () => toast("Voice note recording started")
    };
    actions[action]?.();
}

function appendToComposer(value) {
    elements.messageInput.value = `${elements.messageInput.value}${value}`;
    autoResizeInput();
    elements.messageInput.focus();
}

function createChat() {
    const id = `contact-${Date.now()}`;
    state.conversations[id] = {
        id,
        name: "New Contact",
        avatar: "NC",
        tag: "Custom chat",
        category: "friends",
        preview: "Fresh conversation created.",
        time: "Now",
        unread: 0,
        messages: [
            { author: "Z Chat", avatar: "ZT", text: "This conversation is ready.", time: "Now", self: false }
        ]
    };
    state.activeConversationId = id;
    renderConversations();
    renderChat();
    toast("New chat created");
}

function openDrawer() {
    state.drawerOpen = true;
    elements.mobileDrawer.classList.add("open");
    elements.mobileDrawer.setAttribute("aria-hidden", "false");
    elements.drawerBackdrop.hidden = false;
}

function closeDrawer() {
    state.drawerOpen = false;
    elements.mobileDrawer.classList.remove("open");
    elements.mobileDrawer.setAttribute("aria-hidden", "true");
    elements.drawerBackdrop.hidden = true;
}

function openScanner() {
    state.scannerOpen = true;
    elements.scannerModal.hidden = false;
}

function closeScanner() {
    state.scannerOpen = false;
    elements.scannerModal.hidden = true;
}

function captureScan() {
    elements.scannerStatus.textContent = "Scan captured. Shared as a smart note in chat.";
    toast("Scanner capture complete");
}

function pulsePanel(selector, message) {
    const panel = document.querySelector(selector);
    if (!panel) {
        return;
    }
    panel.classList.remove("pulse");
    void panel.offsetWidth;
    panel.classList.add("pulse");
    toast(message);
}

function syncCallControlLabels() {
    elements.muteToggleBtn.textContent = state.micEnabled ? "Mute" : "Unmute";
    elements.cameraToggleBtn.textContent = state.cameraEnabled ? "Camera On" : "Camera Off";
    elements.speakerToggleBtn.textContent = state.speakerEnabled ? "Speaker On" : "Speaker Off";
    elements.shareScreenBtn.textContent = state.screenStream ? "Stop Share" : "Share Screen";

    elements.muteToggleBtn.classList.toggle("active", !state.micEnabled);
    elements.cameraToggleBtn.classList.toggle("active", state.cameraEnabled);
    elements.speakerToggleBtn.classList.toggle("active", state.speakerEnabled);
    elements.shareScreenBtn.classList.toggle("active", Boolean(state.screenStream));
}

function handleResize() {
    const isMobile = window.innerWidth <= 768;
    if (state.mobile !== isMobile) {
        state.mobile = isMobile;
        closeDrawer();
        closeNotifications();
        toast(isMobile ? "Switched to mobile layout" : "Switched to desktop layout");
    }
}

function handleMediaError(error) {
    console.error(error);
    if (error?.name === "NotAllowedError") {
        toast("Permission denied for camera or microphone");
        return;
    }
    if (error?.name === "NotFoundError") {
        toast("Required media device not found");
        return;
    }
    toast(error?.message || "Call setup failed");
}

function stopStream(stream) {
    stream?.getTracks()?.forEach((track) => track.stop());
}

function toast(message) {
    const item = document.createElement("div");
    item.className = "toast";
    item.textContent = message;
    elements.toastStack.appendChild(item);
    window.setTimeout(() => item.remove(), 2600);
}

init().catch((error) => {
    console.error(error);
    toast(error.message || "Failed to initialize app");
});

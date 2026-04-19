function registerSocketServer(io, state) {
    io.on("connection", (socket) => {
        socket.emit("connection", { id: socket.id });

        socket.on("user:online", ({ userId }) => {
            const user = state.users.find((item) => item.id === userId);
            if (user) {
                user.status = "online";
            }
            io.emit("user:online", { userId });
        });

        socket.on("user:offline", ({ userId }) => {
            const user = state.users.find((item) => item.id === userId);
            if (user) {
                user.status = "offline";
            }
            io.emit("user:offline", { userId });
        });

        socket.on("message:send", (payload) => io.emit("message:receive", payload));
        socket.on("message:delivered", (payload) => io.emit("message:delivered", payload));
        socket.on("message:read", (payload) => io.emit("message:read", payload));
        socket.on("typing:start", (payload) => socket.broadcast.emit("typing:start", payload));
        socket.on("typing:stop", (payload) => socket.broadcast.emit("typing:stop", payload));
        socket.on("chat:new", (payload) => io.emit("chat:new", payload));
        socket.on("disconnect", () => {
            io.emit("disconnect", { id: socket.id });
        });
    });
}

module.exports = { registerSocketServer };

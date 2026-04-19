export const QUIZ_BANK = [
  {
    question: "Which language runs in a web browser?",
    options: ["Python", "Java", "JavaScript", "C++"],
    answer: 2
  },
  {
    question: "Which planet is known as the Red Planet?",
    options: ["Venus", "Mars", "Mercury", "Jupiter"],
    answer: 1
  },
  {
    question: "What does FPS usually mean in gaming?",
    options: ["Frames Per Second", "Fast Player Setup", "File Processing Speed", "Final Play Session"],
    answer: 0
  },
  {
    question: "Which study method spaces review over time?",
    options: ["Cramming", "Spaced repetition", "Guessing", "Speed reading"],
    answer: 1
  }
];

export function createTicTacToeState() {
  return {
    board: Array(9).fill(""),
    current: "X",
    winner: null,
    moves: 0
  };
}

export function applyTicMove(game, index) {
  const next = {
    ...game,
    board: [...game.board]
  };
  if (next.winner || next.board[index]) {
    return next;
  }

  next.board[index] = next.current;
  next.moves += 1;
  next.winner = detectWinner(next.board);
  if (!next.winner) {
    next.current = next.current === "X" ? "O" : "X";
  }
  return next;
}

function detectWinner(board) {
  const lines = [
    [0, 1, 2],
    [3, 4, 5],
    [6, 7, 8],
    [0, 3, 6],
    [1, 4, 7],
    [2, 5, 8],
    [0, 4, 8],
    [2, 4, 6]
  ];

  for (const [a, b, c] of lines) {
    if (board[a] && board[a] === board[b] && board[a] === board[c]) {
      return board[a];
    }
  }

  if (board.every(Boolean)) {
    return "draw";
  }
  return null;
}

export function playRockPaperScissors(move) {
  const botMoves = ["rock", "paper", "scissors"];
  const botMove = botMoves[Math.floor(Math.random() * botMoves.length)];
  let outcome = "draw";

  if (
    (move === "rock" && botMove === "scissors") ||
    (move === "paper" && botMove === "rock") ||
    (move === "scissors" && botMove === "paper")
  ) {
    outcome = "win";
  } else if (move !== botMove) {
    outcome = "lose";
  }

  return { move, botMove, outcome };
}

export function getDailyChallengeBlueprint(dateKey) {
  const numericSeed = Array.from(dateKey).reduce((sum, char) => sum + char.charCodeAt(0), 0);
  const gamingTarget = 1 + (numericSeed % 2);
  const aiTarget = 1 + ((numericSeed + 1) % 2);
  return [
    {
      id: "chat",
      title: "Chat Sprint",
      description: "Send 3 messages today.",
      target: 3,
      points: 15,
      metric: "messagesSent"
    },
    {
      id: "ai",
      title: "AI Check-in",
      description: `Use Z AI ${aiTarget} time${aiTarget > 1 ? "s" : ""}.`,
      target: aiTarget,
      points: 20,
      metric: "aiUses"
    },
    {
      id: "games",
      title: "Game On",
      description: `Finish ${gamingTarget} mini game${gamingTarget > 1 ? "s" : ""}.`,
      target: gamingTarget,
      points: 20,
      metric: "gamesPlayed"
    },
    {
      id: "study",
      title: "Study Focus",
      description: "Join a study room or answer 1 quiz question correctly.",
      target: 1,
      points: 25,
      metric: "studyActions"
    }
  ];
}

export function computeChallengeStats(challengeDoc) {
  const metrics = challengeDoc?.metrics || {};
  const blueprint = getDailyChallengeBlueprint(challengeDoc?.dateKey || new Date().toISOString().slice(0, 10));
  const completed = blueprint.filter((item) => (metrics[item.metric] || 0) >= item.target);
  const points = completed.reduce((sum, item) => sum + item.points, 0);
  return {
    blueprint,
    completedCount: completed.length,
    points
  };
}

// index.js
import express from "express";
import pg from "pg";
import dotenv from "dotenv";
import cors from "cors";
import geoip from "geoip-lite";

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

// ---- Small util to wrap async route handlers ----
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// ---- DB ----
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }, // required for Neon (or many hosted PGs)
  // Fail fast rather than hanging forever
  connectionTimeoutMillis: 10_000, // 10s
  idleTimeoutMillis: 30_000,
  max: 10,
});

// Log pool-level errors (lost client, etc.) without crashing the process
pool.on("error", (err) => {
  console.error("[pg pool] idle client error:", err);
});

// Track DB readiness (for health checks / gating features if you want)
let dbReady = false;

// Retry ensureSchema with backoff, never crash app
async function ensureSchemaWithRetry() {
  let attempt = 0;
  // max delay ~ 60s
  const maxDelay = 60_000;

  while (true) {
    try {
      await ensureSchema();
      dbReady = true;
      console.log("[db] schema ready");
      return;
    } catch (e) {
      dbReady = false;
      attempt += 1;
      const delay = Math.min(1000 * 2 ** Math.min(attempt, 6), maxDelay);
      console.error(`[db] ensureSchema failed (attempt ${attempt}):`, e?.message || e);
      console.error(`[db] retrying in ${Math.round(delay / 1000)}s...`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

async function ensureSchema() {
  // try a quick connection to fail fast if unreachable
  const client = await pool.connect();
  try {
    await client.query(`
      create table if not exists users (
        user_id   text primary key,
        username  text not null,
        country   text,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now()
      );

      create table if not exists leaderboards (
        id         text primary key,
        name       text not null,
        created_at timestamptz not null default now()
      );

      create table if not exists scores (
        id             bigserial primary key,
        leaderboard_id text not null references leaderboards(id) on delete cascade,
        user_id        text not null references users(user_id) on delete cascade,
        score          integer not null,
        time           integer,
        achieved_at    timestamptz not null default now(),
        created_at     timestamptz not null default now()
      );

      create index if not exists idx_scores_lb_score on scores(leaderboard_id, score desc, achieved_at asc);
      create index if not exists idx_scores_user      on scores(user_id);
    `);
  } finally {
    client.release();
  }
}

// Start schema initialization (non-blocking for the web server)
ensureSchemaWithRetry(); // NOTE: no process.exit on failure

// ---- Geo helpers ----
function getCountryFromRequest(req) {
  const ip =
    req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
    req.socket?.remoteAddress ||
    null;

  if (!ip || ip === "::1" || ip === "127.0.0.1") return "??";
  const geo = geoip.lookup(ip);
  if (geo && geo.country) return geo.country; // ISO2
  return "??";
}

// If you want to keep a stub for local testing instead of real IP lookup:
function getUserCountry(_req) {
  const countries = ['FR', 'US', 'DE', 'IT', 'ES', 'GB', 'CA', 'JP', 'AU', 'BR'];
  return countries[Math.floor(Math.random() * countries.length)];
}

// ---- Period helpers ----
function dayWindowClause(dateStr) {
  let startMs;
  if (dateStr) {
    const d = new Date(`${dateStr}T00:00:00Z`);
    startMs = isNaN(d.getTime())
      ? Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), new Date().getUTCDate())
      : d.getTime();
  } else {
    const now = new Date();
    startMs = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  }
  const endMs = startMs + 24 * 60 * 60 * 1000;
  const startSec = Math.floor(startMs / 1000);
  const endSec = Math.floor(endMs / 1000);
  return `s.achieved_at >= to_timestamp(${startSec}) and s.achieved_at < to_timestamp(${endSec})`;
}

function weeklyWhereClause() {
  return `
    s.achieved_at >= date_trunc('week', now() at time zone 'utc')
    and s.achieved_at <  date_trunc('week', now() at time zone 'utc') + interval '1 week'
  `;
}

// ---- Core query (one best row per user, then rank) ----
async function getBoard({ leaderboardId, period, limit, includePlayer, userId, date }) {
  const wherePeriod =
    period === "daily"
      ? dayWindowClause(date)
      : period === "weekly"
      ? weeklyWhereClause()
      : "true"; // all-time

  const base = `
    with best_per_user as (
      select *
      from (
        select
          s.id, s.leaderboard_id, s.user_id, s.time, s.score,
          s.achieved_at as timestamp,
          row_number() over (
            partition by s.user_id
            order by s.score desc, s.achieved_at asc, s.id asc
          ) as rn
        from scores s
        where s.leaderboard_id = $1 and ${wherePeriod}
      ) t
      where rn = 1
    ),
    ranked as (
      select
        bpu.id, bpu.time, bpu.score, bpu.timestamp, bpu.user_id,
        u.username, u.country,
        dense_rank() over (
          order by bpu.score desc, bpu.timestamp asc, bpu.user_id asc
        ) as rank
      from best_per_user bpu
      join users u on u.user_id = bpu.user_id
    )
    select id, rank, username, country, time, score, timestamp, user_id
    from ranked
  `;

  const topRows = (
    await pool.query(
      `${base} where rank <= $2 order by rank asc, timestamp asc`,
      [leaderboardId, limit]
    )
  ).rows;

  let rows = topRows;

  if (includePlayer && userId) {
    const me = (
      await pool.query(`${base} where user_id = $2 limit 1`, [leaderboardId, userId])
    ).rows;
    if (me.length && !rows.some((r) => r.user_id === userId)) {
      rows = [...rows, me[0]].slice(0, limit + 1);
    }
  }

  return rows.map((r) => ({
    id: r.id,
    rank: r.rank,
    username: r.username,
    country: r.country ?? null,
    time: r.time ?? null,
    score: r.score,
    timestamp: r.timestamp,
    isplayer: userId ? r.user_id === userId : false,
  }));
}

// ---- Health ----
app.get("/healthz", (req, res) => {
  res.json({
    ok: true,
    dbReady,
  });
});

// Root health (kept as-is)
app.get("/", (_req, res) => res.send("OK"));

// ---- Routes (all wrapped with asyncHandler) ----

// 1) Player highscore (all-time)
app.get("/api/leaderboards/:leaderboardId/players/:userId/highscore", asyncHandler(async (req, res) => {
  const { leaderboardId, userId } = req.params;

  const q = `
    with best as (
      select s.*
      from scores s
      where s.leaderboard_id = $1 and s.user_id = $2
      order by s.score desc, s.achieved_at asc
      limit 1
    ),
    ranked as (
      select
        s.id,
        dense_rank() over (
          partition by s.leaderboard_id
          order by s.score desc, s.achieved_at asc, s.user_id asc
        ) as rank
      from scores s
      where s.leaderboard_id = $1
    )
    select
      b.id,
      r.rank,
      u.username,
      u.country,
      b.time,
      b.score,
      b.achieved_at as timestamp
    from best b
    join ranked r on r.id = b.id
    join users u on u.user_id = b.user_id
  `;
  const { rows } = await pool.query(q, [leaderboardId, userId]);
  if (!rows.length) return res.status(404).json({ error: "Not found" });

  const row = rows[0];
  res.json({
    id: row.id,
    rank: row.rank,
    username: row.username,
    country: row.country ?? null,
    time: row.time ?? null,
    score: row.score,
    timestamp: row.timestamp,
    isplayer: true,
  });
}));

// 2) Daily (by date; defaults to today). Returns date in dd-mm-yyyy
app.get("/api/leaderboards/:leaderboardId/daily", asyncHandler(async (req, res) => {
  const { leaderboardId } = req.params;
  const limit = Math.max(1, parseInt(req.query.limit || "50", 10));
  const includePlayer = String(req.query.includePlayer || "false").toLowerCase() === "true";
  const userId = req.query.userId || "";
  const date = req.query.date || ""; // YYYY-MM-DD

  const scores = await getBoard({
    leaderboardId,
    period: "daily",
    limit,
    includePlayer,
    userId,
    date,
  });

  const baseDate = date ? new Date(`${date}T00:00:00Z`) : new Date();
  const formattedDate = [
    String(baseDate.getUTCDate()).padStart(2, "0"),
    String(baseDate.getUTCMonth() + 1).padStart(2, "0"),
    baseDate.getUTCFullYear(),
  ].join("-");

  res.json({
    id: leaderboardId,
    name: leaderboardId,
    scores,
    date: formattedDate,
  });
}));

// 3) Weekly (current week)
app.get("/api/leaderboards/:leaderboardId/weekly", asyncHandler(async (req, res) => {
  const { leaderboardId } = req.params;
  const limit = Math.max(1, parseInt(req.query.limit || "50", 10));
  const includePlayer = String(req.query.includePlayer || "false").toLowerCase() === "true";
  const userId = req.query.userId || "";

  const scores = await getBoard({
    leaderboardId,
    period: "weekly",
    limit,
    includePlayer,
    userId,
  });

  res.json({
    id: leaderboardId,
    name: leaderboardId,
    scores,
  });
}));

// 4) All-time
app.get("/api/leaderboards/:leaderboardId/all-time", asyncHandler(async (req, res) => {
  const { leaderboardId } = req.params;
  const limit = Math.max(1, parseInt(req.query.limit || "50", 10));
  const includePlayer = String(req.query.includePlayer || "false").toLowerCase() === "true";
  const userId = req.query.userId || "";

  const scores = await getBoard({
    leaderboardId,
    period: "all",
    limit,
    includePlayer,
    userId,
  });

  res.json({
    id: leaderboardId,
    name: leaderboardId,
    scores,
  });
}));

// 5) Insert new score (creates leaderboard if missing)
app.post("/api/leaderboards/:leaderboardId/scores", asyncHandler(async (req, res) => {
  const { leaderboardId } = req.params;
  const { userId, score, time, timestamp } = req.body || {};

  if (!userId || typeof score !== "number") {
    return res.status(400).json({ error: "userId and numeric score are required" });
  }

  await pool.query(
    `insert into leaderboards (id, name)
     values ($1, $1)
     on conflict (id) do nothing`,
    [leaderboardId]
  );

  const inserted = await pool.query(
    `insert into scores (leaderboard_id, user_id, score, time, achieved_at)
     values ($1, $2, $3, $4, coalesce($5::timestamptz, now()))
     returning id, user_id, time, score, achieved_at as timestamp`,
    [leaderboardId, userId, score, time ?? null, timestamp ?? null]
  );

  const ins = inserted.rows[0];

  const rankQ = `
    with ranked as (
      select
        s.id,
        dense_rank() over (
          partition by s.leaderboard_id
          order by s.score desc, s.achieved_at asc, s.user_id asc
        ) as rank
      from scores s
      where s.leaderboard_id = $1
    )
    select r.rank, u.username, u.country
    from ranked r
    join scores s on s.id = r.id
    join users u on u.user_id = s.user_id
    where r.id = $2
  `;
  const { rows: r } = await pool.query(rankQ, [leaderboardId, ins.id]);

  res.status(201).json({
    id: ins.id,
    rank: r[0]?.rank ?? 0,
    username: r[0]?.username ?? null,
    country: r[0]?.country ?? null,
    time: ins.time ?? null,
    score: ins.score,
    timestamp: ins.timestamp,
    isplayer: true,
  });
}));

// ---- Users ----

// Create or update user (never return userId). Country via IP (ISO2) stored.
app.post("/api/users", asyncHandler(async (req, res) => {
  const { userId, username } = req.body || {};
  if (!userId || !username) {
    return res.status(400).json({ error: "userId and username are required" });
  }

  const trimmed = username.trim();
  if (trimmed.length < 2 || trimmed.length > 20) {
    return res
      .status(400)
      .json({ error: "username must be between 2 and 20 characters after trimming" });
  }

  const country = getUserCountry(req); // or use getCountryFromRequest(req) in prod
  console.log(`User ${userId} from country: ${country}`);
  await pool.query(
    `insert into users (user_id, username, country, created_at, updated_at)
     values ($1, $2, $3, now(), now())
     on conflict (user_id) do update
       set username = excluded.username,
           country = excluded.country,
           updated_at = now()`,
    [userId, trimmed, country]
  );

  res.status(201).json({ username: trimmed, country });
}));

// Get user data (never return userId)
app.get("/api/users/:userId", asyncHandler(async (req, res) => {
  const { userId } = req.params;
  const { rows } = await pool.query(
    `select username, country, created_at, updated_at
     from users
     where user_id = $1`,
    [userId]
  );

  if (!rows.length) return res.status(404).json({ error: "User not found" });

  const { username, country, created_at, updated_at } = rows[0];
  res.json({ username, country, created_at, updated_at });
}));

// Update username only
app.put("/api/users/:userId", asyncHandler(async (req, res) => {
  const { userId } = req.params;
  const { username } = req.body || {};

  if (!username) {
    return res.status(400).json({ error: "username is required" });
  }

  const trimmed = username.trim();
  if (trimmed.length < 2) {
    return res.status(400).json({ error: "username must be at least 2 characters after trimming" });
  }

  const { rowCount } = await pool.query(
    `update users
     set username = $2, updated_at = now()
     where user_id = $1`,
    [userId, trimmed]
  );

  if (rowCount === 0) {
    return res.status(404).json({ error: "User not found" });
  }

  res.json({ username: trimmed });
}));

// ---- Global error handler (keeps process alive & returns JSON) ----
app.use((err, req, res, _next) => {
  // You can special-case PG timeouts to return 503
  const isTimeout =
    err?.code === "ETIMEDOUT" ||
    /timeout/i.test(err?.message || "") ||
    err?.name === "AggregateError";

  const status = isTimeout ? 503 : 500;

  console.error("[error]", {
    path: req.path,
    method: req.method,
    message: err?.message,
    code: err?.code,
    stack: err?.stack,
  });

  res.status(status).json({
    error: isTimeout ? "Database temporarily unavailable" : "Internal Server Error",
    details: process.env.NODE_ENV === "production" ? undefined : err?.message,
  });
});

// ---- Last-resort guards (do NOT exit) ----
process.on("unhandledRejection", (reason) => {
  console.error("[unhandledRejection]", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[uncaughtException]", err);
  // Intentionally do NOT process.exit here; let the server keep running.
});

// ---- Boot ----
const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Leaderboard API running on :${port}`));

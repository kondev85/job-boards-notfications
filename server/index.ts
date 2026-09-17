import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import { Pool } from "pg";
import { getAuth, clerkClient, clerkMiddleware } from "@clerk/express";
import { publishableKeyFromHost } from "@clerk/shared/keys";
import { clerkProxyMiddleware, CLERK_PROXY_PATH, getClerkProxyHost } from "./middlewares/clerkProxyMiddleware";
import multer from "multer";
import { parse } from "csv-parse/sync";
import { spawn } from "node:child_process";
import { createServer as createHttpServer } from "node:http";
import path from "node:path";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const SEARCH_WORKER_STALE_MS = Number(process.env.SEARCH_WORKER_STALE_MS || 10 * 60 * 1000);
const DAILY_IMPORT_STALE_MS = Number(process.env.DAILY_IMPORT_STALE_MS || 15 * 60 * 1000);
const pinnedRecommendationFloor = Math.max(
  0,
  Math.min(100, Number(process.env.PINNED_RECOMMENDATION_FLOOR || 70)),
);
const app = express();
app.use(CLERK_PROXY_PATH, clerkProxyMiddleware());
app.use(cors({ credentials: true, origin: true }));
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

type HealthStatus = "ok" | "degraded" | "down";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isStale(value: unknown, thresholdMs: number): boolean {
  if (!value) return true;
  const timestamp = new Date(String(value)).getTime();
  return !Number.isFinite(timestamp) || Date.now() - timestamp > thresholdMs;
}

async function healthSnapshot() {
  const checkedAt = new Date().toISOString();
  const snapshot: {
    status: HealthStatus;
    checked_at: string;
    web: { status: "up" };
    database: {
      status: HealthStatus;
      schema: "ready" | "not_ready" | "unknown";
      latency_ms?: number;
      missing_tables?: string[];
      error?: string;
    };
    daily_import: {
      status: string;
      latest_run_id: number | null;
      latest_status: string | null;
      latest_started_at: unknown;
      latest_updated_at: unknown;
      latest_completed_at: unknown;
      heartbeat_at: unknown;
      board_count: number | null;
      failed_board_count: number | null;
      last_successful_at: unknown;
    };
    search_worker: {
      status: "idle" | "running" | "stale" | "never" | "unknown";
      active_runs: number;
      stale_runs: number;
      latest_run_id: number | null;
      latest_status: string | null;
      latest_updated_at: unknown;
      last_successful_at: unknown;
    };
    last_successful_job_at: unknown;
  } = {
    status: "degraded",
    checked_at: checkedAt,
    web: { status: "up" },
    database: { status: "down", schema: "unknown" },
    daily_import: {
      status: "unknown",
      latest_run_id: null,
      latest_status: null,
      latest_started_at: null,
      latest_updated_at: null,
      latest_completed_at: null,
      heartbeat_at: null,
      board_count: null,
      failed_board_count: null,
      last_successful_at: null,
    },
    search_worker: {
      status: "unknown",
      active_runs: 0,
      stale_runs: 0,
      latest_run_id: null,
      latest_status: null,
      latest_updated_at: null,
      last_successful_at: null,
    },
    last_successful_job_at: null,
  };

  const databaseStartedAt = Date.now();
  try {
    await pool.query("SELECT 1");
    snapshot.database.status = "ok";
    snapshot.database.latency_ms = Date.now() - databaseStartedAt;
  } catch (error) {
    snapshot.database.error = errorMessage(error);
    console.error("[health] database check failed:", snapshot.database.error);
    return snapshot;
  }

  try {
    const schema = await pool.query<{ table_name: string | null }>(
      `SELECT table_name
       FROM unnest(ARRAY[
         'users',
         'search_runs',
         'daily_import_runs',
         'daily_import_run_boards'
       ]::text[]) AS required(table_name)
       WHERE to_regclass('public.' || table_name) IS NULL`,
    );
    const missingTables = schema.rows.map((row) => row.table_name).filter(Boolean) as string[];
    if (missingTables.length) {
      snapshot.database.status = "degraded";
      snapshot.database.schema = "not_ready";
      snapshot.database.missing_tables = missingTables;
      console.error("[health] database schema is not ready; missing:", missingTables.join(", "));
      return snapshot;
    }
    snapshot.database.schema = "ready";

    const [dailyLatest, dailySuccess, searchLatest, searchState, searchSuccess] = await Promise.all([
      pool.query(
        `SELECT import_run_id, status, started_at, updated_at, completed_at,
                heartbeat_at, board_count, failed_board_count
         FROM daily_import_runs
         ORDER BY started_at DESC
         LIMIT 1`,
      ),
      pool.query(
        `SELECT max(completed_at) AS last_successful_at
         FROM daily_import_runs
         WHERE status IN ('completed', 'completed_with_errors')`,
      ),
      pool.query(
        `SELECT run_id, status, progress, error, created_at, updated_at
         FROM search_runs
         ORDER BY created_at DESC
         LIMIT 1`,
      ),
      pool.query(
        `SELECT
           count(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active_runs,
           count(*) FILTER (
             WHERE status IN ('queued', 'running')
               AND updated_at < now() - make_interval(secs => $1)
           )::int AS stale_runs
         FROM search_runs`,
        [SEARCH_WORKER_STALE_MS / 1000],
      ),
      pool.query(
        `SELECT max(updated_at) AS last_successful_at
         FROM search_runs
         WHERE status IN ('completed', 'completed_with_warnings')`,
      ),
    ]);

    const latestDaily = dailyLatest.rows[0];
    const latestSearch = searchLatest.rows[0];
    const searchCounts = searchState.rows[0] || { active_runs: 0, stale_runs: 0 };
    const dailyStatus = latestDaily?.status || null;
    const dailyStale = dailyStatus === "running" && isStale(latestDaily.heartbeat_at, DAILY_IMPORT_STALE_MS);
    const activeRuns = Number(searchCounts.active_runs || 0);
    const staleRuns = Number(searchCounts.stale_runs || 0);

    snapshot.daily_import = {
      status: dailyStale ? "stale" : dailyStatus || "never",
      latest_run_id: latestDaily ? Number(latestDaily.import_run_id) : null,
      latest_status: dailyStatus,
      latest_started_at: latestDaily?.started_at || null,
      latest_updated_at: latestDaily?.updated_at || null,
      latest_completed_at: latestDaily?.completed_at || null,
      heartbeat_at: latestDaily?.heartbeat_at || null,
      board_count: latestDaily ? Number(latestDaily.board_count) : null,
      failed_board_count: latestDaily ? Number(latestDaily.failed_board_count) : null,
      last_successful_at: dailySuccess.rows[0]?.last_successful_at || null,
    };
    snapshot.search_worker = {
      status:
        activeRuns === 0
          ? latestSearch
            ? "idle"
            : "never"
          : staleRuns > 0
            ? "stale"
            : "running",
      active_runs: activeRuns,
      stale_runs: staleRuns,
      latest_run_id: latestSearch ? Number(latestSearch.run_id) : null,
      latest_status: latestSearch?.status || null,
      latest_updated_at: latestSearch?.updated_at || null,
      last_successful_at: searchSuccess.rows[0]?.last_successful_at || null,
    };

    const successfulJobTimestamps = [
      snapshot.daily_import.last_successful_at,
      snapshot.search_worker.last_successful_at,
    ]
      .filter(Boolean)
      .map((value) => new Date(String(value)).getTime())
      .filter(Number.isFinite);
    snapshot.last_successful_job_at = successfulJobTimestamps.length
      ? new Date(Math.max(...successfulJobTimestamps)).toISOString()
      : null;
    snapshot.status =
      snapshot.database.status === "ok" &&
      !dailyStale &&
      staleRuns === 0
        ? "ok"
        : "degraded";
  } catch (error) {
    snapshot.database.status = "degraded";
    snapshot.database.schema = "not_ready";
    snapshot.database.error = errorMessage(error);
    console.error("[health] operational status query failed:", snapshot.database.error);
  }

  return snapshot;
}

async function sendHealth(_req: Request, res: Response) {
  const snapshot = await healthSnapshot();
  res.status(snapshot.status === "ok" ? 200 : 503).json(snapshot);
}

// Liveness remains available even when PostgreSQL is unavailable. Readiness and
// the detailed operational view report the dependency state separately.
app.get("/healthz", (_req, res) => {
  res.status(200).json({ status: "ok", checked_at: new Date().toISOString(), web: { status: "up" } });
});
app.get("/readyz", sendHealth);
app.get("/api/health", sendHealth);

app.use(clerkMiddleware((req) => ({
  publishableKey: publishableKeyFromHost(getClerkProxyHost(req) ?? "", process.env.CLERK_PUBLISHABLE_KEY),
})));
pool.on("error", (error) => {
  console.error("[database] idle client error:", errorMessage(error));
});
type AuthedRequest = Request & { localUserId?: number };
async function requireAuth(req: AuthedRequest, res: Response, next: NextFunction) {
  const auth = getAuth(req);
  const clerkId = auth?.userId || auth?.sessionClaims?.userId;
  if (!clerkId) return res.status(401).json({ error: "Unauthorized" });
  try {
    const clerkUser = await clerkClient.users.getUser(String(clerkId));
    const primary = clerkUser.emailAddresses.find((item) => item.id === clerkUser.primaryEmailAddressId);
    const email = primary?.verification?.status === "verified" ? primary.emailAddress.toLowerCase() : null;
    const client = await pool.connect();
    try {
      let row = (await client.query("SELECT user_id FROM users WHERE clerk_user_id=$1", [clerkId])).rows[0];
      if (!row && email)
        row = (await client.query("SELECT user_id FROM users WHERE lower(email)=lower($1)", [email])).rows[0];
      if (!row) row = (await client.query(
        "INSERT INTO users(name,email,clerk_user_id) VALUES($1,$2,$3) ON CONFLICT (clerk_user_id) DO UPDATE SET updated_at=now() RETURNING user_id",
        [`${clerkUser.firstName || ""} ${clerkUser.lastName || ""}`.trim() || "New user", email || `${clerkId}@clerk.local`, clerkId],
      )).rows[0];
      else await client.query("UPDATE users SET clerk_user_id=$1, updated_at=now() WHERE user_id=$2", [clerkId, row.user_id]);
      await client.query(
        `INSERT INTO feedback_signals(user_id,signal,weight)
         VALUES ($1,'saved',3),($1,'applied',1),($1,'rejected',-2)
         ON CONFLICT DO NOTHING`,
        [row.user_id],
      );
      req.localUserId = Number(row.user_id);
    } finally { client.release(); }
    next();
  } catch (error) { next(error); }
}
app.use("/api", requireAuth);
const uid = (req: AuthedRequest) => req.localUserId!;

app.get("/api/me", async (req: AuthedRequest, res) => {
  const r = await pool.query("SELECT user_id,name,email,target_roles,target_industries,base_city,base_country,remote_allowed,onsite_allowed,hybrid_allowed,min_match_score FROM users WHERE user_id=$1", [uid(req)]);
  res.json(r.rows[0]);
});
app.get("/api/profile/summary", async (req: AuthedRequest, res) => {
  const r = await pool.query("SELECT name,profile_json,profile_text,cv_text,target_roles,target_industries,base_city,base_country,remote_allowed,onsite_allowed,onsite_max_distance_km,hybrid_allowed,hybrid_max_distance_km,willing_to_relocate,relocation_cities,relocation_countries,min_match_score,profile_generated_at,profile_model,profile_version FROM users WHERE user_id=$1", [uid(req)]);
  res.json(r.rows[0] || {});
});
app.patch("/api/profile", async (req: AuthedRequest, res) => {
  const allowed = ["name","profile_text","cv_text","target_roles","target_industries","base_city","base_country","remote_allowed","onsite_allowed","onsite_max_distance_km","hybrid_allowed","hybrid_max_distance_km","willing_to_relocate","relocation_cities","relocation_countries","min_match_score"];
  const entries = Object.entries(req.body || {}).filter(([key]) => allowed.includes(key));
  if (!entries.length) return res.status(400).json({ error: "No profile fields supplied" });
  const current = await pool.query("SELECT profile_text,cv_text,base_city,base_country FROM users WHERE user_id=$1", [uid(req)]);
  const supplied = Object.fromEntries(entries);
  const invalidateProfile =
    ("profile_text" in supplied && supplied.profile_text !== current.rows[0]?.profile_text) ||
    ("cv_text" in supplied && supplied.cv_text !== current.rows[0]?.cv_text);
  const sets = entries.map(([key], i) => `"${key}"=$${i + 1}`);
  if (invalidateProfile) {
    sets.push(
      "profile_json=NULL",
      "profile_source_hash=NULL",
      "profile_generated_at=NULL",
      "profile_model=NULL",
      "profile_version=NULL",
    );
  }
  const invalidateCoordinates =
    ("base_city" in supplied && supplied.base_city !== current.rows[0]?.base_city) ||
    ("base_country" in supplied && supplied.base_country !== current.rows[0]?.base_country);
  if (invalidateCoordinates) sets.push("base_latitude=NULL", "base_longitude=NULL");
  const values = entries.map(([, value]) => value);
  values.push(uid(req));
  const r = await pool.query(`UPDATE users SET ${sets.join(",")},updated_at=now() WHERE user_id=$${values.length} RETURNING name,profile_text,cv_text,target_roles,target_industries,base_city,base_country,remote_allowed,onsite_allowed,onsite_max_distance_km,hybrid_allowed,hybrid_max_distance_km,willing_to_relocate,relocation_cities,relocation_countries,min_match_score`, values);
  res.json(r.rows[0]);
});
app.get("/api/boards/options", async (_req, res) => {
  const r = await pool.query("SELECT b.board_id,b.ats,b.slug,c.display_name company FROM job_boards b JOIN companies c USING(company_id) WHERE b.active ORDER BY c.display_name");
  res.json(r.rows);
});
app.get("/api/boards/search", async (req, res) => {
  const term = String(req.query.q || "").trim();
  const r = await pool.query("SELECT b.board_id,b.ats,b.slug,c.display_name company FROM job_boards b JOIN companies c USING(company_id) WHERE b.active AND ($1='' OR c.display_name ILIKE $2 OR b.slug ILIKE $2) ORDER BY c.display_name LIMIT 100", [term, `%${term}%`]);
  res.json(r.rows);
});
app.get("/api/jobs/matched", async (req: AuthedRequest, res) => {
  const q = req.query as Record<string, string>;
  const requestedScope = q.scope || "matched";
  if (!["matched", "all"].includes(requestedScope)) {
    return res.status(400).json({ error: "scope must be matched or all" });
  }
  const scope = requestedScope as "matched" | "all";
  const requestedLimit = q.limit || "20";
  const parsedLimit = Number.parseInt(requestedLimit, 10);
  const parsedPage = Number.parseInt(q.page || "1", 10);
  const limit = Number.isFinite(parsedLimit) && parsedLimit > 0
    ? Math.min(parsedLimit, 100)
    : 20;
  const showAll = requestedLimit === "all";
  const page = Number.isFinite(parsedPage) && parsedPage > 0 ? parsedPage : 1;
  const matchJoin = scope === "matched"
    ? "JOIN job_matches jm ON jm.job_id=j.job_id AND jm.user_id=$1 AND jm.match_status='matched'"
    : "LEFT JOIN job_matches jm ON jm.job_id=j.job_id AND jm.user_id=$1 AND jm.match_status='matched'";
  const where = ["j.closed_at IS NULL", "EXISTS (SELECT 1 FROM job_boards ab WHERE ab.board_id=j.board_id AND ab.active IS TRUE)"]; const params: unknown[] = [uid(req)];
  const add = (sql: string, value: unknown) => { params.push(value); where.push(sql.replace("?", `$${params.length}`)); };
  if (q.search) {
    params.push(`%${q.search}%`);
    where.push(`(j.title ILIKE $${params.length} OR j.company ILIKE $${params.length} OR j.location_raw ILIKE $${params.length})`);
  }
  if (q.ats) add("j.ats=?", q.ats); if (q.company) add("j.company ILIKE ?", `%${q.company}%`);
  if (q.title) add("j.title ILIKE ?", `%${q.title}%`); if (q.location) add("j.location_raw ILIKE ?", `%${q.location}%`);
  const workplace = q.workplace || q.workplace_type;
  const minScore = q.minScore || q.min_score;
  const date = q.date || q.date_from;
  if (workplace) add("lower(j.workplace_type)=lower(?)", workplace); if (minScore) add("jm.score>=?", Number(minScore));
  if (q.status) { params.push(q.status); where.push(`COALESCE(s.status,'new')=$${params.length}`); }
  if (date) add("j.published_at>=?", date);
  const locationSql = `(CASE
    WHEN lower(trim(location_part)) LIKE '%monaco%' OR lower(trim(location_part)) ~ '(^|[, -])(mc)([, -]|$)' THEN 'monaco'
    WHEN lower(trim(location_part)) ~ '^remote([[:space:]]*[-:][[:space:]]*|[[:space:]]+|$)' THEN 'remote'
    ELSE regexp_replace(lower(trim(split_part(location_part, ',', 1))), '[^a-z0-9]+', '-', 'g')
  END)`;
  const facetWhere = [...where];
  const facetParams = [...params];
  const hasLocationFilter = q.locations !== undefined;
  const requestedLocations = hasLocationFilter
    ? String(q.locations).split(",").filter(Boolean)
    : null;
  if (hasLocationFilter) {
    if (!requestedLocations?.length || requestedLocations.includes("__none__")) {
      where.push("FALSE");
    } else {
      params.push(requestedLocations);
      where.push(`EXISTS (SELECT 1 FROM regexp_split_to_table(COALESCE(j.location_raw,''), '[[:space:]]*;[[:space:]]*') AS location_part WHERE ${locationSql} = ANY($${params.length}::text[]))`);
    }
  }
  const sort = ["date", "published_at"].includes(q.sort) ? "j.published_at DESC NULLS LAST" : q.sort === "title" ? "j.title ASC" : "jm.score DESC NULLS LAST";
  const count = await pool.query(`SELECT count(*) FROM jobs j ${matchJoin} LEFT JOIN user_job_state s ON s.user_id=$1 AND s.job_id=j.job_id WHERE ${where.join(" AND ")}`, params);
  const facetRows = await pool.query(`SELECT j.job_id,j.location_raw FROM jobs j ${matchJoin} LEFT JOIN user_job_state s ON s.user_id=$1 AND s.job_id=j.job_id WHERE ${facetWhere.join(" AND ")}`, facetParams);
  const facets = new Map<string, { value:string; label:string; count:number }>();
  for (const row of facetRows.rows) {
    const seen = new Set<string>();
    for (const part of String(row.location_raw || "").split(";")) {
      const cleaned = part.replace(/[–—]/g, "-").replace(/\s+/g, " ").trim();
      if (!cleaned) continue;
      const lower = cleaned.toLowerCase();
      const monaco = lower.includes("monaco") || /(^|[, -])mc([, -]|$)/.test(lower);
      const remote = /^remote(?:\s*[-:]\s*|\s+|$)/i.test(cleaned);
      const city = cleaned.split(",")[0].trim();
      const value = monaco ? "monaco" : remote ? "remote" : city.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
      const label = monaco ? "Monaco" : remote ? "Remote" : city;
      if (!value || seen.has(value)) continue;
      seen.add(value);
      const existing = facets.get(value);
      facets.set(value, existing ? { ...existing, count: existing.count + 1 } : { value, label, count: 1 });
    }
  }
  const total = Number(count.rows[0].count);
  const effectiveLimit = showAll ? total : limit;
  params.push(effectiveLimit, (page - 1) * effectiveLimit);
  const rows = await pool.query(`SELECT jm.match_id,j.job_id,j.ats,j.external_id,j.company,j.title,j.location_raw,j.workplace_type,j.published_at,j.job_url,jm.score,(jm.job_id IS NOT NULL) AS matched,COALESCE(s.status,'new') status,s.viewed_at,(s.viewed_at IS NOT NULL) AS viewed FROM jobs j ${matchJoin} LEFT JOIN user_job_state s ON s.user_id=$1 AND s.job_id=j.job_id WHERE ${where.join(" AND ")} ORDER BY ${sort} LIMIT $${params.length-1} OFFSET $${params.length}`, params);
  res.json({ rows: rows.rows, total, page, limit: effectiveLimit, scope, facets: { locations: [...facets.values()].sort((a, b) => a.label.localeCompare(b.label)) } });
});
app.get("/api/recommendations/latest", async (req: AuthedRequest, res) => {
  const r = await pool.query("SELECT r.*,j.title,j.company,j.job_url,j.ats,j.external_id,j.location_raw,j.workplace_type,j.published_at,j.description_text FROM job_profile_reviews r JOIN jobs j USING(job_id) JOIN job_boards b ON b.board_id=j.board_id AND b.active IS TRUE WHERE r.user_id=$1 AND r.final_fit_score >= $2 AND r.recommendation_run_id=(SELECT run_id FROM profile_recommendation_runs WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1) ORDER BY r.final_rank NULLS LAST", [uid(req), pinnedRecommendationFloor]); res.json(r.rows);
});
app.patch("/api/jobs/:jobId/status", async (req: AuthedRequest, res) => {
  const status = req.body?.status; if (!["new","viewed","saved","applied","rejected"].includes(status)) return res.status(400).json({ error: "Invalid status" });
  const client = await pool.connect(); try {
    await client.query("BEGIN");
    const changed = await client.query(
      `INSERT INTO user_job_state(user_id,job_id,status,viewed_at)
       SELECT $1,j.job_id,$3,CASE WHEN $3='viewed' THEN now() ELSE NULL END
       FROM jobs j
       WHERE j.job_id=$2 AND j.closed_at IS NULL
         AND EXISTS (SELECT 1 FROM job_boards b WHERE b.board_id=j.board_id AND b.active IS TRUE)
       ON CONFLICT(user_id,job_id) DO UPDATE
        SET status=EXCLUDED.status,
            viewed_at=CASE WHEN EXCLUDED.status='viewed' THEN COALESCE(user_job_state.viewed_at,EXCLUDED.viewed_at) ELSE user_job_state.viewed_at END,
            updated_at=now()
       RETURNING status,viewed_at`,
      [uid(req), req.params.jobId, status],
    );
    if (!changed.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Open job not found" });
    }
    await client.query("INSERT INTO user_job_status_history(user_id,job_id,status) VALUES($1,$2,$3)", [uid(req), req.params.jobId, status]);
    await client.query("COMMIT");
    res.json({ status: changed.rows[0].status, viewedAt: changed.rows[0].viewed_at });
  } catch (e) { await client.query("ROLLBACK"); throw e; } finally { client.release(); }
});
app.post("/api/jobs/:jobId/viewed", async (req: AuthedRequest, res) => {
  const viewedAt = new Date();
  const result = await pool.query(
    `INSERT INTO user_job_state(user_id,job_id,viewed_at)
     SELECT $1,j.job_id,$3
     FROM jobs j
     WHERE j.job_id=$2 AND j.closed_at IS NULL
       AND EXISTS (SELECT 1 FROM job_boards b WHERE b.board_id=j.board_id AND b.active IS TRUE)
     ON CONFLICT(user_id,job_id) DO UPDATE
       SET viewed_at=COALESCE(user_job_state.viewed_at,EXCLUDED.viewed_at),
           status=CASE WHEN user_job_state.status='new' THEN 'viewed' ELSE user_job_state.status END,
           updated_at=now()
     RETURNING viewed_at,status`,
    [uid(req), req.params.jobId, viewedAt],
  );
  if (!result.rowCount) return res.status(404).json({ error: "Open job not found" });
  res.json({ viewed: true, viewedAt: result.rows[0].viewed_at, status: result.rows[0].status });
});
app.post("/api/search-runs", async (req: AuthedRequest, res) => {
  const { cutoff, scope = "all", ats = null, boardIds = [] } = req.body || {};
  if (!cutoff || !/^\d{4}-\d{2}-\d{2}$/.test(cutoff) || !["all","ats","boards"].includes(scope)) return res.status(400).json({ error: "cutoff (YYYY-MM-DD) and valid scope are required" });
  if (scope === "ats" && ![
    "ashby", "greenhouse", "lever", "smartrecruiters", "workday",
    "recruitee", "teamtailor", "workable",
  ].includes(ats)) return res.status(400).json({ error: "Select a valid ATS" });
  if (scope === "boards" && (!Array.isArray(boardIds) || boardIds.length === 0)) return res.status(400).json({ error: "Select at least one board" });
  const running = await pool.query("SELECT run_id FROM search_runs WHERE owner_user_id=$1 AND status IN ('queued','running') LIMIT 1", [uid(req)]);
  if (running.rowCount) return res.status(409).json({ error: "A search is already running", runId: running.rows[0].run_id });
   let r;
   try {
     r = await pool.query("INSERT INTO search_runs(owner_user_id,scope,ats,board_ids,cutoff,run_type) VALUES($1,$2,$3,$4,$5,'manual') RETURNING run_id", [uid(req),scope,ats,boardIds,cutoff]);
   } catch (e) {
     if ((e as {code?:string}).code === "23505") return res.status(409).json({ error: "A search is already running" });
     throw e;
   }
  const runId = r.rows[0].run_id; const child = spawn("python3", [path.resolve("search_worker.py"), String(runId), String(uid(req))], { detached: true, stdio: "ignore" }); child.unref(); res.status(202).json({ runId });
});
app.get("/api/search-runs/:id", async (req: AuthedRequest, res) => { const r = await pool.query("SELECT run_id,progress,status,scope,cutoff,run_type,error,created_at,updated_at FROM search_runs WHERE run_id=$1 AND owner_user_id=$2", [req.params.id,uid(req)]); if (!r.rows[0]) return res.sendStatus(404); res.json(r.rows[0]); });
app.get("/api/search-runs", async (req: AuthedRequest, res) => { const r = await pool.query("SELECT run_id,progress,status,scope,cutoff,run_type,error,created_at,updated_at FROM search_runs WHERE owner_user_id=$1 ORDER BY created_at DESC LIMIT 25", [uid(req)]); res.json(r.rows); });
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
app.post("/api/import", upload.single("file"), async (req: AuthedRequest, res) => {
  if (!req.file) return res.status(400).json({ error: "CSV file is required" }); const records = parse(req.file.buffer, { columns: true, skip_empty_lines: true, bom: true }) as Record<string,string>[]; let updated = 0;
  for (const row of records) { const status = String(row.status || row.match_status || "").trim().toLowerCase(); if (!["new","viewed","saved","applied","rejected"].includes(status)) continue; const r = await pool.query("SELECT job_id FROM jobs WHERE ((ats=$1 AND external_id=$2) OR ($3<>'' AND job_url=$3)) AND EXISTS (SELECT 1 FROM job_boards b WHERE b.board_id=jobs.board_id AND b.active IS TRUE) LIMIT 1", [row.ats,row.external_id,row.job_url || ""]); if (r.rows[0]) { await pool.query("INSERT INTO user_job_state(user_id,job_id,status,viewed_at) VALUES($1,$2,$3,CASE WHEN $3='viewed' THEN now() ELSE NULL END) ON CONFLICT(user_id,job_id) DO UPDATE SET status=EXCLUDED.status,viewed_at=CASE WHEN EXCLUDED.status='viewed' THEN COALESCE(user_job_state.viewed_at,EXCLUDED.viewed_at) ELSE user_job_state.viewed_at END,updated_at=now()", [uid(req),r.rows[0].job_id,status]); await pool.query("INSERT INTO user_job_status_history(user_id,job_id,status) VALUES($1,$2,$3)", [uid(req),r.rows[0].job_id,status]); updated++; } }
  res.json({ imported: updated, rows: records.length });
});
app.get("/api/export.csv", async (req: AuthedRequest, res) => { const r = await pool.query("SELECT j.ats,j.external_id,j.company,j.title,j.location_raw,j.job_url,jm.score,COALESCE(s.status,'new') status FROM job_matches jm JOIN jobs j USING(job_id) JOIN job_boards b ON b.board_id=j.board_id AND b.active IS TRUE LEFT JOIN user_job_state s ON s.user_id=jm.user_id AND s.job_id=j.job_id WHERE jm.user_id=$1 ORDER BY jm.score DESC", [uid(req)]); const header = "ats,external_id,company,title,location,job_url,score,status\n"; const csv = header + r.rows.map(x => Object.values(x).map(v => `"${String(v ?? "").replaceAll('"','""')}"`).join(",")).join("\n"); res.type("text/csv").attachment("matched-jobs.csv").send(csv); });
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => res.status(500).json({ error: err.message }));
async function start() {
  console.log(
    "[startup] Database migrations are not run by the web process; "
    + "apply schema changes through the project migration/publish flow.",
  );
  const httpServer = createHttpServer(app);
  if (process.env.NODE_ENV === "production") {
    app.use(express.static(path.resolve("dist")));
    app.get("*splat", (_req, res) => res.sendFile(path.resolve("dist/index.html")));
  } else {
    const { createServer } = await import("vite");
    const vite = await createServer({
      server: { middlewareMode: true, hmr: { server: httpServer } },
      appType: "spa",
    });
    app.use(vite.middlewares);
  }
  httpServer.listen(5000, "0.0.0.0", () => console.log("API and web app listening on port 5000"));
}
if (process.env.NODE_ENV !== "test") start().catch((error) => {
  console.error("Database migration failed:", error);
  process.exitCode = 1;
});
export default app;
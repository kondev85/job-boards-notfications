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
import { readFileSync } from "node:fs";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const app = express();
app.use(CLERK_PROXY_PATH, clerkProxyMiddleware());
app.use(cors({ credentials: true, origin: true }));
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(clerkMiddleware((req) => ({
  publishableKey: publishableKeyFromHost(getClerkProxyHost(req) ?? "", process.env.CLERK_PUBLISHABLE_KEY),
})));
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
  const q = req.query as Record<string, string>; const limit = Math.min(Number(q.limit || 25), 100); const page = Math.max(Number(q.page || 1), 1);
  const where = ["jm.user_id=$1", "j.closed_at IS NULL", "EXISTS (SELECT 1 FROM job_boards ab WHERE ab.board_id=j.board_id AND ab.active IS TRUE)"]; const params: unknown[] = [uid(req)];
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
  const sort = ["date", "published_at"].includes(q.sort) ? "j.published_at DESC NULLS LAST" : q.sort === "title" ? "j.title ASC" : "jm.score DESC NULLS LAST";
  const count = await pool.query(`SELECT count(*) FROM job_matches jm JOIN jobs j USING(job_id) LEFT JOIN user_job_state s ON s.user_id=jm.user_id AND s.job_id=j.job_id WHERE ${where.join(" AND ")}`, params);
  params.push(limit, (page - 1) * limit);
  const rows = await pool.query(`SELECT jm.match_id,j.job_id,j.ats,j.external_id,j.company,j.title,j.location_raw,j.workplace_type,j.published_at,j.job_url,jm.score,COALESCE(s.status,'new') status FROM job_matches jm JOIN jobs j USING(job_id) LEFT JOIN user_job_state s ON s.user_id=jm.user_id AND s.job_id=j.job_id WHERE ${where.join(" AND ")} ORDER BY ${sort} LIMIT $${params.length-1} OFFSET $${params.length}`, params);
  res.json({ rows: rows.rows, total: Number(count.rows[0].count), page, limit });
});
app.get("/api/recommendations/latest", async (req: AuthedRequest, res) => {
  const r = await pool.query("SELECT r.*,j.title,j.company,j.job_url,j.ats,j.external_id,j.location_raw,j.workplace_type,j.published_at,j.description_text FROM job_profile_reviews r JOIN jobs j USING(job_id) JOIN job_boards b ON b.board_id=j.board_id AND b.active IS TRUE WHERE r.user_id=$1 AND r.recommendation_run_id=(SELECT run_id FROM profile_recommendation_runs WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1) ORDER BY r.final_rank NULLS LAST", [uid(req)]); res.json(r.rows);
});
app.patch("/api/jobs/:jobId/status", async (req: AuthedRequest, res) => {
  const status = req.body?.status; if (!["new","saved","applied","rejected"].includes(status)) return res.status(400).json({ error: "Invalid status" });
  const client = await pool.connect(); try {
    await client.query("BEGIN");
    const changed = await client.query(
      `INSERT INTO user_job_state(user_id,job_id,status)
       SELECT $1,jm.job_id,$3 FROM job_matches jm
       WHERE jm.user_id=$1 AND jm.job_id=$2
       ON CONFLICT(user_id,job_id) DO UPDATE
       SET status=EXCLUDED.status,updated_at=now()
       RETURNING status`,
      [uid(req), req.params.jobId, status],
    );
    if (!changed.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Matched job not found" });
    }
    await client.query("INSERT INTO user_job_status_history(user_id,job_id,status) VALUES($1,$2,$3)", [uid(req), req.params.jobId, status]);
    await client.query("COMMIT");
    res.json({ status });
  } catch (e) { await client.query("ROLLBACK"); throw e; } finally { client.release(); }
});
app.post("/api/search-runs", async (req: AuthedRequest, res) => {
  const { cutoff, scope = "all", ats = null, boardIds = [] } = req.body || {};
  if (!cutoff || !/^\d{4}-\d{2}-\d{2}$/.test(cutoff) || !["all","ats","boards"].includes(scope)) return res.status(400).json({ error: "cutoff (YYYY-MM-DD) and valid scope are required" });
  if (scope === "ats" && !["ashby","greenhouse","lever"].includes(ats)) return res.status(400).json({ error: "Select a valid ATS" });
  if (scope === "boards" && (!Array.isArray(boardIds) || boardIds.length === 0)) return res.status(400).json({ error: "Select at least one board" });
  const running = await pool.query("SELECT run_id FROM search_runs WHERE owner_user_id=$1 AND status IN ('queued','running') LIMIT 1", [uid(req)]);
  if (running.rowCount) return res.status(409).json({ error: "A search is already running", runId: running.rows[0].run_id });
  const r = await pool.query("INSERT INTO search_runs(owner_user_id,scope,ats,board_ids,cutoff) VALUES($1,$2,$3,$4,$5) RETURNING run_id", [uid(req),scope,ats,boardIds,cutoff]);
  const runId = r.rows[0].run_id; const child = spawn("python3", [path.resolve("search_worker.py"), String(runId), String(uid(req))], { detached: true, stdio: "ignore" }); child.unref(); res.status(202).json({ runId });
});
app.get("/api/search-runs/:id", async (req: AuthedRequest, res) => { const r = await pool.query("SELECT run_id,progress,status,scope,cutoff,error,created_at,updated_at FROM search_runs WHERE run_id=$1 AND owner_user_id=$2", [req.params.id,uid(req)]); if (!r.rows[0]) return res.sendStatus(404); res.json(r.rows[0]); });
app.get("/api/search-runs", async (req: AuthedRequest, res) => { const r = await pool.query("SELECT run_id,progress,status,scope,cutoff,error,created_at,updated_at FROM search_runs WHERE owner_user_id=$1 ORDER BY created_at DESC LIMIT 25", [uid(req)]); res.json(r.rows); });
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });
app.post("/api/import", upload.single("file"), async (req: AuthedRequest, res) => {
  if (!req.file) return res.status(400).json({ error: "CSV file is required" }); const records = parse(req.file.buffer, { columns: true, skip_empty_lines: true, bom: true }) as Record<string,string>[]; let updated = 0;
  for (const row of records) { const status = String(row.status || row.match_status || "").trim().toLowerCase(); if (!["new","saved","applied","rejected"].includes(status)) continue; const r = await pool.query("SELECT job_id FROM jobs WHERE ((ats=$1 AND external_id=$2) OR ($3<>'' AND job_url=$3)) AND EXISTS (SELECT 1 FROM job_boards b WHERE b.board_id=jobs.board_id AND b.active IS TRUE) LIMIT 1", [row.ats,row.external_id,row.job_url || ""]); if (r.rows[0]) { await pool.query("INSERT INTO user_job_state(user_id,job_id,status) VALUES($1,$2,$3) ON CONFLICT(user_id,job_id) DO UPDATE SET status=EXCLUDED.status,updated_at=now()", [uid(req),r.rows[0].job_id,status]); await pool.query("INSERT INTO user_job_status_history(user_id,job_id,status) VALUES($1,$2,$3)", [uid(req),r.rows[0].job_id,status]); updated++; } }
  res.json({ imported: updated, rows: records.length });
});
app.get("/api/export.csv", async (req: AuthedRequest, res) => { const r = await pool.query("SELECT j.ats,j.external_id,j.company,j.title,j.location_raw,j.job_url,jm.score,COALESCE(s.status,'new') status FROM job_matches jm JOIN jobs j USING(job_id) JOIN job_boards b ON b.board_id=j.board_id AND b.active IS TRUE LEFT JOIN user_job_state s ON s.user_id=jm.user_id AND s.job_id=j.job_id WHERE jm.user_id=$1 ORDER BY jm.score DESC", [uid(req)]); const header = "ats,external_id,company,title,location,job_url,score,status\n"; const csv = header + r.rows.map(x => Object.values(x).map(v => `"${String(v ?? "").replaceAll('"','""')}"`).join(",")).join("\n"); res.type("text/csv").attachment("matched-jobs.csv").send(csv); });
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => res.status(500).json({ error: err.message }));
async function start() {
  // The CLI remains the canonical owner of the large base schema; this small
  // migration is safe to apply repeatedly when the web service starts.
  await pool.query(readFileSync(path.resolve("migrations/001_authenticated_app.sql"), "utf8"));
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
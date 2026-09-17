ALTER TABLE users ADD COLUMN IF NOT EXISTS clerk_user_id TEXT UNIQUE;
ALTER TABLE job_boards DROP CONSTRAINT IF EXISTS job_boards_ats_check;
ALTER TABLE job_boards ADD CONSTRAINT job_boards_ats_check
  CHECK (ats IN (
    'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
    'recruitee', 'teamtailor', 'workable'
  ));
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_ats_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_ats_check
  CHECK (ats IN (
    'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
    'recruitee', 'teamtailor', 'workable'
  ));

CREATE TABLE IF NOT EXISTS user_job_state (
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  job_id BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'new'
    CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected')),
  viewed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, job_id)
);
ALTER TABLE user_job_state ADD COLUMN IF NOT EXISTS viewed_at TIMESTAMPTZ;
ALTER TABLE user_job_state DROP CONSTRAINT IF EXISTS user_job_state_status_check;
ALTER TABLE user_job_state ADD CONSTRAINT user_job_state_status_check
  CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected'));
CREATE TABLE IF NOT EXISTS user_job_status_history (
  history_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  job_id BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected')),
  changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE user_job_status_history DROP CONSTRAINT IF EXISTS user_job_status_history_status_check;
ALTER TABLE user_job_status_history ADD CONSTRAINT user_job_status_history_status_check
  CHECK (status IN ('new', 'viewed', 'saved', 'applied', 'rejected'));
CREATE TABLE IF NOT EXISTS search_runs (
  run_id BIGSERIAL PRIMARY KEY,
  owner_user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  progress INTEGER NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'completed', 'completed_with_warnings', 'failed')),
  scope TEXT NOT NULL CHECK (scope IN ('all', 'ats', 'boards')),
  ats TEXT,
  board_ids BIGINT[],
  cutoff DATE NOT NULL,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE search_runs ADD COLUMN IF NOT EXISTS run_type TEXT NOT NULL DEFAULT 'manual';
ALTER TABLE search_runs DROP CONSTRAINT IF EXISTS search_runs_status_check;
ALTER TABLE search_runs ADD CONSTRAINT search_runs_status_check
  CHECK (status IN ('queued', 'running', 'completed', 'completed_with_warnings', 'failed'));
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'search_runs'::regclass
      AND conname = 'search_runs_run_type_check'
  ) THEN
    ALTER TABLE search_runs ADD CONSTRAINT search_runs_run_type_check
      CHECK (run_type IN ('manual', 'scheduled'));
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS feedback_signals (
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  signal TEXT NOT NULL CHECK (signal IN ('saved', 'applied', 'rejected')),
  weight INTEGER NOT NULL,
  PRIMARY KEY (user_id, signal)
);
INSERT INTO feedback_signals (user_id, signal, weight)
SELECT user_id, signal, weight FROM users CROSS JOIN
  (VALUES ('saved', 3), ('applied', 1), ('rejected', -2)) AS defaults(signal, weight)
ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS user_job_state_status_idx ON user_job_state(user_id, status);
CREATE INDEX IF NOT EXISTS search_runs_owner_idx ON search_runs(owner_user_id, created_at DESC);
WITH active_runs AS (
  SELECT run_id,
         row_number() OVER (
           PARTITION BY owner_user_id
           ORDER BY updated_at DESC, run_id DESC
         ) AS active_rank
  FROM search_runs
  WHERE status IN ('queued', 'running')
)
UPDATE search_runs
SET status = 'failed',
    error = COALESCE(error || ' | ', '') || 'Superseded while enforcing one active run per user',
    updated_at = now()
WHERE run_id IN (
  SELECT run_id FROM active_runs WHERE active_rank > 1
);
CREATE UNIQUE INDEX IF NOT EXISTS search_runs_one_active_per_user_idx
  ON search_runs(owner_user_id) WHERE status IN ('queued', 'running');
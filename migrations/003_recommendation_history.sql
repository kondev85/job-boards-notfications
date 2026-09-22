CREATE TABLE IF NOT EXISTS profile_recommendation_runs (
  run_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  daily_import_run_id BIGINT REFERENCES daily_import_runs(import_run_id)
    ON DELETE SET NULL,
  profile_source_hash TEXT NOT NULL,
  published_after TIMESTAMPTZ,
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ,
  candidate_floor INTEGER NOT NULL,
  top_n INTEGER NOT NULL,
  candidate_count INTEGER NOT NULL,
  shortlist_hash TEXT NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE profile_recommendation_runs
  ADD COLUMN IF NOT EXISTS daily_import_run_id BIGINT
  REFERENCES daily_import_runs(import_run_id) ON DELETE SET NULL;
ALTER TABLE profile_recommendation_runs
  ADD COLUMN IF NOT EXISTS published_after TIMESTAMPTZ;
ALTER TABLE profile_recommendation_runs
  ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ;
ALTER TABLE profile_recommendation_runs
  ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS profile_recommendation_runs_user_idx
  ON profile_recommendation_runs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS profile_recommendation_runs_daily_idx
  ON profile_recommendation_runs(daily_import_run_id);

CREATE TABLE IF NOT EXISTS profile_recommendation_items (
  item_id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL
    REFERENCES profile_recommendation_runs(run_id) ON DELETE CASCADE,
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  job_id BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  job_source_hash TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  gemini_model TEXT NOT NULL,
  batch_fit_score INTEGER CHECK (batch_fit_score BETWEEN 0 AND 100),
  batch_recommendation TEXT,
  batch_strengths JSONB,
  batch_concerns JSONB,
  batch_rationale TEXT,
  final_fit_score INTEGER CHECK (final_fit_score BETWEEN 0 AND 100),
  final_rank INTEGER,
  final_recommendation TEXT,
  final_strengths JSONB,
  final_concerns JSONB,
  final_rationale TEXT,
  is_recommended BOOLEAN NOT NULL DEFAULT FALSE,
  reviewed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT profile_recommendation_items_run_job_key
    UNIQUE (run_id, user_id, job_id)
);

CREATE INDEX IF NOT EXISTS profile_recommendation_items_run_idx
  ON profile_recommendation_items(run_id, is_recommended, final_rank);
CREATE INDEX IF NOT EXISTS profile_recommendation_items_user_job_idx
  ON profile_recommendation_items(user_id, job_id, reviewed_at DESC);
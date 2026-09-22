INSERT INTO profile_recommendation_items (
  run_id, user_id, job_id, job_source_hash,
  prompt_version, gemini_model, batch_fit_score,
  batch_recommendation, batch_strengths, batch_concerns,
  batch_rationale, final_fit_score, final_rank,
  final_recommendation, final_strengths, final_concerns,
  final_rationale, is_recommended, reviewed_at
)
SELECT
  r.run_id,
  jpr.user_id,
  jpr.job_id,
  jpr.job_source_hash,
  jpr.prompt_version,
  jpr.gemini_model,
  jpr.batch_fit_score,
  jpr.batch_recommendation,
  jpr.batch_strengths,
  jpr.batch_concerns,
  jpr.batch_rationale,
  jpr.final_fit_score,
  jpr.final_rank,
  jpr.final_recommendation,
  jpr.final_strengths,
  jpr.final_concerns,
  jpr.final_rationale,
  TRUE,
  COALESCE(
    jpr.final_reviewed_at,
    jpr.batch_reviewed_at,
    jpr.updated_at,
    jpr.created_at,
    r.created_at
  )
FROM job_profile_reviews jpr
JOIN profile_recommendation_runs r
  ON r.run_id = jpr.recommendation_run_id
 AND r.user_id = jpr.user_id
WHERE jpr.recommendation_run_id IS NOT NULL
ON CONFLICT (run_id, user_id, job_id) DO NOTHING;
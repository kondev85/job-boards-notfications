ALTER TABLE jobs ADD COLUMN IF NOT EXISTS published_at_source TEXT;

UPDATE jobs
SET published_at_source = 'provider'
WHERE published_at IS NOT NULL
  AND published_at_source IS NULL;
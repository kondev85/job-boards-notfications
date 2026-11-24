-- Add HiBob to the ATS allow-lists without narrowing previously supported values.
ALTER TABLE job_boards DROP CONSTRAINT IF EXISTS job_boards_ats_check;
ALTER TABLE job_boards ADD CONSTRAINT job_boards_ats_check
  CHECK (ats IN (
    'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
    'recruitee', 'teamtailor', 'workable', 'personio', 'bamboohr',
    'pinpoint', 'breezy', 'hibob', 'rippling'
  ));

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_ats_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_ats_check
  CHECK (ats IN (
    'ashby', 'greenhouse', 'lever', 'smartrecruiters', 'workday',
    'recruitee', 'teamtailor', 'workable', 'personio', 'bamboohr',
    'pinpoint', 'breezy', 'hibob', 'rippling'
  ));

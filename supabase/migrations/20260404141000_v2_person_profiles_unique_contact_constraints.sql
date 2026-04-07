-- Permanent guard: one profile per normalized contact.

-- Ensure existing duplicates are merged before unique indexes.
SELECT v2.rebuild_profiles_now();

CREATE UNIQUE INDEX IF NOT EXISTS ux_person_profiles_canonical_email_unique
  ON v2.person_profiles (canonical_email)
  WHERE canonical_email IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_person_profiles_canonical_phone_unique
  ON v2.person_profiles (canonical_phone)
  WHERE canonical_phone IS NOT NULL;

-- Run this in Supabase SQL Editor (supabase.com → your project → SQL Editor)

-- 1. Subscribers table
CREATE TABLE subscribers (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  email text UNIQUE NOT NULL,
  name text DEFAULT '',
  firm text DEFAULT '',
  status text DEFAULT 'active' CHECK (status IN ('active', 'unsubscribed')),
  unsubscribe_token uuid DEFAULT gen_random_uuid() NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_subscribers_status ON subscribers (status);
CREATE INDEX idx_subscribers_token ON subscribers (unsubscribe_token);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER subscribers_updated_at
  BEFORE UPDATE ON subscribers
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- 2. RLS (daily script uses service_role key which bypasses this)
ALTER TABLE subscribers ENABLE ROW LEVEL SECURITY;

-- 3. Subscribe function (call from your site when you add the button)
CREATE OR REPLACE FUNCTION public.subscribe(
  p_email text,
  p_name text DEFAULT '',
  p_firm text DEFAULT ''
)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result subscribers;
BEGIN
  INSERT INTO subscribers (email, name, firm, status)
  VALUES (lower(trim(p_email)), trim(p_name), trim(p_firm), 'active')
  ON CONFLICT (email) DO UPDATE
    SET status = 'active',
        name = CASE WHEN trim(p_name) != '' THEN trim(p_name) ELSE subscribers.name END,
        firm = CASE WHEN trim(p_firm) != '' THEN trim(p_firm) ELSE subscribers.firm END
  RETURNING * INTO result;

  RETURN json_build_object('success', true, 'email', result.email);
END;
$$;

-- 4. Unsubscribe function (called from unsubscribe page in email footer)
CREATE OR REPLACE FUNCTION public.unsubscribe(p_token uuid)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result subscribers;
BEGIN
  UPDATE subscribers
  SET status = 'unsubscribed'
  WHERE unsubscribe_token = p_token AND status = 'active'
  RETURNING * INTO result;

  IF result IS NULL THEN
    RETURN json_build_object('success', false, 'message', 'Already unsubscribed or invalid link');
  END IF;

  RETURN json_build_object('success', true, 'email', result.email);
END;
$$;

GRANT EXECUTE ON FUNCTION public.subscribe TO anon;
GRANT EXECUTE ON FUNCTION public.unsubscribe TO anon;

-- 5. Add yourself as the first subscriber
INSERT INTO subscribers (email, name, firm) VALUES ('connor@brieflywealth.com', 'Connor', 'Briefly Wealth');

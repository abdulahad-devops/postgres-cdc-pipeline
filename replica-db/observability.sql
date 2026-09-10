ALTER TABLE public.orders
ADD COLUMN IF NOT EXISTS replicated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;

CREATE TABLE IF NOT EXISTS public.cdc_protected_deletes (
  audit_id BIGSERIAL PRIMARY KEY,
  source_table TEXT NOT NULL DEFAULT 'public.orders',
  order_id BIGINT NOT NULL,
  deleted_customer_name TEXT,
  deleted_amount NUMERIC(12, 2),
  deleted_status TEXT,
  deleted_updated_at TIMESTAMPTZ,
  source_lsn NUMERIC NOT NULL,
  source_event_timestamp TIMESTAMPTZ,
  protected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (source_table, order_id, source_lsn)
);

ALTER TABLE public.cdc_protected_deletes
  ADD COLUMN IF NOT EXISTS deleted_customer_name TEXT,
  ADD COLUMN IF NOT EXISTS deleted_amount NUMERIC(12, 2),
  ADD COLUMN IF NOT EXISTS deleted_status TEXT,
  ADD COLUMN IF NOT EXISTS deleted_updated_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS cdc_protected_deletes_order_id_idx
ON public.cdc_protected_deletes (order_id);

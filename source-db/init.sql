CREATE ROLE cdc_user
WITH REPLICATION LOGIN PASSWORD 'cdc_password';

CREATE TABLE public.orders (
    id BIGSERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'Pending',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

GRANT CONNECT ON DATABASE source_db TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON TABLE public.orders TO cdc_user;

CREATE PUBLICATION orders_publication
FOR TABLE public.orders;

INSERT INTO public.orders (
    customer_name,
    amount,
    status
)
VALUES
    ('Ali', 5000, 'Pending'),
    ('Sara', 3200, 'Shipped');
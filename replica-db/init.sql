CREATE TABLE public.orders (
    id BIGINT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(30) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE public.orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_ref TEXT NOT NULL UNIQUE,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_idr INTEGER NOT NULL CHECK (unit_price_idr > 0),
    status TEXT NOT NULL CHECK (status IN ('paid', 'cancelled')),
    placed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE PUBLICATION kilatmart_orders FOR TABLE public.orders;

INSERT INTO public.orders (order_ref, sku, quantity, unit_price_idr, status) VALUES
    ('KM-1001', 'KOPI-250G', 2, 68000, 'paid'),
    ('KM-1002', 'TEH-25CT', 1, 22000, 'paid'),
    ('KM-1003', 'GULA-1KG', 1, 18000, 'cancelled'),
    ('KM-1004', 'KOPI-250G', 1, 68000, 'paid'),
    ('KM-1005', 'TEH-25CT', 3, 22000, 'paid');


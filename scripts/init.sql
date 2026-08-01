CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    full_name       VARCHAR(150) NOT NULL,
    email           VARCHAR(150) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT NOT NULL REFERENCES customers(customer_id),
    status          VARCHAR(30) NOT NULL DEFAULT 'created',
    total_amount    NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders     REPLICA IDENTITY FULL;

INSERT INTO customers (full_name, email) VALUES
    ('Ana Souza', 'ana.souza@example.com'),
    ('Bruno Lima', 'bruno.lima@example.com');

-- One schema per service-owned table group (DESIGN.md section 2).
CREATE SCHEMA IF NOT EXISTS orders;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS inventory;

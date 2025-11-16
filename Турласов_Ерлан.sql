-- Создание схем
CREATE SCHEMA IF NOT EXISTS stg_sales;
CREATE SCHEMA IF NOT EXISTS stg_sales_fact;
CREATE SCHEMA IF NOT EXISTS stg;

-- СХЕМА stg_sales - справочники (dimensions)
CREATE TABLE stg_sales.brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name TEXT NULL
);

CREATE TABLE stg_sales.product_lines (
    line_id SERIAL PRIMARY KEY,
    line_name TEXT NULL
);

CREATE TABLE stg_sales.product_classes (
    class_id SERIAL PRIMARY KEY,
    class_name TEXT NULL
);

CREATE TABLE stg_sales.product_sizes (
    size_id SERIAL PRIMARY KEY,
    size_name TEXT NULL
);

CREATE TABLE stg_sales.order_statuses (
    status_id SERIAL PRIMARY KEY,
    status_name TEXT NOT NULL
);

CREATE TABLE stg_sales.genders (
    gender_id SERIAL PRIMARY KEY,
    gender_code TEXT NOT NULL
);

CREATE TABLE stg_sales.industries (
    industry_id SERIAL PRIMARY KEY,
    industry_name TEXT NOT NULL
);

CREATE TABLE stg_sales.job_roles (
    role_id SERIAL PRIMARY KEY,
    job_title TEXT NULL
);

CREATE TABLE stg_sales.wealth_segments (
    wealth_id SERIAL PRIMARY KEY,
    wealth_name TEXT NOT NULL
);

CREATE TABLE stg_sales.countries (
    country_id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL
);

CREATE TABLE stg_sales.states (
    state_id SERIAL PRIMARY KEY,
    state_code TEXT NOT NULL,
    country_id INT NOT NULL,
    FOREIGN KEY (country_id) REFERENCES stg_sales.countries(country_id)
);



-- СХЕМА stg_sales_fact - фактовые таблицы и основные сущности
CREATE TABLE stg_sales_fact.products (
    product_id SERIAL PRIMARY KEY,
    source_product_id INT NOT NULL,
    brand_id INT NULL,
    line_id INT NULL,
    class_id INT NULL,
    size_id INT NULL,
    FOREIGN KEY (brand_id) REFERENCES stg_sales.brands(brand_id),
    FOREIGN KEY (line_id) REFERENCES stg_sales.product_lines(line_id),
    FOREIGN KEY (class_id) REFERENCES stg_sales.product_classes(class_id),
    FOREIGN KEY (size_id) REFERENCES stg_sales.product_sizes(size_id)
);

CREATE TABLE stg_sales_fact.customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NULL,
    gender_id INT NOT NULL,
    dob DATE NULL,
    role_id INT NULL,
    industry_id INT NOT NULL,
    wealth_id INT NOT NULL,
    deceased_indicator TEXT NOT NULL,
    owns_car TEXT NOT NULL,
    address TEXT NOT NULL,
    postcode INT NOT NULL,
    state_id INT NOT NULL,
    property_valuation INT NOT NULL,
    FOREIGN KEY (gender_id) REFERENCES stg_sales.genders(gender_id),
    FOREIGN KEY (role_id) REFERENCES stg_sales.job_roles(role_id),
    FOREIGN KEY (industry_id) REFERENCES stg_sales.industries(industry_id),
    FOREIGN KEY (wealth_id) REFERENCES stg_sales.wealth_segments(wealth_id),
    FOREIGN KEY (state_id) REFERENCES stg_sales.states(state_id)
);

CREATE TABLE stg_sales_fact.transactions (
    transaction_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    online_order BOOLEAN NULL,
    status_id INT NOT NULL,
    list_price NUMERIC(12,2) NOT NULL,
    standard_cost NUMERIC(12,2) NULL,
    FOREIGN KEY (product_id) REFERENCES stg_sales_fact.products(product_id),
    FOREIGN KEY (customer_id) REFERENCES stg_sales_fact.customers(customer_id),
    FOREIGN KEY (status_id) REFERENCES stg_sales.order_statuses(status_id)
);



-- из stg.transaction
INSERT INTO stg_sales.brands (brand_name)
SELECT DISTINCT t.brand
FROM stg.transaction t
WHERE t.brand IS NOT NULL;

INSERT INTO stg_sales.product_lines (line_name)
SELECT DISTINCT t.product_line
FROM stg.transaction t
WHERE t.product_line IS NOT NULL;

INSERT INTO stg_sales.product_classes (class_name)
SELECT DISTINCT t.product_class
FROM stg.transaction t
WHERE t.product_class IS NOT NULL;

INSERT INTO stg_sales.product_sizes (size_name)
SELECT DISTINCT t.product_size
FROM stg.transaction t
WHERE t.product_size IS NOT NULL;

INSERT INTO stg_sales.order_statuses (status_name)
SELECT DISTINCT t.order_status
FROM stg.transaction t
WHERE t.order_status IS NOT NULL;

-- из stg.customer
INSERT INTO stg_sales.genders (gender_code)
SELECT DISTINCT c.gender
FROM stg.customer c
WHERE c.gender IS NOT NULL;

INSERT INTO stg_sales.industries (industry_name)
SELECT DISTINCT c.job_industry_category
FROM stg.customer c
WHERE c.job_industry_category IS NOT NULL;

INSERT INTO stg_sales.job_roles (job_title)
SELECT DISTINCT c.job_title
FROM stg.customer c
WHERE c.job_title IS NOT NULL;

INSERT INTO stg_sales.wealth_segments (wealth_name)
SELECT DISTINCT c.wealth_segment
FROM stg.customer c
WHERE c.wealth_segment IS NOT NULL;

INSERT INTO stg_sales.countries (country_name)
SELECT DISTINCT c.country
FROM stg.customer c
WHERE c.country IS NOT NULL;

INSERT INTO stg_sales.states (state_code, country_id)
SELECT DISTINCT
    c.state,
    co.country_id
FROM stg.customer c
JOIN stg_sales.countries co
    ON co.country_name = c.country
WHERE c.state IS NOT NULL
    AND c.country IS NOT NULL;

--Вставка в таблицы stg_sales_fact 
--фикс некорректного названия таблицы и атрибута после импорта файла 
ALTER TABLE stg."transactions" RENAME TO transaction;
--фикс некорректного названия таблицы и атрибута после импорта файла 
ALTER TABLE stg.customer RENAME COLUMN "DOB" TO dob;

WITH combos AS (
    SELECT DISTINCT
        t.product_id AS source_product_id,   
        t.brand,
        t.product_line,
        t.product_class,
        t.product_size
    FROM stg.transaction t
)
INSERT INTO stg_sales_fact.products (source_product_id, brand_id, line_id, class_id, size_id)
SELECT        
    c.source_product_id::int,
    b.brand_id,
    l.line_id,
    pc.class_id,
    s.size_id
FROM combos c
LEFT JOIN stg_sales.brands b ON b.brand_name  = c.brand
LEFT JOIN stg_sales.product_lines l ON l.line_name   = c.product_line
LEFT JOIN stg_sales.product_classes pc ON pc.class_name = c.product_class
LEFT JOIN stg_sales.product_sizes s ON s.size_name   = c.product_size;



INSERT INTO stg_sales_fact.customers (
    customer_id, first_name, last_name, gender_id, dob, role_id, industry_id,
    wealth_id, deceased_indicator, owns_car, address, postcode, state_id, property_valuation
)
SELECT
    c.customer_id::int,
    c.first_name,
    c.last_name,
    g.gender_id,
    c.dob::date,
    r.role_id,
    i.industry_id,
    w.wealth_id,
    c.deceased_indicator,
    c.owns_car,
    c.address,
    c.postcode::int,
    s.state_id,
    c.property_valuation::int
FROM stg.customer c
LEFT JOIN stg_sales.genders g
    ON g.gender_code = c.gender
LEFT JOIN stg_sales.job_roles r
    ON r.job_title = c.job_title
LEFT JOIN stg_sales.industries i
    ON i.industry_name = c.job_industry_category
LEFT JOIN stg_sales.wealth_segments w
    ON w.wealth_name = c.wealth_segment
LEFT JOIN stg_sales.countries co
    ON co.country_name = c.country
LEFT JOIN stg_sales.states s
    ON s.state_code = c.state
        AND s.country_id = co.country_id;


INSERT INTO stg_sales_fact.transactions (
  transaction_id, product_id, customer_id, transaction_date,
  online_order, status_id, list_price, standard_cost
)
SELECT
    t.transaction_id::int,
    p.product_id::int,          
    c.customer_id::int,
    t.transaction_date::date,
    t.online_order::bool,
    os.status_id::int,
    REPLACE(t.list_price::text, ',', '.')::numeric,
    REPLACE(t.standard_cost::text, ',', '.')::numeric
FROM stg.transaction t
LEFT JOIN stg_sales.brands b_t
    ON b_t.brand_name = t.brand
LEFT JOIN stg_sales.product_lines l_t
    ON l_t.line_name = t.product_line
LEFT JOIN stg_sales.product_classes pc_t
    ON pc_t.class_name = t.product_class
LEFT JOIN stg_sales.product_sizes s_t
    ON s_t.size_name = t.product_size
LEFT JOIN stg_sales_fact.products p
    ON p.source_product_id = t.product_id::int
    AND (p.brand_id IS NOT DISTINCT FROM b_t.brand_id)
    AND (p.line_id IS NOT DISTINCT FROM l_t.line_id)
    AND (p.class_id IS NOT DISTINCT FROM pc_t.class_id)
    AND (p.size_id IS NOT DISTINCT FROM s_t.size_id)
LEFT JOIN stg_sales_fact.customers c
    ON c.customer_id = t.customer_id::int
LEFT JOIN stg_sales.order_statuses os 
    ON os.status_name = t.order_status
WHERE c.customer_id IS NOT NULL;


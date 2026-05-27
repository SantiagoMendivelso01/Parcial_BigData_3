-- Schema ShopStream Data Warehouse

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS top_pages_by_time (
    id                  BIGSERIAL PRIMARY KEY,
    event_date          DATE            NOT NULL,
    page_url            VARCHAR(512)    NOT NULL,
    page_type           VARCHAR(50),
    avg_time_seconds    NUMERIC(10, 2),
    total_visits        BIGINT,
    loaded_at           TIMESTAMPTZ     DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_top_pages_date_url ON top_pages_by_time(event_date, page_url);

CREATE TABLE IF NOT EXISTS bounce_rate (
    id               BIGSERIAL PRIMARY KEY,
    event_date       DATE         NOT NULL,
    page_type        VARCHAR(50)  NOT NULL,
    total_sessions   BIGINT,
    bounce_sessions  BIGINT,
    bounce_rate_pct  NUMERIC(5, 2),
    loaded_at        TIMESTAMPTZ  DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_bounce_date_pagetype ON bounce_rate(event_date, page_type);

CREATE TABLE IF NOT EXISTS time_by_device_country (
    id               BIGSERIAL PRIMARY KEY,
    event_date       DATE         NOT NULL,
    device_type      VARCHAR(20)  NOT NULL,
    country          VARCHAR(5)   NOT NULL,
    avg_time_seconds NUMERIC(10, 2),
    total_sessions   BIGINT,
    unique_users     BIGINT,
    loaded_at        TIMESTAMPTZ  DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tdc_date_device_country ON time_by_device_country(event_date, device_type, country);

CREATE TABLE IF NOT EXISTS conversion_funnel (
    id                   BIGSERIAL PRIMARY KEY,
    event_date           DATE        NOT NULL,
    funnel_stage         VARCHAR(50) NOT NULL,
    users                BIGINT,
    conversion_rate_pct  NUMERIC(5, 2),
    loaded_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_funnel_date_stage ON conversion_funnel(event_date, funnel_stage);

CREATE TABLE IF NOT EXISTS product_view_vs_cart (
    id                  BIGSERIAL PRIMARY KEY,
    event_date          DATE            NOT NULL,
    product_id          VARCHAR(20)     NOT NULL,
    category            VARCHAR(50),
    price               NUMERIC(12, 2),
    total_views         BIGINT,
    cart_adds           BIGINT,
    view_to_cart_rate   NUMERIC(5, 2),
    is_low_conversion   BOOLEAN,
    loaded_at           TIMESTAMPTZ     DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_pvvc_date_product ON product_view_vs_cart(event_date, product_id);

CREATE TABLE IF NOT EXISTS navigation_paths (
    id               BIGSERIAL PRIMARY KEY,
    event_date       DATE          NOT NULL,
    navigation_path  TEXT          NOT NULL,
    session_count    BIGINT,
    loaded_at        TIMESTAMPTZ   DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_navpaths_date_path ON navigation_paths(event_date, navigation_path);

CREATE TABLE IF NOT EXISTS anomalies (
    id             BIGSERIAL PRIMARY KEY,
    event_date     DATE          NOT NULL,
    session_id     VARCHAR(100)  NOT NULL,
    user_id        VARCHAR(100),
    page_url       VARCHAR(512),
    page_type      VARCHAR(50),
    z_score        NUMERIC(8, 4),
    anomaly_type   VARCHAR(100),
    anomaly_field  VARCHAR(100),
    anomaly_value  NUMERIC(15, 4),
    detected_at    TIMESTAMPTZ,
    loaded_at      TIMESTAMPTZ   DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_anomalies_date ON anomalies(event_date);

CREATE OR REPLACE VIEW daily_summary AS
SELECT
    cf.event_date,
    cf.users                                                        AS total_users,
    (SELECT SUM(br.total_sessions) FROM bounce_rate br
     WHERE br.event_date = cf.event_date)                           AS total_sessions,
    (SELECT ROUND(AVG(br.bounce_rate_pct), 2) FROM bounce_rate br
     WHERE br.event_date = cf.event_date)                           AS avg_bounce_rate_pct,
    (SELECT MAX(cf2.conversion_rate_pct) FROM conversion_funnel cf2
     WHERE cf2.event_date = cf.event_date
       AND cf2.funnel_stage = '4_checkout')                         AS checkout_conversion_pct,
    (SELECT COUNT(*) FROM anomalies a
     WHERE a.event_date = cf.event_date)                            AS anomaly_count
FROM conversion_funnel cf
WHERE cf.funnel_stage = '1_page_view'
ORDER BY cf.event_date DESC;

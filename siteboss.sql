CREATE TABLE "nodes" (
  "id" integer PRIMARY KEY,
  "id_text" varchar UNIQUE NOT NULL,
  "label" varchar UNIQUE NOT NULL,
  "name" varchar
);

CREATE TABLE "sensor_text" (
  "id" integer PRIMARY KEY,
  "si" varchar UNIQUE NOT NULL,
  "slogan" varchar NOT NULL
);

CREATE TABLE "severity_text" (
  "id" integer PRIMARY KEY,
  "sv" integer UNIQUE NOT NULL,
  "slogan" varchar NOT NULL
);

CREATE TABLE "notifications" (
  "id" integer PRIMARY KEY,
  "receipt_dt" datetime,
  "receipt_dt_utc" datetime,
  "event_ts" integer,
  "event_dt" datetime,
  "event_dt_utc" datetime,
  "evt_to_rcpt_sec" integer,
  "time_ok" bool,
  "ne_id" varchar REFERENCES "nodes" ("id_text"),
  "mt" varchar,
  "st" varchar,
  "si" varchar,
  "va" varchar,
  "sc" varchar,
  "sv" varchar,
  "ke" varchar,
  "cn" varchar,
  "na" varchar,
  "rn" varchar,
  "ss" varchar,
  "it" varchar
);

CREATE TABLE "notifications_historical" (
  "id" integer PRIMARY KEY,
  "receipt_dt" datetime,
  "receipt_dt_utc" datetime,
  "event_ts" integer,
  "event_dt" datetime,
  "event_dt_utc" datetime,
  "evt_to_rcpt_sec" integer,
  "time_ok" bool,
  "ne_id" varchar REFERENCES "nodes" ("id_text"),
  "mt" varchar,
  "st" varchar,
  "si" varchar,
  "va" varchar,
  "sc" varchar,
  "sv" varchar,
  "ke" varchar,
  "cn" varchar,
  "na" varchar,
  "rn" varchar,
  "ss" varchar,
  "it" varchar
);

CREATE TABLE "alarms" (
  "id" integer PRIMARY KEY,
  "notification" integer REFERENCES "notifications" ("id"),
  "event_datetime" datetime,
  "node_label" varchar,
  "node_name" varchar,
  "alarm" varchar,
  "state" varchar
);

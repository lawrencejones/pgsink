--
-- PostgreSQL database dump
--

-- Dumped from database version 13.1 (Debian 13.1-1.pgdg100+1)
-- Dumped by pg_dump version 13.1 (Debian 13.1-1.pgdg100+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pgsink; Type: SCHEMA; Schema: -; Owner: pgsink
--

CREATE SCHEMA pgsink;


ALTER SCHEMA pgsink OWNER TO pgsink;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: import_jobs; Type: TABLE; Schema: pgsink; Owner: pgsink
--

CREATE TABLE pgsink.import_jobs (
    id bigint NOT NULL,
    subscription_id text NOT NULL,
    table_name text NOT NULL,
    cursor text,
    completed_at timestamp with time zone,
    expired_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    error text,
    schema text NOT NULL,
    error_count bigint DEFAULT 0 NOT NULL,
    last_error_at timestamp with time zone
);


ALTER TABLE pgsink.import_jobs OWNER TO pgsink;

--
-- Name: import_jobs_id_seq; Type: SEQUENCE; Schema: pgsink; Owner: pgsink
--

CREATE SEQUENCE pgsink.import_jobs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pgsink.import_jobs_id_seq OWNER TO pgsink;

--
-- Name: import_jobs_id_seq; Type: SEQUENCE OWNED BY; Schema: pgsink; Owner: pgsink
--

ALTER SEQUENCE pgsink.import_jobs_id_seq OWNED BY pgsink.import_jobs.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: pgsink; Owner: pgsink
--

CREATE TABLE pgsink.schema_migrations (
    id integer NOT NULL,
    version_id bigint NOT NULL,
    is_applied boolean NOT NULL,
    tstamp timestamp without time zone DEFAULT now()
);


ALTER TABLE pgsink.schema_migrations OWNER TO pgsink;

--
-- Name: schema_migrations_id_seq; Type: SEQUENCE; Schema: pgsink; Owner: pgsink
--

CREATE SEQUENCE pgsink.schema_migrations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE pgsink.schema_migrations_id_seq OWNER TO pgsink;

--
-- Name: schema_migrations_id_seq; Type: SEQUENCE OWNED BY; Schema: pgsink; Owner: pgsink
--

ALTER SEQUENCE pgsink.schema_migrations_id_seq OWNED BY pgsink.schema_migrations.id;


--
-- Name: import_jobs id; Type: DEFAULT; Schema: pgsink; Owner: pgsink
--

ALTER TABLE ONLY pgsink.import_jobs ALTER COLUMN id SET DEFAULT nextval('pgsink.import_jobs_id_seq'::regclass);


--
-- Name: schema_migrations id; Type: DEFAULT; Schema: pgsink; Owner: pgsink
--

ALTER TABLE ONLY pgsink.schema_migrations ALTER COLUMN id SET DEFAULT nextval('pgsink.schema_migrations_id_seq'::regclass);


--
-- Name: import_jobs import_jobs_pkey; Type: CONSTRAINT; Schema: pgsink; Owner: pgsink
--

ALTER TABLE ONLY pgsink.import_jobs
    ADD CONSTRAINT import_jobs_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: pgsink; Owner: pgsink
--

ALTER TABLE ONLY pgsink.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--


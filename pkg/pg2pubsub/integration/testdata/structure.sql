--
-- PostgreSQL database dump
--

-- Dumped from database version 11.5
-- Dumped by pg_dump version 11.5

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

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: example; Type: TABLE; Schema: public; Owner: pg2pubsub_test
--

CREATE TABLE public.example (
    id bigint NOT NULL,
    msg text
);


ALTER TABLE public.example OWNER TO pg2pubsub_test;

--
-- Name: example_id_seq; Type: SEQUENCE; Schema: public; Owner: pg2pubsub_test
--

CREATE SEQUENCE public.example_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.example_id_seq OWNER TO pg2pubsub_test;

--
-- Name: example_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: pg2pubsub_test
--

ALTER SEQUENCE public.example_id_seq OWNED BY public.example.id;


--
-- Name: example id; Type: DEFAULT; Schema: public; Owner: pg2pubsub_test
--

ALTER TABLE ONLY public.example ALTER COLUMN id SET DEFAULT nextval('public.example_id_seq'::regclass);


--
-- Name: example example_pkey; Type: CONSTRAINT; Schema: public; Owner: pg2pubsub_test
--

ALTER TABLE ONLY public.example
    ADD CONSTRAINT example_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--


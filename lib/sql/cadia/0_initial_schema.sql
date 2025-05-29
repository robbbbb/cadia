--
-- PostgreSQL database dump
--

-- Dumped from database version 14.17 (Debian 14.17-1.pgdg110+1)
-- Dumped by pg_dump version 14.17 (Debian 14.17-1.pgdg110+1)

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

SET default_table_access_method = heap;

--
-- Name: searches; Type: TABLE; Schema: public; Owner: root
--

CREATE TABLE public.searches (
    id integer NOT NULL,
    criteria bytea,
    "timestamp" timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.searches OWNER TO root;

--
-- Name: searches_id_seq; Type: SEQUENCE; Schema: public; Owner: root
--

CREATE SEQUENCE public.searches_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.searches_id_seq OWNER TO root;

--
-- Name: searches_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: root
--

ALTER SEQUENCE public.searches_id_seq OWNED BY public.searches.id;


--
-- Name: tags; Type: TABLE; Schema: public; Owner: root
--

CREATE TABLE public.tags (
    id integer NOT NULL,
    search_id integer,
    name text,
    colour text,
    icon text,
    notes text
);


ALTER TABLE public.tags OWNER TO root;

--
-- Name: tags_id_seq; Type: SEQUENCE; Schema: public; Owner: root
--

CREATE SEQUENCE public.tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tags_id_seq OWNER TO root;

--
-- Name: tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: root
--

ALTER SEQUENCE public.tags_id_seq OWNED BY public.tags.id;


--
-- Name: searches id; Type: DEFAULT; Schema: public; Owner: root
--

ALTER TABLE ONLY public.searches ALTER COLUMN id SET DEFAULT nextval('public.searches_id_seq'::regclass);


--
-- Name: tags id; Type: DEFAULT; Schema: public; Owner: root
--

ALTER TABLE ONLY public.tags ALTER COLUMN id SET DEFAULT nextval('public.tags_id_seq'::regclass);


--
-- Name: searches searches_pkey; Type: CONSTRAINT; Schema: public; Owner: root
--

ALTER TABLE ONLY public.searches
    ADD CONSTRAINT searches_pkey PRIMARY KEY (id);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: public; Owner: root
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (id);


--
-- Name: TABLE searches; Type: ACL; Schema: public; Owner: root
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.searches TO cadia_website;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.searches TO cadia_update;


--
-- Name: SEQUENCE searches_id_seq; Type: ACL; Schema: public; Owner: root
--

GRANT USAGE ON SEQUENCE public.searches_id_seq TO cadia_website;
GRANT USAGE ON SEQUENCE public.searches_id_seq TO cadia_update;


--
-- Name: TABLE tags; Type: ACL; Schema: public; Owner: root
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.tags TO cadia_website;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.tags TO cadia_update;


--
-- PostgreSQL database dump complete
--


--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'LATIN1';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: migration; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE migration (
    "from" character varying(255),
    "to" character varying(255),
    key character varying(1024),
    env character varying(255),
    pid integer,
    mid bigint NOT NULL,
    date timestamp without time zone DEFAULT now(),
    last_update timestamp without time zone DEFAULT now(),
    status character varying(255)
);


ALTER TABLE public.migration OWNER TO postgres;

--
-- Name: migration_mid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE migration_mid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.migration_mid_seq OWNER TO postgres;

--
-- Name: migration_mid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE migration_mid_seq OWNED BY migration.mid;


--
-- Name: mid; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY migration ALTER COLUMN mid SET DEFAULT nextval('migration_mid_seq'::regclass);


--
-- PostgreSQL database dump complete
--


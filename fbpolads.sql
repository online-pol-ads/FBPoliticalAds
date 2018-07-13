--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.6
-- Dumped by pg_dump version 10.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: ad_sponsors; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.ad_sponsors (
    nyu_id integer NOT NULL,
    name character varying,
    nyu_category integer
);


ALTER TABLE public.ad_sponsors OWNER TO nyufbpolads;

--
-- Name: ad_sponsors_id_seq; Type: SEQUENCE; Schema: public; Owner: nyufbpolads
--

CREATE SEQUENCE public.ad_sponsors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ad_sponsors_id_seq OWNER TO nyufbpolads;

--
-- Name: ad_sponsors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: nyufbpolads
--

ALTER SEQUENCE public.ad_sponsors_id_seq OWNED BY public.ad_sponsors.nyu_id;


--
-- Name: ads; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.ads (
    archive_id bigint NOT NULL,
    id bigint NOT NULL,
    page_id bigint NOT NULL,
    start_date date,
    end_date date,
    text character varying,
    image_url character varying(256),
    video_url character varying(256),
    is_deleted boolean,
    is_active boolean,
    ad_sponsor_id integer,
    nyu_id integer NOT NULL,
    has_cards boolean
);


ALTER TABLE public.ads OWNER TO nyufbpolads;

--
-- Name: ads_serial_id_seq; Type: SEQUENCE; Schema: public; Owner: nyufbpolads
--

CREATE SEQUENCE public.ads_serial_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ads_serial_id_seq OWNER TO nyufbpolads;

--
-- Name: ads_serial_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: nyufbpolads
--

ALTER SEQUENCE public.ads_serial_id_seq OWNED BY public.ads.nyu_id;


--
-- Name: cards; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.cards (
    ad_archive_id bigint NOT NULL,
    text character varying,
    title character varying,
    video_url character varying(256),
    image_url character varying(256)
);


ALTER TABLE public.cards OWNER TO nyufbpolads;

--
-- Name: categories; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.categories (
    id bigint NOT NULL,
    name character varying
);


ALTER TABLE public.categories OWNER TO nyufbpolads;

--
-- Name: nyu_sponsor_categories; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.nyu_sponsor_categories (
    name character varying(255),
    id integer NOT NULL
);


ALTER TABLE public.nyu_sponsor_categories OWNER TO nyufbpolads;

--
-- Name: categories_id_seq; Type: SEQUENCE; Schema: public; Owner: nyufbpolads
--

CREATE SEQUENCE public.categories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.categories_id_seq OWNER TO nyufbpolads;

--
-- Name: categories_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: nyufbpolads
--

ALTER SEQUENCE public.categories_id_seq OWNED BY public.nyu_sponsor_categories.id;


--
-- Name: demo_group; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.demo_group (
    gender character varying(16),
    age_range character varying(256),
    nyu_id integer NOT NULL
);


ALTER TABLE public.demo_group OWNER TO nyufbpolads;

--
-- Name: demo_group_id_seq; Type: SEQUENCE; Schema: public; Owner: nyufbpolads
--

CREATE SEQUENCE public.demo_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.demo_group_id_seq OWNER TO nyufbpolads;

--
-- Name: demo_group_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: nyufbpolads
--

ALTER SEQUENCE public.demo_group_id_seq OWNED BY public.demo_group.nyu_id;


--
-- Name: page_categories; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.page_categories (
    page_id bigint NOT NULL,
    category_id bigint NOT NULL
);


ALTER TABLE public.page_categories OWNER TO nyufbpolads;

--
-- Name: pages; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.pages (
    id bigint NOT NULL,
    name character varying(256) NOT NULL,
    url character varying(256),
    is_deleted boolean,
    category integer
);


ALTER TABLE public.pages OWNER TO nyufbpolads;

--
-- Name: regions; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.regions (
    name character varying(256),
    nyu_id integer NOT NULL
);


ALTER TABLE public.regions OWNER TO nyufbpolads;

--
-- Name: regions_id_seq; Type: SEQUENCE; Schema: public; Owner: nyufbpolads
--

CREATE SEQUENCE public.regions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.regions_id_seq OWNER TO nyufbpolads;

--
-- Name: regions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: nyufbpolads
--

ALTER SEQUENCE public.regions_id_seq OWNED BY public.regions.nyu_id;


--
-- Name: snapshot_demo; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.snapshot_demo (
    demo_id integer NOT NULL,
    value real,
    nyu_snapshot_id integer,
    max_impressions integer,
    min_impressions integer
);


ALTER TABLE public.snapshot_demo OWNER TO nyufbpolads;

--
-- Name: snapshot_region; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.snapshot_region (
    region_id integer NOT NULL,
    value real,
    nyu_snapshot_id integer,
    min_impressions integer,
    max_impressions integer
);


ALTER TABLE public.snapshot_region OWNER TO nyufbpolads;

--
-- Name: snapshots; Type: TABLE; Schema: public; Owner: nyufbpolads
--

CREATE TABLE public.snapshots (
    id bigint NOT NULL,
    ad_archive_id bigint NOT NULL,
    date date NOT NULL,
    max_spend integer,
    max_impressions integer,
    nyu_id integer NOT NULL,
    min_impressions integer,
    most_recent boolean,
    min_spend integer,
    currency character(64),
    start_date date,
    end_date date,
    is_active boolean
);


ALTER TABLE public.snapshots OWNER TO nyufbpolads;

--
-- Name: snapshots_serial_id_seq; Type: SEQUENCE; Schema: public; Owner: nyufbpolads
--

CREATE SEQUENCE public.snapshots_serial_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.snapshots_serial_id_seq OWNER TO nyufbpolads;

--
-- Name: snapshots_serial_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: nyufbpolads
--

ALTER SEQUENCE public.snapshots_serial_id_seq OWNED BY public.snapshots.nyu_id;


--
-- Name: ad_sponsors nyu_id; Type: DEFAULT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.ad_sponsors ALTER COLUMN nyu_id SET DEFAULT nextval('public.ad_sponsors_id_seq'::regclass);


--
-- Name: ads nyu_id; Type: DEFAULT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.ads ALTER COLUMN nyu_id SET DEFAULT nextval('public.ads_serial_id_seq'::regclass);


--
-- Name: demo_group nyu_id; Type: DEFAULT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.demo_group ALTER COLUMN nyu_id SET DEFAULT nextval('public.demo_group_id_seq'::regclass);


--
-- Name: nyu_sponsor_categories id; Type: DEFAULT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.nyu_sponsor_categories ALTER COLUMN id SET DEFAULT nextval('public.categories_id_seq'::regclass);


--
-- Name: regions nyu_id; Type: DEFAULT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.regions ALTER COLUMN nyu_id SET DEFAULT nextval('public.regions_id_seq'::regclass);


--
-- Name: snapshots nyu_id; Type: DEFAULT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.snapshots ALTER COLUMN nyu_id SET DEFAULT nextval('public.snapshots_serial_id_seq'::regclass);


--
-- Name: ads ads_pkey; Type: CONSTRAINT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.ads
    ADD CONSTRAINT ads_pkey PRIMARY KEY (nyu_id);


--
-- Name: categories page_category_pkey; Type: CONSTRAINT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.categories
    ADD CONSTRAINT page_category_pkey PRIMARY KEY (id);


--
-- Name: pages pages_pkey; Type: CONSTRAINT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.pages
    ADD CONSTRAINT pages_pkey PRIMARY KEY (id);


--
-- Name: snapshots snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: nyufbpolads
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_pkey PRIMARY KEY (nyu_id);


--
-- Name: ad_sponsors_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX ad_sponsors_id_index ON public.ad_sponsors USING btree (nyu_id);


--
-- Name: ad_sponsors_name_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX ad_sponsors_name_index ON public.ad_sponsors USING btree (name);


--
-- Name: ads_page_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX ads_page_id_index ON public.ads USING btree (page_id);


--
-- Name: ads_sponsor_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX ads_sponsor_id_index ON public.ads USING btree (ad_sponsor_id);


--
-- Name: demo_group_age_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX demo_group_age_index ON public.demo_group USING btree (age_range);


--
-- Name: demo_group_gender_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX demo_group_gender_index ON public.demo_group USING btree (gender);


--
-- Name: page_categories_category_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX page_categories_category_id_index ON public.page_categories USING btree (category_id);


--
-- Name: page_categories_page_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX page_categories_page_id_index ON public.page_categories USING btree (page_id);


--
-- Name: region_name_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX region_name_index ON public.regions USING btree (name);


--
-- Name: snapshot_demo_demo_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX snapshot_demo_demo_id_index ON public.snapshot_demo USING btree (demo_id);


--
-- Name: snapshot_demo_snapshot_serial_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX snapshot_demo_snapshot_serial_id_index ON public.snapshot_demo USING btree (nyu_snapshot_id);


--
-- Name: snapshot_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX snapshot_id_index ON public.snapshots USING btree (id);


--
-- Name: snapshot_region_region_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX snapshot_region_region_id_index ON public.snapshot_region USING btree (region_id);


--
-- Name: snapshot_region_snapshot_serial_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX snapshot_region_snapshot_serial_id_index ON public.snapshot_region USING btree (nyu_snapshot_id);


--
-- Name: snapshots_ad_archive_id_index; Type: INDEX; Schema: public; Owner: nyufbpolads
--

CREATE INDEX snapshots_ad_archive_id_index ON public.snapshots USING btree (ad_archive_id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: nyufbpolads
--

REVOKE ALL ON SCHEMA public FROM rdsadmin;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO nyufbpolads;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--


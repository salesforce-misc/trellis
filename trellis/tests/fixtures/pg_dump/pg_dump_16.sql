--
-- PostgreSQL database dump
--

\restrict IIQ1QhZIhaSUEhWXhCUWEBlIhK5fA7fxJLMVvp51PEfb4GER1YNuTnoN15xfDFd

-- Dumped from database version 16.15
-- Dumped by pg_dump version 16.15

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
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA public;


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: audit(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.audit() RETURNS trigger
    LANGUAGE plpgsql
    AS $$begin return null; end$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: Cats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."Cats" (
    id bigint NOT NULL,
    name text
);


--
-- Name: o1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.o1 (
    id bigint NOT NULL,
    g bigint,
    b bigint
);


--
-- Name: orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.orders (
    id bigint NOT NULL,
    g bigint,
    a bigint
);


--
-- Name: Cats Cats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."Cats"
    ADD CONSTRAINT "Cats_pkey" PRIMARY KEY (id);


--
-- Name: o1 o1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.o1
    ADD CONSTRAINT o1_pkey PRIMARY KEY (id);


--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);


--
-- Name: orders orders_audit; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER orders_audit AFTER INSERT ON public.orders FOR EACH STATEMENT EXECUTE FUNCTION public.audit();


--
-- Name: orders trellis_capture_begin; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trellis_capture_begin BEFORE INSERT OR DELETE OR UPDATE ON public.orders FOR EACH STATEMENT EXECUTE FUNCTION trellis.cap_begin_orders_c74da4a21f58f903();

ALTER TABLE public.orders ENABLE ALWAYS TRIGGER trellis_capture_begin;


--
-- Name: orders trellis_capture_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trellis_capture_delete AFTER DELETE ON public.orders REFERENCING OLD TABLE AS trellis_old FOR EACH STATEMENT EXECUTE FUNCTION trellis.cap_delete_orders_c74da4a21f58f903();

ALTER TABLE public.orders ENABLE ALWAYS TRIGGER trellis_capture_delete;


--
-- Name: orders trellis_capture_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trellis_capture_insert AFTER INSERT ON public.orders REFERENCING NEW TABLE AS trellis_new FOR EACH STATEMENT EXECUTE FUNCTION trellis.cap_insert_orders_c74da4a21f58f903();

ALTER TABLE public.orders ENABLE ALWAYS TRIGGER trellis_capture_insert;


--
-- Name: orders trellis_capture_truncate; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trellis_capture_truncate AFTER TRUNCATE ON public.orders FOR EACH STATEMENT EXECUTE FUNCTION trellis.cap_truncate_orders_c74da4a21f58f903();

ALTER TABLE public.orders ENABLE ALWAYS TRIGGER trellis_capture_truncate;


--
-- Name: orders trellis_capture_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trellis_capture_update AFTER UPDATE ON public.orders REFERENCING OLD TABLE AS trellis_old NEW TABLE AS trellis_new FOR EACH STATEMENT EXECUTE FUNCTION trellis.cap_update_orders_c74da4a21f58f903();

ALTER TABLE public.orders ENABLE ALWAYS TRIGGER trellis_capture_update;


--
-- PostgreSQL database dump complete
--

\unrestrict IIQ1QhZIhaSUEhWXhCUWEBlIhK5fA7fxJLMVvp51PEfb4GER1YNuTnoN15xfDFd


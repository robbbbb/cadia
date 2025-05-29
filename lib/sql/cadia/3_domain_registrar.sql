CREATE TABLE public.domain_registrar (
    registrar_id INTEGER NOT NULL PRIMARY KEY,
    registrar_name TEXT
);

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.domain_registrar TO cadia_website;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.domain_registrar TO cadia_update;
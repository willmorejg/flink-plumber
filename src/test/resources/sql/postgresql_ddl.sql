-- public.willmores definition

-- Drop table

-- DROP TABLE willmores;

CREATE TABLE willmores (
	id serial4 NOT NULL,
	given_name varchar(255) NULL,
	middle_name varchar(255) NULL,
	surname varchar(255) NULL,
	suffix varchar(255) NULL,
	created_at timestamp NULL DEFAULT now(),
	updated_at timestamp NOT NULL DEFAULT now()
);

-- Table Triggers

create trigger set_timestamp before
update
    on
    public.willmores for each row execute function trigger_set_timestamp();

-- public."policy" definition

-- Drop table

-- DROP TABLE "policy";

CREATE TABLE "policy" (
	policy_id serial4 NOT NULL,
	policy_uuid varchar NULL DEFAULT uuid_generate_v4(),
	policy_number varchar NOT NULL,
	date_code int4 NOT NULL,
	status varchar NOT NULL,
	effective_date date NOT NULL,
	expiration_date date NOT NULL,
	created_at timestamp NULL DEFAULT now(),
	updated_at timestamp NOT NULL DEFAULT now(),
	CONSTRAINT policy_pk PRIMARY KEY (policy_id)
);
CREATE UNIQUE INDEX idx_policy_pn_dc_eff_exp ON public.policy USING btree (policy_number, date_code, effective_date, expiration_date);

-- Table Triggers

create trigger set_ts_policy before
update
    on
    public.policy for each row execute function trigger_set_timestamp();

-- public.risk definition

-- Drop table

-- DROP TABLE public.risk;

CREATE TABLE public.risk (
	risk_id serial4 NOT NULL,
	risk_uuid varchar NULL DEFAULT uuid_generate_v4(),
	policy_id int4 NOT NULL,
	parent_risk_id int4 NULL,
	risk_type varchar NULL,
	description varchar NULL,
	created_at timestamp NULL DEFAULT now(),
	updated_at timestamp NOT NULL DEFAULT now(),
	CONSTRAINT risk_pk PRIMARY KEY (risk_id),
	CONSTRAINT risk_parent_fk FOREIGN KEY (parent_risk_id) REFERENCES public.risk(risk_id),
	CONSTRAINT risk_policy_fk FOREIGN KEY (policy_id) REFERENCES public.policy(policy_id)
);

create index risk_policy_id_idx on public.risk using btree (policy_id);

-- Table Triggers

create trigger set_ts_risk before
update
    on
    public.risk for each row execute function trigger_set_timestamp();
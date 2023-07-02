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
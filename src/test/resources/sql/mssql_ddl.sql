-- willmores.dbo.willmores definition
-- Drop table

 -- DROP TABLE willmores.dbo.willmores;

CREATE TABLE willmores.dbo.willmores (
	id bigint IDENTITY(1,
1) PRIMARY KEY,
	given_name varchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	middle_name varchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	surname varchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	suffix varchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	created_at datetime2(0) DEFAULT GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'US Eastern Standard Time' NULL,
	updated_at datetime2(0) DEFAULT GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'US Eastern Standard Time' NULL
);

CREATE TRIGGER dbo.trgAfterUpdate ON
willmores.dbo.willmores
AFTER
INSERT
	,
	UPDATE
	AS
  UPDATE
	willmores.dbo.willmores
SET
	updated_at = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'US Eastern Standard Time'
FROM
	Inserted i
 ;
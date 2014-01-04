DROP VIEW IF EXISTS account_latest_project_time;
CREATE VIEW account_latest_project_time AS
SELECT
    a.account_name
	, (
		SELECT max(time_start)
		FROM project p
		WHERE p.account_id = a.account_id
	) latest_project_time
FROM account a
;

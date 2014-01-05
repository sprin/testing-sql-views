WITH parent AS (
    INSERT INTO ${parent_table}(${comma_join(parent_cols)}) VALUES
        ${mogrify(parent_values)}
    RETURNING ${comma_join(parent_pk)}
)
, ${cte_alias}(${comma_join(cte_cols)}) AS (VALUES
${mogrify(cte_values)})
INSERT INTO ${child_table}(${comma_join(child_cols)})
SELECT ${comma_join(child_cols)}
FROM parent, ${cte_alias}


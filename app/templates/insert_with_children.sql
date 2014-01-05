% for parent in parents:
${insert_with_children(parent, loop.index)}
% endfor
SELECT 1

<%def name="insert_with_children(o, i)">
% if i == 0:
WITH parent${i} AS (
% else:
, parent${i} AS (
%endif
	INSERT INTO ${o['parent_table']}(${comma_join(o['parent_cols'])}) VALUES
		${mogrify(o['parent_values'])}
	RETURNING ${comma_join(o['parent_pk'])}
) , values${i}(${comma_join(o['cte_cols'])}) AS (VALUES
	${mogrify(o['cte_values'])}
) , child_insert${i} AS (
	INSERT INTO ${o['child_table']}(${comma_join(o['child_cols'])})
	SELECT ${comma_join(o['child_cols'])}
	FROM parent${i}, values${i}
)
</%def>

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
	INSERT INTO ${o['tablename']}(${comma_join(o['values_cols'])}) VALUES
		${mogrify(o['values'])}
	RETURNING ${comma_join(o['pk'])}
)
% for children in o['all_children']:
${insert_children(children, i, loop.index)}
% endfor
</%def>

<%def name="insert_children(o, i, j)">
, values${i}_${j}(${comma_join(o['values_cols'])}) AS (VALUES
${mogrify(o['values'])}
) , child_insert${i}_${j} AS (
	INSERT INTO ${o['tablename']}(${comma_join(o['insert_cols'])})
	SELECT ${comma_join(o['insert_cols'])}
	FROM parent${i}, values${i}_${j}
)</%def>

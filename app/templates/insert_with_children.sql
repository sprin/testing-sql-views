% for o in records:
${insert_records(o, loop.index)}
% endfor
SELECT 1\
\
<%def name="insert_records(o, i)">\
% if not o.get('natural_joins'):
    % if i == 0:
WITH ${o['tablename']}_insert AS (
    % else:
, ${o['tablename']}_insert AS (
    %endif
	INSERT INTO ${o['tablename']}(${comma_join(o['value_cols'])}) VALUES
	${mogrify(o['values'])}
	RETURNING *
)
% else:
, ${o['tablename']}_values(${comma_join(o['value_cols'])}) AS (VALUES
	${mogrify(o['values'])}
) , ${o['tablename']}_insert AS (
	INSERT INTO ${o['tablename']}(${join_fks(o['natural_joins'])}${comma_join(o['insert_cols'])})
	SELECT ${join_parent_pks(o['natural_joins'])}${comma_join(o['insert_cols'], 'v')}
	FROM ${o['tablename']}_values v
% for j in o['natural_joins']:
	JOIN ${j['parent_table']}_insert p${loop.index}
	ON v.${j['natural_fk']} = p${loop.index}.${j['parent_natural_key']}
	RETURNING *
)
% endfor
%endif
</%def>\
\
<%def name="join_fks(natural_joins)">\
% for j in natural_joins:
${j['surrogate_fk']}, \
% endfor
</%def>\
\
<%def name="join_parent_pks(natural_joins)">\
% for j in natural_joins:
p${loop.index}.${j['surrogate_fk']}, \
% endfor
</%def>\

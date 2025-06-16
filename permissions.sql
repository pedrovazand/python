SELECT 'ALTER FUNCTION ' || n.nspname || '.' || p.proname || '('||string_agg(format_type(t.oid, NULL), ', ') ||') OWNER TO siscobra_r;'
  FROM pg_proc p
  	  JOIN pg_namespace n ON n.oid = p.pronamespace
  	  JOIN unnest(p.proargtypes) WITH ORDINALITY AS args(type_oid, ord) ON TRUE
  	  JOIN pg_type t ON t.oid = args.type_oid
WHERE p.proname ~* ANY (ARRAY['extrator_layout', 'base_filtro'])
GROUP BY n.nspname, p.proname;

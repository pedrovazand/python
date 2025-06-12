CREATE OR REPLACE FUNCTION public.base_arquivo_dinamico(_segcod integer, _file text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
	_filters RECORD;
	_query_filter TEXT = '';
    _header TEXT[];
	_column TEXT[];
    _dinamic_header TEXT = '';
    _dinamic_column TEXT = '';
	_group_by TEXT = '';
    _query TEXT = '';
    _message_error TEXT = '';
	_nivel_table INT[];
	_nivel_filter INT = 1;
	_index INT;
	_start_time TIMESTAMP = clock_timestamp();
BEGIN

	SELECT array_agg(bf.segfilcoldes ORDER BY sc.segcamord),
		   array_agg(bf.segfilcol ORDER BY sc.segcamord),
		   array_agg(DISTINCT COALESCE(bf.basfilniv, 0))
	  INTO _header,
	       _column,
		   _nivel_table
	  FROM segmentacao_campo sc
	  	  JOIN base_filtro bf ON sc.basfilseq = bf.basfilseq 
	 WHERE sc.segcod = _segcod;

	SELECT string_agg('''' || _header[i] || '''', E',\n\t\t')
	  INTO _dinamic_header
	  FROM generate_series(1, array_length(_header, 1)) tab(i);

	SELECT string_agg('COALESCE((' || _column[i] || ')::TEXT, '''')', E',\n\t\t')
	  INTO _dinamic_column
	  FROM generate_series(1, array_length(_column, 1)) tab(i);	

	_query := '
SELECT ' || _dinamic_header || '
 UNION ALL
SELECT DISTINCT ' || _dinamic_column || '
  FROM devedor dev
  	  JOIN pessoa pes ON dev.devcod = pes.devcod AND dev.carcod = pes.carcod AND pes.tippescod = 1
	  JOIN LATERAL (SELECT *
  					  FROM retorno ret 
  					 WHERE ret.devcod = dev.devcod 
  					   AND ret.carcod = dev.carcod
  				     ORDER BY ret.retdatinc DESC,
  				 			  ret.retseq DESC
  					 LIMIT 1) ret ON TRUE
  	  JOIN carteira_situacao cs ON ret.sitcod = cs.sitcod AND ret.carcod = cs.carcod';

	FOREACH _index IN ARRAY _nivel_table LOOP 
		IF _index IN (2, 3) AND _nivel_filter NOT IN (2, 3) THEN 
			_nivel_filter = 2;

			_query = _query||'
			JOIN contrato con ON dev.devcod = con.devcod AND dev.carcod = con.carcod';
		END IF;
	
		IF _index = 3 THEN 
			_nivel_filter = 3;

			_query = _query||'
  	  		JOIN contrato_parcela cp ON con.concod = cp.concod';
		END IF;
		
		IF _index = 4 THEN 
			_query = _query||'
  	  		LEFT JOIN LATERAL (SELECT array_agg(tel.teldddtel||tel.teltel ORDER BY tel.teldatatu DESC) telefone
								 FROM telefone tel 
								WHERE pes.pescod = tel.pescod 
								  AND tel.telsit NOT IN (2, 5, 88)) tel ON TRUE';
		END IF;
	
		IF _index = 5 THEN 
			_query = _query||'
		  LEFT JOIN LATERAL (SELECT array_agg(enn.endlog ORDER BY enn.enddatatu DESC) logradouro,
								    array_agg(enn.endbai ORDER BY enn.enddatatu DESC) bairro,
								    array_agg(enn.endcom ORDER BY enn.enddatatu DESC) complemento,
								    array_agg(enn.endcep ORDER BY enn.enddatatu DESC) cep,
								    array_agg(enn.endcid ORDER BY enn.enddatatu DESC) cidade,
								    array_agg(enn.endufcod ORDER BY enn.enddatatu DESC) uf
							   FROM endereco enn 
							  WHERE pes.pescod = enn.pescod 
								AND enn.endsit NOT IN (2, 5, 88)
							    AND enn.tipendcod = 1) enn ON TRUE';
		END IF;

		IF _index = 7 THEN 
			IF _nivel_filter IN (2, 3) THEN 
			    _query = _query||'
			    LEFT JOIN contrato_parcela_acordo cpa ON cp.conparseq = cpa.conparseq
				LEFT JOIN acordo aco ON cpa.acocod = aco.acocod AND aco.acoati = 0';
			ELSE 
			    _query = _query||'
			    LEFT JOIN acordo aco ON dev.devcod = aco.devcod AND dev.carcod = dev.carcod AND aco.acoati = 0';
		    END IF;
		END IF;

	END LOOP; 

	_query = _query||'
	WHERE dev.devsal > 0
      AND dev.devati = 0';
	
	FOR _filters IN 
		SELECT s1.segfilcod,
			   sf.segfilnom
		  FROM segmentacaolevel1 s1 
		  	  JOIN segmentacao_filtro sf ON s1.segfilcod = sf.segfilcod
		 WHERE s1.segcod = _segcod

	LOOP
		_query_filter = segmentacao_parametros_default(_segcod, _filters.segfilcod, _nivel_filter);
		
 		IF _query_filter = '' THEN 
			_query_filter = segmentacao_parametros_custom(_segcod, _filters.segfilcod, _nivel_filter);
		END IF;
		
		IF _query_filter <> '' THEN 
			_query = _query||_query_filter;
		ELSE 
			_message_error = _message_error||_filters.segfilnom||' ('||_filters.segfilcod|| E') \n';
		END IF;
	END LOOP; 

	_query = ' COPY ('||_query||') TO '''||_file||''' ';

	IF _message_error <> '' THEN 
		_message_error = 'Filtro(s) nao cadastrados: '||_message_error;
		
		INSERT INTO logsegmentacao (logsegcod, logsegque, logsegdatinc, logsegsegcod, logsegsqlerr, logsegbasfil, logsegdatfim)
		VALUES(nextval('logsegseq'), _query, _start_time, _segcod, _message_error, 'base_arquivo_dinamico(int, text)', clock_timestamp());
	ELSE 

		EXECUTE _query;
	
		INSERT INTO logsegmentacao (logsegcod, logsegque, logsegdatinc, logsegsegcod, logsegsqlerr, logsegbasfil, logsegdatfim)
		VALUES(nextval('logsegseq'), _query, _start_time, _segcod, '', 'base_arquivo_dinamico(int, text)', clock_timestamp());
	END IF;

EXCEPTION 
	WHEN OTHERS THEN 

	INSERT INTO logsegmentacao (logsegcod, logsegque, logsegdatinc, logsegsegcod, logsegsqlerr, logsegbasfil, logsegdatfim)
	VALUES(nextval('logsegseq'), _query, _start_time, _segcod, SQLERRM, 'base_arquivo_dinamico(int, text)', clock_timestamp());
END;
$function$
;
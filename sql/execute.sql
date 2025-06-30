CREATE OR REPLACE FUNCTION public.formatar_valor(_valor numeric)
 RETURNS text
 LANGUAGE sql
AS $function$
    SELECT 'R$ ' || trim(REPLACE(REPLACE(REPLACE(to_char(_valor, '999G999G999G990D99'), '.', '|'), ',', '.'), '|', ','))
    $function$
;

CREATE OR REPLACE FUNCTION devedor_saldo_vencido(_devcod BIGINT, _carcod BIGINT)
RETURNS NUMERIC
LANGUAGE SQL 
AS $$
	SELECT COALESCE(sum(cp.conparvalsal))
	  FROM contrato con 
	  	  JOIN contrato_parcela cp ON con.concod = cp.concod 
	 WHERE con.devcod = _devcod 
	   AND con.carcod = _carcod
	   AND cp.conparvalsal > 0
	   AND cp.conparati = 0
	   AND cp.conpardatven < current_date;
$$;

CREATE FUNCTION formatar_cpf_cnpj(_devcpf bigint)
RETURNS varchar 
LANGUAGE SQL 
AS $$

	SELECT 
	 	  CASE 
		      WHEN length(_devcpf::TEXT) <= 11 THEN lpad(_devcpf::TEXT, 11, '0') 
		      ELSE lpad(_devcpf::TEXT, 14, '0')                                 
      	   END;
$$;

ALTER TABLE logsegmentacao ALTER COLUMN logsegcod SET DEFAULT nextval('logsegseq'::regclass);
 
  DELETE FROM logsegmentacao l 
  WHERE l.logsegcod IN (SELECT logsegcod 
  						  FROM logsegmentacao 
  						 GROUP BY logsegcod 
  						HAVING count(logsegcod) > 1)
    OR l.logsegcod ISNULL;

 ALTER TABLE logsegmentacao ADD CONSTRAINT logsegmentacao_pkey PRIMARY KEY (logsegcod);
 

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        WHERE t.tgname = 'devedor_atualizardados'
          AND c.relname = 'devedor'
    ) THEN
        EXECUTE 'ALTER TABLE devedor DISABLE TRIGGER devedor_atualizardados';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.devedor_devati()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
	_vencimento_recente DATE;
	_vencimento_antigo DATE;
BEGIN
	IF (tg_op = 'UPDATE') THEN
		SELECT COALESCE(min(cp.conpardatven),'0001-01-01'::date), 
			   COALESCE(max(cp.conpardatven),'0001-01-01'::date)
		  INTO _vencimento_antigo, 
			   _vencimento_recente 
		  FROM contrato con 
			  JOIN contrato_parcela cp ON con.concod = cp.concod AND cp.conparvalsal > 0
		 WHERE con.carcod = NEW.carcod
		   AND con.devcod = NEW.devcod;
											
		NEW.devvenmaisantigo = _vencimento_antigo;
		NEW.devvenmaisrecente = _vencimento_recente;
	
		IF COALESCE(NEW.devsal, 0) <> COALESCE(OLD.devsal, 1) THEN
			IF NEW.devsal <= 0 THEN
				IF OLD.devati = 0 THEN
					NEW.devati = 1;
				END IF;

				IF COALESCE(OLD.devdatpridev, '0001-01-01'::date) = '0001-01-01'::date then
					NEW.devdatpridev = current_date;
				END IF;
			ELSE
				IF OLD.devati = 1 THEN
					NEW.devati = 0;
				END IF;
			END IF;	
		END IF;
	END IF;
RETURN NEW; 
END;
$function$
; 

CREATE OR REPLACE FUNCTION valor_pago_acordo(_acocod BIGINT)
RETURNS NUMERIC 
LANGUAGE SQL 
AS $$

	SELECT sum(apr.acoparretvallan)
	  FROM acordo_parcela ap 
	  	  JOIN acordo_parcela_retomada apr ON ap.acoparseq = apr.acoparseq AND apr.acoparretati = 0 AND apr.tipenccod = 1
	 WHERE ap.acocod = _acocod;

$$;

SELECT setval('logsegseq', max(logsegcod) + 1) FROM logsegmentacao;

INSERT INTO segmentacao_base_filtro (segbasfiltro, segbasnom, segbastip, segbasusuatu, segbasdatatu, segbasusuinc, segbasdatinc, segbasati, segbasqtdexesim, segbasincarqmon, segbastipexe) 
VALUES(1000, 'VAGO', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);    
    
WITH gerar_campos AS (
    SELECT max(basfilseq) + 1 sequencial
      FROM base_filtro bf 
     WHERE basfilseq < 1000
)
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfildatinc, basfilusuinc, basfildatatu, basfilusuatu, basfilniv) 
SELECT generate_series(sequencial, 999), 1000, 'VAGO', 'VAGO', clock_timestamp(), 9999, clock_timestamp(), 9999, NULL   
  FROM gerar_campos
  ON CONFLICT DO NOTHING;

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

SELECT setval('basfilseq', max(basfilseq)) FROM base_filtro bf;

CREATE FUNCTION parcelas_pagas(_tipo int2, _identificador int8)
RETURNS bigint 
LANGUAGE SQL 
AS $$

	 SELECT CASE 
			 	WHEN _tipo = 1 THEN (SELECT count(cpr.conparseq)
			 						   FROM contrato con 
			 						  	   JOIN contrato_parcela cp ON con.concod = cp.concod 
			 						  	   JOIN contrato_parcela_retomada cpr ON cp.conparseq = cpr.conparseq
			 						  WHERE cpr.conparretati = 0
			 						    AND cpr.tipenccod = 1
			 						    AND con.devcod = _identificador) -- Devedor
			 	
			 	WHEN _tipo = 2 THEN (SELECT count(cpr.conparseq)
			 						   FROM contrato_parcela cp 
			 						   	   JOIN contrato_parcela_retomada cpr ON cp.conparseq = cpr.conparseq 
			 						  WHERE cpr.conparretati = 0
			 						    AND cpr.tipenccod = 1
			 						    AND cp.concod = _identificador) -- Contrato
			 						 
			 	WHEN _tipo = 3 THEN (SELECT count(apr.acoparretseq)
									   FROM acordo_parcela ap	
									   	   JOIN acordo_parcela_retomada apr ON ap.acoparseq = apr.acoparseq 
									  WHERE apr.acoparretati = 0
									    AND apr.tipenccod = 1
									    AND ap.acocod = _identificador) -- Acordo
			 END;
$$;

CREATE OR REPLACE FUNCTION public.valor_pago(_tipo int, _identificador bigint)
 RETURNS numeric
 LANGUAGE sql
AS $function$

	SELECT CASE 
			   WHEN _tipo = 1 THEN (SELECT sum(cpr.conparretvallan) FILTER (WHERE cpr.tipenccod BETWEEN 1 AND 4999) - 
			   							   COALESCE(sum(cpr.conparretvallan) FILTER (WHERE cpr.tipenccod BETWEEN 6000 AND 6012), 0)
									  FROM contrato con 
									  	  JOIN contrato_parcela cp ON con.concod = cp.concod 
									  	  JOIN contrato_parcela_retomada cpr ON cp.conparseq = cpr.conparseq AND cpr.conparretati = 0
									 WHERE con.devcod = _identificador)
			   						  	
			   WHEN _tipo = 2 THEN (SELECT sum(cpr.conparretvallan) FILTER (WHERE cpr.tipenccod BETWEEN 1 AND 4999) - 
			   							   COALESCE(sum(cpr.conparretvallan) FILTER (WHERE cpr.tipenccod BETWEEN 6000 AND 6012), 0)
									  FROM contrato_parcela cp  
									  	  JOIN contrato_parcela_retomada cpr ON cp.conparseq = cpr.conparseq AND cpr.conparretati = 0
									 WHERE cp.concod = _identificador)
									 
			   WHEN _tipo = 3 THEN (SELECT sum(apr.acoparretvallan) FILTER (WHERE apr.tipenccod BETWEEN 1 AND 4999) - 
			   							   COALESCE(sum(apr.acoparretvallan) FILTER (WHERE apr.tipenccod BETWEEN 6000 AND 6012), 0)
									  FROM acordo_parcela ap 
									  	  JOIN acordo_parcela_retomada apr ON ap.acoparseq = apr.acoparseq AND apr.acoparretati = 0
									 WHERE ap.acocod = _identificador)	
			END;
$function$
;

CREATE OR REPLACE FUNCTION public.status_parcela(p_conparseq bigint)
 RETURNS text
 LANGUAGE sql
AS $function$
SELECT
	  CASE
		 WHEN (COALESCE(sum(cp.conparvallan), 0) - (COALESCE(sum(cpr.conparretvallan), 0))) <= 0 THEN 'LQ'
		 WHEN (COALESCE(sum(cp.conparvallan), 0) - (COALESCE(sum(cpr.conparretvallan), 0))) > 0 AND (COALESCE(sum(cp.conparvallan),0) - (COALESCE(sum(cpr.conparretvallan),0))) < (COALESCE(sum(cp.conparvallan),0)) THEN 'PC'
		 WHEN cp.conparacocod > 0 THEN 'AC' 
		 WHEN cp.conparati = 1  THEN 'DV'
		 WHEN (COALESCE(sum(cp.conparvallan), 0) - COALESCE(sum(cpr.conparretvallan), 0)) = (COALESCE(sum(cp.conparvallan), 0)) THEN 'SM'
		 ELSE 'NI'
	  END
FROM contrato_parcela cp
	LEFT JOIN contrato_parcela_retomada cpr ON (cp.conparseq = cpr.conparseq AND cpr.tipenccod = 1 AND cpr.conparretati = 0)
WHERE cp.conparseq = $1
GROUP BY cp.conparseq, cp.conparati, cp.conparvallan, cp.conparacocod
ORDER BY cp.conparseq;
$function$;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(21, 'Devedores em Acordo?', 'Informe S/N.', 'C', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(24, 'Detalhes da Parcela', 'Detalhes da Parcela', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(25, 'Detalhes do Devedor', 'Detalhes do Devedor', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(26, 'Telefone', '', 'I', 3, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(30, 'Tipo de Retomada', '', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(31, 'Tipo de Recebimento', '', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(38, 'Dias de Atraso contrato', 'Dias de:', 'I', 4, 'Até', 'I', 4, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(39, 'Estado Civil', '', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(41, 'Tipo Endereço devedor', '', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(48, 'Carteira', '', '', 3, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(2, 'Tel. Status Registro', '', '', 7, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(10001, 'Depósito', 'Depósito', 'V', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(260, 'Clientes com data de Agenda Vencida', '', '', 0, '', '', 0, '', '', 0, '', '', 0, 1)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(10002, 'Dias Atraso Acordo', 'De', 'I', 0, 'Até', 'I', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(713, 'Usuário da Agenda', '', '', 0, '', '', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(10003, 'Clientes sem Acordo ou Novação', '', '', 0, '', '', 0, '', '', 0, '', '', 0, 1)
ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_filtro (segfilcod, segfilnom, segcaption01, segcapvisible01, segcapsize01, segcaption02, segcapvisible02, segcapsize02, segcaption03, segcapvisible03, segcapsize03, segcaption04, segcapvisible04, segcapsize04, segfiladdfil) 
VALUES(10004, 'Dias de Atraso da Novação', 'Dias inicial', 'I', 0, 'Dias Final', 'I', 0, '', '', 0, '', '', 0, NULL)
ON CONFLICT DO NOTHING;


CREATE OR REPLACE FUNCTION public.segmentacao_parametros_default(_segcod bigint, _segfilcod bigint, _nivel integer)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
    _query TEXT = '';
    _string TEXT = '';
    _count integer = 0;
    _inteiro01 integer = 0;
    _inteiro02 integer = 0;
    _texto01 TEXT = '';
    _texto02 TEXT = '';
    _ponteiro record;
    _valor01 NUMERIC(14,2);
    _valor02 NUMERIC(14,2);
    contadorsegfilD integer = 0;
    contadorsegfilC integer = 0;
    contadorsegfilI integer = 0;
    contadorsegfilV integer = 0;
BEGIN
    /* Níveis
    1 - devedor - dev 
    2 - contrato - con
    3 - parcela - cpa
    */

	-- Pedro Henrique Vaz de Andrade, 14/05/2025 - Alterado código dos filtros.
	-- Essa função NUNCA deve ser alterada e deve ser padrão em todos os clientes.

    /* Dias em Atraso */
    IF _segfilcod = 38 /*10000*/ THEN
        SELECT ''''||(CURRENT_DATE - sll.segfili02::int)::TEXT||'''',
               ''''||(CURRENT_DATE - sll.segfili01::int)::TEXT||''''
          INTO _texto01,
               _texto02  
          FROM segmentacaolevel1level2 sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod
         LIMIT 1;
     
          _query = '
              AND dev.devvenmaisantigo >= '|| _texto01 ||'
              AND dev.devvenmaisantigo <= '|| _texto02 ||' ';
    END IF;
 
    /* Valor em Aberto do Devedor */
    IF _segfilcod = 3 /*10001*/ THEN

        SELECT COALESCE(sll.segfilv01,0), COALESCE(sll.segfilv02,0)
          INTO _valor01, _valor02
          FROM segmentacaolevel1level2  sll
         WHERE  sll.segfilcod = _segfilcod
           AND  sll.segcod    = _segcod;

        _query = ' 
            AND dev.devsal BETWEEN '|| _valor01||' AND '|| _valor02;
    
    END IF;

    /* Valor Inadimplente do Devedor */
    IF _segfilcod = 10000 /*10002*/ THEN 

        SELECT COALESCE(sll.segfilv01,0), COALESCE(sll.segfilv02,0)
          INTO _valor01, _valor02
          FROM segmentacaolevel1level2  sll
         WHERE  sll.segfilcod = _segfilcod
           AND  sll.segcod    = _segcod;
        
        IF _nivel <= 1 THEN 
        
          _query = '
              AND COALESCE((SELECT SUM(conparvalsal)
                              FROM contrato c
                                  JOIN contrato_parcela cp on c.concod = cp.concod
                             WHERE c.devcod = dev.devcod
                               AND cp.conparati    = 0
                               AND cp.conparvalsal > 0
                               AND cp.conpardatven < current_date), 0) BETWEEN '|| _valor01 ||' AND ' || _valor02 ||' ';
                             
        ELSEIF _nivel IN (2, 3) THEN  
        
          _query = '
              AND COALESCE((SELECT SUM(conparvalsal)
                              FROM contrato_parcela cp 
                             WHERE c.concod = cp.concod
                               AND cp.conparati    = 0
                               AND cp.conparvalsal > 0
                               AND cp.conpardatven < current_date), 0) BETWEEN '|| _valor01 ||' AND ' || _valor02 ||' ';
                             
        END IF;
            
    END IF;

    /* Status Telefone */
    IF _segfilcod = 2 /*10003*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
              
        IF _count > 1 THEN
            _query = '
                AND EXISTS (SELECT NULL 
                              FROM pessoa pes
                                JOIN telefone tel ON (tel.pescod = pes.pescod)
                             WHERE pes.devcod = dev.devcod
                               AND tel.telsit IN ('|| _string ||') )';
        ELSE
            _query = '
                AND EXISTS (SELECT NULL 
                              FROM pessoa pes
                                JOIN telefone tel ON (tel.pescod = pes.pescod)
                             WHERE pes.devcod = dev.devcod
                               AND tel.telsit = '|| _string ||' )';
        END IF;   

    END IF;

    /* Carteira/ Assessoria */
    IF _segfilcod = 48 /*10004*/ THEN 
     
        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
             
        IF _count > 1 THEN
            _query = '
                AND dev.carcod IN ('|| _string ||')';
        ELSE 
            _query = '
                AND dev.carcod = ('|| _string ||')';
             
        END IF;
    
    END IF;

    /* Depósito  */
    IF _segfilcod = 10001 /*10005*/ THEN 
    
        SELECT string_agg(sll.segfilv01::TEXT,','),
               count(*)
          INTO _string,
               _count
          FROM segmentacaolevel1level2  sll
         WHERE  sll.segfilcod = _segfilcod
           AND  sll.segcod    = _segcod;
    
        IF _count > 1 THEN 
          _query = '
            AND EXISTS (SELECT NULL 
                          FROM deposito_registro drg
                         WHERE drg.devcod = dev.devcod
                           AND drg.depcod IN ('|| _string ||') )';
        
         ELSE 
          _query = ' 
            AND EXISTS (SELECT NULL 
                          FROM deposito_registro drg
                         WHERE drg.devcod = dev.devcod
                           AND drg.depcod = '|| _string ||')';
         END IF;
    END IF;

    /* Produto  */
    IF _segfilcod = 5 /*10006*/ THEN
    
        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2 sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod; 
    
        IF _nivel IN (2, 3) THEN 
            IF _count > 1 THEN
                _query = '
                    AND con.procod IN ('|| _string ||'';
                  
            ELSE 
                _query = '
                    AND con.procod = '|| _string ||' ';
                 
            END IF;
          
         ELSEIF _nivel <= 1 THEN
         
            IF _count > 1 THEN
              _query = '
                  AND EXISTS (SELECT NULL 
                                FROM contrato con 
                               WHERE con.devcod = dev.devcod
                                 AND con.carcod = dev.carcod
                                 AND con.convalsal > 0
                                 AND con.procod IN ('|| _string ||'))';
                                
          ELSE 
              _query = '
                  AND EXISTS (SELECT NULL 
                                FROM contrato con 
                               WHERE con.devcod = dev.devcod
                                 AND con.carcod = dev.carcod
                                 AND con.convalsal > 0
                                 AND con.procod = '|| _string ||' )';
               
          END IF;
        
        END IF;
      
    END IF;
   
    /* Filial */
    IF _segfilcod = 6 /*10007*/ THEN 
    
        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2 sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
    
         IF _nivel <= 1 THEN 
            IF _count > 1 THEN
                _query = '
                    AND EXISTS (SELECT NULL 
                                  FROM contrato con 
                                 WHERE con.devcod = dev.devcod
                                   AND con.carcod = dev.carcod
                                   AND con.convalsal > 0
                                   AND con.filcod IN ('|| _string ||'))';
                                  
            ELSE 
                _query = '
                    AND EXISTS (SELECT NULL 
                                  FROM contrato con 
                                 WHERE con.devcod = dev.devcod
                                   AND con.carcod = dev.carcod
                                   AND con.convalsal > 0
                                   AND con.filcod = ('|| _string ||'))';
                 
            END IF;
          
         ELSEIF _nivel IN (2, 3) THEN 
         
           IF _count > 1 THEN
                _query = '
                    AND con.filcod IN ('|| _string ||')';
                                  
           ELSE 
                _query = '
                    AND con.filcod = ('|| _string ||')';
                 
           END IF;
         
         END IF;
          
    END IF;
   
/*    /* Empresa */
    IF _segfilcod = 10008 THEN 
   
        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2 sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;

        IF _count > 1 THEN
            _query = '
                AND dev.devempcod IN ('|| _string ||')';
                              
        ELSE 
            _query = '
                AND dev.devempcod = '|| _string ||' ';
             
        END IF;       
          
    END IF;*/
    
    /* Situação */
    IF _segfilcod = 1 /*10009*/ THEN 
   
        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
             
        IF _count > 1 THEN
            _query = '
                AND ret.sitcod IN ('|| _string ||')';
        ELSE 
            _query = '
                AND ret.sitcod = '|| _string ||' ';
             
        END IF;   
   
    END IF;

    /* Status Agenda (A vencer / Vencida)  */
    IF _segfilcod = 260 /*10010*/ THEN 

        _query = '
            AND ret.retdatage <= current_date ';
    
    END IF;

    /* Detalhes Devedor */
    IF _segfilcod = 25 /*10011*/ THEN
        FOR _ponteiro IN (SELECT *
                           FROM segmentacaolevel1level2  c
                          WHERE c.segfilcod = _segfilcod
                            AND c.segcod    = _segcod) 
        LOOP
        --Se for VARCHAR
            IF COALESCE(_ponteiro.segfilc01, '') <> '' THEN
                _query = '
                    AND EXISTS (SELECT 1 
                                  FROM devedor_detalhe dt
                                 WHERE dt.carcod = dev.carcod
                                   AND dt.devcod = dev.devcod
                                   AND dt.devdetcar <> ''''
                                   AND dt.devdetcod = (SELECT c.segfillegcod 
                                                         FROM segmentacaolevel1level2  c
                                                        WHERE c.segfilcod  = '|| _segfilcod || '
                                                          AND c.segcod     = '|| _segcod    || '
                                                          AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1)
                                   AND UPPER(dt.devdetcar) = (SELECT UPPER(segfilc01)
                                                                FROM segmentacaolevel1level2  c
                                                               WHERE c.segfilcod = '|| _segfilcod ||'
                                                                 AND c.segcod    = '|| _segcod    ||' 
                                                                 AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1))';
                contadorsegfilC = contadorsegfilC + 1;
            END IF;
            --Se for INTEGER
            IF COALESCE(_ponteiro.segfili01, 0) <> 0 THEN
                _query = '
                    AND EXISTS (SELECT 1 
                                  FROM devedor_detalhe dt
                                 WHERE dt.carcod = dev.carcod
                                   AND dt.devcod = dev.devcod 
                                   AND dt.devdetint <> 0
                                   AND dt.devdetcod = (SELECT c.segfillegcod 
                                                         FROM segmentacaolevel1level2  c
                                                        WHERE c.segfilcod = '|| _segfilcod || '
                                                          AND c.segcod    = '|| _segcod    || '
                                                          AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1)
                                   AND dt.devdetint = (SELECT segfili01
                                                         FROM segmentacaolevel1level2  c
                                                        WHERE c.segfilcod = '|| _segfilcod ||'
                                                          AND c.segcod    = '|| _segcod    ||'
                                                          AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1))';
                contadorsegfilI = contadorsegfilI + 1; 
            End If;
            --Se for NUMERIC
            IF COALESCE(_ponteiro.segfilv01, 0.00) <> 0.00 THEN
                _query = ' 
                    AND EXISTS (SELECT 1
                                  FROM devedor_detalhe dt
                                 WHERE dt.carcod = dev.carcod
                                   AND dt.devcod = dev.devcod
                                   AND dt.devdetmoe <> 0.00
                                   AND dt.devdetcod = (SELECT c.segfillegcod 
                                                         FROM   segmentacaolevel1level2  c
                                                        WHERE  c.segfilcod = '|| _segfilcod || '
                                                          AND  c.segcod    = '|| _segcod    || '
                                                          AND  c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1)
                                   AND dt.devdetmoe = (SELECT segfilv01
                                                         FROM segmentacaolevel1level2  c
                                                        WHERE c.segfilcod = '|| _segfilcod || '
                                                          AND c.segcod    = '|| _segcod    || '
                                                          AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' limit 1))';
                contadorsegfilV = contadorsegfilV + 1;
            END IF;
            --Se for DATA
            IF COALESCE(_ponteiro.segfild01, '0001-01-01') <> '0001-01-01' THEN
                _query = ' 
                    AND EXISTS (SELECT 1 
                                  FROM devedor_detalhe dt
                                 WHERE dt.carcod = dev.carcod
                                   AND dt.devcod = dev.devcod
                                   AND dt.devdetdat <> ''0001-01-01''
                                   AND dt.devdetcod = (SELECT c.segfillegcod 
                                                         FROM segmentacaolevel1level2  c
                                                        WHERE c.segfilcod = '|| _segfilcod || '
                                                          AND c.segcod    = '|| _segcod    || '
                                                          AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1)
                                   AND dt.devdetdat = (SELECT segfild01
                                                         FROM segmentacaolevel1level2  c
                                                        WHERE c.segfilcod = '|| _segfilcod || '
                                                          AND c.segcod    = '|| _segcod    || '
                                                          AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1))';
                contadorsegfilD = contadorsegfilD + 1;
            End If;
        End Loop;
    End If;

    /* Detalhes Contrato */
    IF _segfilcod = 24 /*10012*/ THEN 
    
        FOR _ponteiro IN (SELECT DISTINCT ON (segfillegcod) *
                            FROM segmentacaolevel1level2  ssl
                           WHERE ssl.segfilcod = _segfilcod
                             AND ssl.segcod    = _segcod) 
        LOOP

            /* Caso for Varchar */
            IF COALESCE(_ponteiro.segfilc01, '') <> '' THEN
              IF _nivel IN (2, 3) THEN -- Contrato e Parcela
                _query = '
                    AND EXISTS ( SELECT NULL
                                   FROM contrato_detalhe cd
                                  WHERE cd.concod = con.concod
                                    AND cd.condetcar <> ''''
                                    AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                    AND UPPER(cd.condetcar) IN (SELECT UPPER(segfilc01)    
                                                                  FROM segmentacaolevel1level2  c
                                                                 WHERE c.segfilcod = '|| _segfilcod || '
                                                                   AND c.segcod    = '|| _segcod    || '
                                                                   AND c.segfilc01 <> '''' ))';
              ELSE
                _query = '
                    AND EXISTS ( SELECT NULL
                                   FROM contrato ct
                                    JOIN contrato_detalhe cd ON (cd.concod = ct.concod)
                                  WHERE ct.carcod = dev.carcod
                                    AND ct.devcod = dev.devcod 
                                    AND cd.condetcar <> ''''
                                    AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                    AND UPPER(cd.condetcar) IN (SELECT UPPER(segfilc01)    
                                                                  FROM segmentacaolevel1level2  c
                                                                 WHERE c.segfilcod = '|| _segfilcod || '
                                                                   AND c.segcod    = '|| _segcod    || '
                                                                   AND c.segfilc01 <> '''' ))';
              END IF;
              contadorsegfilC = contadorsegfilC + 1;
            END IF;
        
            /* Caso for interger */
            IF COALESCE(_ponteiro.segfili01, 0) <> 0  THEN
              IF _nivel IN (2, 3) THEN -- Contrato e Parcela
                _query = '
                    AND EXISTS (SELECT NULL
                                  FROM contrato_detalhe cd
                                 WHERE cd.concod = con.concod
                                   AND cd.condetint <> 0
                                   AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                   AND cd.condetint IN (SELECT segfili01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '))';
              ELSE
                _query = '
                    AND EXISTS (SELECT NULL
                                  FROM contrato ct
                                    JOIN contrato_detalhe cd ON (cd.concod = ct.concod)
                                 WHERE ct.carcod = dev.carcod
                                   AND ct.devcod = dev.devcod 
                                   AND cd.condetint <> 0
                                   AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                   AND cd.condetint IN (SELECT segfili01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '))';
              END IF;
              contadorsegfilI = contadorsegfilI + 1;     
            END IF;
        
            /* Caso for Numeric */                
            IF COALESCE(_ponteiro.segfilv01,0.00) <> 0.00 THEN
              IF _nivel IN (2, 3) THEN -- Contrato e Parcela
                _query = '
                    AND EXISTS (SELECT NULL
                                  FROM contrato_detalhe cd
                                 WHERE cd.concod = con.concod 
                                   AND cd.condetmoe <> 0.00
                                   AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                   AND cd.condetmoe IN (SELECT segfilv01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfilv01 <> 0.00))';
              ELSE
                _query = '
                    AND EXISTS (SELECT NULL
                                  FROM contrato ct
                                    JOIN contrato_detalhe cd ON (cd.concod = ct.concod)
                                 WHERE ct.carcod = dev.carcod
                                   AND ct.devcod = dev.devcod 
                                   AND cd.condetmoe <> 0.00
                                   AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                   AND cd.condetmoe IN (SELECT segfilv01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfilv01 <> 0.00))';
              END IF;
              contadorsegfilV = contadorsegfilV + 1;
            END IF;
        
            /* Caso for Date */
            IF COALESCE(_ponteiro.segfild01,'0001-01-01') <> '0001-01-01' THEN 
              IF _nivel IN (2, 3) THEN -- Contrato e Parcela
                _query = '
                    AND EXISTS (SELECT NULL
                                  FROM contrato_detalhe cd
                                 WHERE cd.concod = con.concod
                                   AND cd.condetdat <> ''0001-01-01''
                                   AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                   AND cd.condetdat IN (SELECT segfild01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfild01 <> ''0001-01-01'' ))';
              ELSE
                _query = '
                    AND EXISTS (SELECT NULL
                                  FROM contrato ct
                                    JOIN contrato_detalhe cd ON (cd.concod = ct.concod)
                                 WHERE ct.carcod = dev.carcod
                                   AND ct.devcod = dev.devcod 
                                   AND cd.condetdat <> ''0001-01-01''
                                   AND cd.condetcod = '||_ponteiro.segfillegcod||'
                                   AND cd.condetdat IN (SELECT segfild01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfild01 <> ''0001-01-01'' ))';
              END IF;
              contadorsegfilD = contadorsegfilD + 1;
            END IF;
        END LOOP;
    END IF;
 
    /* Detalhes Parcela */
    IF _segfilcod = 26 /*10013*/ THEN
        FOR _ponteiro IN (SELECT DISTINCT ON (segfillegcod) *
                            FROM segmentacaolevel1level2  ssl
                           WHERE ssl.segfilcod = _segfilcod
                             AND ssl.segcod    = _segcod) 
        LOOP
            --Se for VARCHAR
            IF COALESCE(_ponteiro.segfilc01, '') <> '' THEN
              IF _nivel = 3 THEN -- Parcela
                _query = '
                    AND EXISTS(SELECT NULL
                                 FROM contrato_parcela_detalhe cpd
                                WHERE cpd.conparseq = cpa.conparseq
                                  AND cpd.conpardetcar <> ''''
                                  AND  cpd.conpardetcod = (SELECT c.segfillegcod 
                                                             FROM segmentacaolevel1level2  c
                                                            WHERE c.segfilcod = '|| _segfilcod || '
                                                              AND c.segcod    = '|| _segcod    || '
                                                              AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1)
                                  AND UPPER(cpd.conpardetcar) = (SELECT UPPER(segfilc01)
                                                                   FROM segmentacaolevel1level2  c
                                                                  WHERE c.segfilcod = '|| _segfilcod || '
                                                                    AND c.segcod    = '|| _segcod    || '
                                                                    AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1))';
              ELSIF _nivel = 2 THEN -- Contrato
                _query = '
                    AND EXISTS(SELECT NULL 
                                 FROM contrato_parcela cp
                                    JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                                WHERE cp.concod = con.concod
                                  AND cpd.conpardetcar <> ''''
                                  AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                             FROM segmentacaolevel1level2  c
                                                            WHERE c.segfilcod = '|| _segfilcod || '
                                                              AND c.segcod    = '|| _segcod    || '
                                                              AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1)
                                  AND UPPER(cpd.conpardetcar) = (SELECT UPPER(segfilc01)
                                                                   FROM segmentacaolevel1level2  c
                                                                  WHERE c.segfilcod = '|| _segfilcod || '
                                                                    AND c.segcod    = '|| _segcod    || '
                                                                    AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1))';
              ELSE
                _query = '
                    AND EXISTS(SELECT NULL
                                 FROM contrato c
                                    JOIN contrato_parcela cp ON (cp.concod = c.concod)
                                    JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                                WHERE c.carcod = dev.carcod
                                  AND c.devcod = dev.devcod 
                                  AND cpd.conpardetcar <> ''''
                                  AND  cpd.conpardetcod = (SELECT c.segfillegcod 
                                                             FROM segmentacaolevel1level2  c
                                                            WHERE c.segfilcod = '|| _segfilcod || '
                                                              AND c.segcod    = '|| _segcod    || '
                                                              AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1)
                                  AND UPPER(cpd.conpardetcar) = (SELECT UPPER(segfilc01)
                                                                   FROM segmentacaolevel1level2  c
                                                                  WHERE c.segfilcod = '|| _segfilcod || '
                                                                    AND c.segcod    = '|| _segcod    || '
                                                                    AND c.segfilc01 <> '''' OFFSET '||contadorsegfilC||' LIMIT 1))';
              END IF;
                        contadorsegfilC = contadorsegfilC + 1;
            END IF;
                    --Se for INTEGER
            IF COALESCE(_ponteiro.segfili01, 0) <> 0 THEN
              IF _nivel = 3 THEN -- Parcela
                _query = ' 
                  AND EXISTS(SELECT NULL
                               FROM contrato_parcela_detalhe cpd
                              WHERE cpd.conparseq = cpa.conparseq
                                AND cpd.conpardetint <> 0
                                AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1)
                                AND cpd.conpardetint = (SELECT segfili01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '   
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1))';
              ELSIF _nivel = 2 THEN -- Parcela
                _query = ' 
                  AND EXISTS(SELECT NULL
                               FROM contrato_parcela cp
                                  JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                              WHERE cp.concod = con.concod
                                AND cpd.conpardetint <> 0
                                AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1)
                                AND cpd.conpardetint = (SELECT segfili01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '   
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1))';
              ELSE 
                _query = ' 
                  AND EXISTS(SELECT NULL 
                               FROM contrato c
                                  JOIN contrato_parcela cp ON (cp.concod = c.concod)
                                  JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                              WHERE c.carcod = dev.carcod
                                AND c.devcod = dev.devcod 
                                AND cpd.conpardetint <> 0
                                AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1)
                                AND cpd.conpardetint = (SELECT segfili01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '   
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfili01 <> 0 OFFSET '||contadorsegfilI||' LIMIT 1))';
              END IF;
              contadorsegfilI = contadorsegfilI + 1;
            END IF;
            --Se for Numeric
            IF COALESCE(_ponteiro.segfilv01, 0.00) <> 0.00 THEN
              IF _nivel = 3 THEN -- Parcela
                _query = ' 
                    AND EXISTS(SELECT NULL
                                 FROM contrato_parcela_detalhe cpd
                                WHERE cpd.conparseq = cpa.conparseq
                                  AND cpd.conpardetmoe <> 0.00
                                  AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                            FROM segmentacaolevel1level2  c
                                                           WHERE c.segfilcod = '|| _segfilcod || '
                                                             AND c.segcod    = '|| _segcod    || '
                                                             AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1)
                                  AND cpd.conpardetmoe = (SELECT segfilv01
                                                            FROM segmentacaolevel1level2  c
                                                           WHERE c.segfilcod = '|| _segfilcod || '
                                                             AND c.segcod    = '|| _segcod    || '
                                                             AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1))';
              ELSIF _nivel = 2 THEN -- Contrato
                _query = ' 
                    AND EXISTS(SELECT NULL
                                 FROM contrato_parcela cp
                                    JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                                WHERE cp.concod = con.concod
                                AND cpd.conpardetmoe <> 0.00
                                AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1)
                                AND cpd.conpardetmoe = (SELECT segfilv01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1))';
              ELSE
                _query = ' 
                    AND EXISTS(SELECT NULL
                                 FROM contrato c
                                    JOIN contrato_parcela cp ON (cp.concod = c.concod)
                                    JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                                WHERE c.carcod = dev.carcod
                                AND c.devcod = dev.devcod 
                                AND cpd.conpardetmoe <> 0.00
                                AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1)
                                AND cpd.conpardetmoe = (SELECT segfilv01
                                                          FROM segmentacaolevel1level2  c
                                                         WHERE c.segfilcod = '|| _segfilcod || '
                                                           AND c.segcod    = '|| _segcod    || '
                                                           AND c.segfilv01 <> 0.00 OFFSET '||contadorsegfilV||' LIMIT 1))';
              END IF;
              contadorsegfilV = contadorsegfilV + 1;
            END IF;
            --Se for DATA
            IF COALESCE(_ponteiro.segfild01, '0001-01-01') <> '0001-01-01' THEN 
              IF _nivel = 3 THEN -- Parcela
                _query = ' 
                    AND EXISTS(SELECT NULL 
                                 FROM contrato_parcela_detalhe cpd
                                WHERE cpd.conparseq = cpa.conparseq
                                  AND cpd.conpardetdat <> ''0001-01-01''
                                  AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                            FROM segmentacaolevel1level2  c
                                                           WHERE c.segfilcod = '|| _segfilcod || '
                                                             AND c.segcod    = '|| _segcod    || '
                                                             AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1)
                                 AND cpd.conpardetdat = (SELECT segfild01
                                                           FROM segmentacaolevel1level2  c
                                                          WHERE c.segfilcod = '|| _segfilcod || '
                                                            AND c.segcod    = '|| _segcod    || ' 
                                                            AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1))';
              ELSIF _nivel = 2 THEN -- Contrato
                _query = ' 
                    AND EXISTS(SELECT NULL
                                 FROM contrato_parcela cp
                                    JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                                WHERE cp.concod = con.concod
                                  AND cpd.conpardetdat <> ''0001-01-01''
                                  AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                            FROM segmentacaolevel1level2  c
                                                           WHERE c.segfilcod = '|| _segfilcod || '
                                                             AND c.segcod    = '|| _segcod    || '
                                                             AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1)
                                 AND cpd.conpardetdat = (SELECT segfild01
                                                           FROM segmentacaolevel1level2  c
                                                          WHERE c.segfilcod = '|| _segfilcod || '
                                                            AND c.segcod    = '|| _segcod    || ' 
                                                            AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1))';
              ELSE
                _query = ' 
                    AND EXISTS(SELECT NULL
                                 FROM contrato c
                                    JOIN contrato_parcela cp ON (cp.concod = c.concod)
                                    JOIN contrato_parcela_detalhe cpd ON (cpd.conparseq = cp.conparseq)
                                WHERE c.carcod = dev.carcod
                                  AND c.devcod = dev.devcod 
                                  AND cpd.conpardetdat <> ''0001-01-01''
                                  AND cpd.conpardetcod = (SELECT c.segfillegcod 
                                                            FROM segmentacaolevel1level2  c
                                                           WHERE c.segfilcod = '|| _segfilcod || '
                                                             AND c.segcod    = '|| _segcod    || '
                                                             AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1)
                                 AND cpd.conpardetdat = (SELECT segfild01
                                                           FROM segmentacaolevel1level2  c
                                                          WHERE c.segfilcod = '|| _segfilcod || '
                                                            AND c.segcod    = '|| _segcod    || ' 
                                                            AND c.segfild01 <> ''0001-01-01'' OFFSET '||contadorsegfilD||' LIMIT 1))';
              END IF;
              contadorsegfilD = contadorsegfilD + 1;
            END IF;
        END LOOP;
    END IF;

    /* Devedores em Acordo?  */
    IF _segfilcod = 21 /*10014*/ THEN 
      SELECT sll.segfili01, sll.segfilc01
        INTO _inteiro01, _texto01
        FROM segmentacaolevel1level2 sll
       WHERE sll.segfilcod = _segfilcod
         AND sll.segcod    = _segcod;
  
      IF _inteiro01 = 1 OR _texto01 IN ('S','SIM')   THEN
        _query = ' 
        AND EXISTS (SELECT NULL 
                      FROM acordo aco
                      WHERE aco.devcod = dev.devcod
                        AND aco.carcod = dev.carcod
                        AND aco.acoati = 0
                        AND aco.acovalsal > 0 LIMIT 1) ';
      ELSE 
        _query = ' 
        AND NOT EXISTS (SELECT NULL 
                      FROM acordo aco
                      WHERE aco.devcod = dev.devcod
                        AND aco.carcod = dev.carcod
                        AND aco.acoati = 0
                        AND aco.acovalsal > 0) ';
      END IF;
   END IF;

    /* Dias em Atraso Acordo */
    IF _segfilcod = 10002 /*10015*/ THEN 
        SELECT ''''||(CURRENT_DATE - sll.segfili02::int)::TEXT||'''',
               ''''||(CURRENT_DATE - sll.segfili01::int)::TEXT||''''
          INTO _texto01,
               _texto02  
          FROM segmentacaolevel1level2 sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod
         LIMIT 1;
    
        _query = '
              AND EXISTS(SELECT NULL
                           FROM (SELECT min(ap.acopardatven) as dat_venc
                                   FROM acordo a
                                      JOIN acordo_parcela ap ON a.acocod = ap.acocod
                                      JOIN LATERAL (SELECT SUM(apr.acoparretvallan) as valor_pago 
                                                      FROM acordo_parcela_retomada apr 
                                                     WHERE apr.acoparseq = ap.acoparseq 
                                                       AND apr.tipenccod = 1 
                                                       AND apr.acoparretati = 0) xs ON TRUE 
                                  WHERE (ap.acoparvallan - COALESCE(xs.valor_pago, 0)) > 0
                                    AND a.devcod = dev.devcod
                                    AND a.carcod = dev.carcod) x1 
                          WHERE x1.dat_venc  >= '|| _texto01 ||'
                            AND x1.dat_venc  <= '|| _texto02 ||')';
    END IF;
  
    /* Estado Civil */
    IF _segfilcod = 39 /*10016*/ THEN 
    
        SELECT string_agg(''''||sll.segfilc01::TEXT||'''',','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2 sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
       
        IF _count > 1 THEN
            _query = '
                AND pes.pesestciv IN ('|| _string ||') ';
        ELSE 
            _query = '
                AND pes.pesestciv = '|| _string ||' ';
             
        END IF;
    
    END IF;

    /* Usuário da Agenda */
    IF _segfilcod = 713 /*10017*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
             
        IF _count > 1 THEN
            _query = '
                AND ret.usucod IN ('|| _string ||')';
        ELSE 
            _query = '
                AND ret.usucod = '|| _string ||'';
             
        END IF;   
   
    END IF;

    /* Operador da Agenda */
    IF _segfilcod = 10 /*10018*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod    = _segcod;
             
        IF _count > 1 THEN
            _query = '
                AND ret.retusucod IN ('|| _string ||')';
        ELSE 
            _query = '
                AND ret.retusucod = '|| _string ||'';
             
        END IF;
   
    END IF;
    
    /* Complemento */
    IF _segfilcod = 65 /*10019*/ THEN 
        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
       
        IF _count > 1 THEN
            _query = '
                AND ret.sitcomcod IN ('|| _string ||')';
        ELSE
            _query = '
                AND ret.sitcomcod = '|| _string ||'';
             
        END IF;
    END IF;

    /* UF */
    IF _segfilcod = 8 /*10020*/ THEN 

        SELECT string_agg(''''||sll.segfilc01::TEXT||'''',','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
          _query = ' AND EXISTS (SELECT NULL
                                            FROM endereco e 
                                           WHERE e.pescod = pes.pescod
                                             AND e.endsit NOT IN (2, 5)
                                             AND e.endufcod IN ('|| _string ||'))';
        ELSE 
            _query = ' AND EXISTS (SELECT NULL
                                            FROM endereco e 
                                           WHERE e.pescod = pes.pescod
                                             AND e.endsit NOT IN (2, 5)
                                             AND e.endufcod = '|| _string ||' )';
        END IF;   
   
    END IF;

    /* Tipo Pessoa */
    IF _segfilcod = 18 /*10021*/ THEN 

        SELECT string_agg(''''||sll.segfilc01::TEXT||'''',','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
            _query = ' AND pes.pespes in ('||_string||') ';
        ELSE 
            _query = ' AND pes.pespes = '||_string||' ';
        END IF;   
   
    END IF;

    /* Tipo Telefone */
    IF _segfilcod = 26 /*10022*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
          _query = ' AND EXISTS (SELECT NULL
                                            FROM telefone t
                                           WHERE t.pescod = pes.pescod
                                             AND t.tiptelcod IN ('|| _string ||'))';
        ELSE 
            _query = ' AND EXISTS (SELECT NULL
                                            FROM telefone t
                                           WHERE t.pescod = pes.pescod
                                             AND t.tiptelcod = '|| _string ||')';
        END IF;   
   
    END IF;


    /* Tipo Retomada */
    IF _segfilcod = 30 /*10023*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
          _query = ' AND EXISTS (SELECT NULL
                                            FROM operacao o 
                                           WHERE o.devcod = dev.devcod
                                             AND o.opeatican = 0
                                             AND o.tipretcod IN ('|| _string ||'))';
        ELSE 
            _query = ' AND EXISTS (SELECT NULL
                                            FROM operacao o 
                                           WHERE o.devcod = dev.devcod
                                             AND o.opeatican = 0
                                             AND o.tipretcod = '|| _string ||') ';
        END IF;   
   
    END IF;

    /* Tipo Recebimento */
    IF _segfilcod = 31 /*10024*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
          _query = ' AND EXISTS (SELECT NULL
                                            FROM operacao o 
                                           WHERE o.devcod = dev.devcod
                                             AND o.opeatican = 0
                                             AND o.rectipcod IN ('|| _string ||'))';
        ELSE 
            _query = ' AND EXISTS (SELECT NULL
                                            FROM operacao o 
                                           WHERE o.devcod = dev.devcod
                                             AND o.opeatican = 0
                                             AND o.rectipcod = ('|| _string ||'))';
        END IF;   
   
    END IF;

/*    /* Tipo Ocupação */
    IF _segfilcod = 10025 THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
            _query = ' AND EXISTS (SELECT NULL
                                              FROM trabalho t
                                             WHERE t.pescod = pes.pescod
                                               AND t.tipocucod IN ('|| _string ||'))';
        ELSE
            _query = ' AND EXISTS (SELECT NULL
                                              FROM trabalho t
                                             WHERE t.pescod = pes.pescod
                                               AND t.tipocucod = '|| _string ||')';
        END IF;
    END IF;*/

    /* Classificação Telefone */
    IF _segfilcod = 180 /*10026*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
            _query = ' AND EXISTS (SELECT NULL
                                              FROM telefone t
                                             WHERE t.pescod = pes.pescod
                                               AND t.telsit NOT IN (2,5)
                                               AND t.telclas IN ('|| _string ||'))';
        ELSE
            _query = ' AND EXISTS (SELECT NULL
                                              FROM telefone t
                                             WHERE t.pescod = pes.pescod
                                               AND t.telsit NOT IN (2,5)
                                               AND t.telclas = '|| _string ||' )';
        END IF;
    END IF;


    /* Cidade */
    IF _segfilcod = 7 /*10027*/ THEN 

        SELECT string_agg(''''||trim(sll.segfilc01::TEXT)||'''',','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
            _query = ' AND EXISTS (SELECT NULL
                                              FROM endereco e
                                             WHERE e.pescod = pes.pescod
                                               AND e.endsit NOT IN (2,5)
                                               AND e.endcid IN ('|| _string ||'))';
        ELSE
            _query = ' AND EXISTS (SELECT NULL
                                              FROM endereco e
                                             WHERE e.pescod = pes.pescod
                                               AND e.endsit NOT IN (2,5)
                                               AND e.endcid = '|| _string ||' )';
        END IF;
    END IF;


/*    /* Segmento */
    IF _segfilcod = 10028 THEN

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
            _query = ' AND dev.devsegcob IN ('|| _string ||')';
        ELSE
            _query = ' AND dev.devsegcob = '|| _string ||'';
        END IF;
    END IF;*/

    /* Tipo Endereço */
    IF _segfilcod = 41 /*10029*/ THEN 

        SELECT string_agg(sll.segfili01::TEXT,','), count(*)
          INTO _string, _count
          FROM segmentacaolevel1level2  sll
         WHERE sll.segfilcod = _segfilcod
           AND sll.segcod = _segcod;
             
        IF _count > 1 THEN
          _query = ' AND EXISTS (SELECT NULL
                                            FROM endereco e 
                                           WHERE e.pescod = pes.pescod
                                             AND e.endsit NOT IN (2, 5)
                                             AND e.tipendcod IN ('|| _string ||'))';
        ELSE 
            _query = ' AND EXISTS (SELECT NULL
                                            FROM endereco e 
                                           WHERE e.pescod = pes.pescod
                                             AND e.endsit NOT IN (2, 5)
                                             AND e.tipendcod = '|| _string ||' )';
        END IF;   
   
    END IF;

/*
    /* Devedor Randomico */
    IF _segfilcod = 10030 THEN

      SELECT COALESCE(sll.segfilv01,0), COALESCE(sll.segfilv02,0)
        INTO _valor01, _valor02
        FROM segmentacaolevel1level2  sll
       WHERE  sll.segfilcod = _segfilcod
         AND  sll.segcod    = _segcod;

        _query = ' 
            AND dev.devnumran BETWEEN '|| _valor01||' AND '|| _valor02;
    
    END IF;
*/

    /* Clientes com ou sem novação*/
    IF _segfilcod = 10005 /*10031*/ THEN 
      SELECT s.segfilc01
        INTO _texto01 
        FROM segmentacaolevel1level2 s
       WHERE s.segcod = _segcod
         AND s.segfilcod = _segfilcod;
          
      IF _nivel <= 1 THEN          
        IF _texto01 = 'S' THEN 
          _query = '
               AND EXISTS (SELECT NULL
                             FROM contrato con 
                            WHERE con.devcod = dev.devcod 
                              AND con.procod = 28
                              AND con.convalsal > 0)';
        ELSE 
          _query = '
             AND NOT EXISTS (SELECT NULL 
                               FROM contrato con 
                                   JOIN contrato_parcela cp ON con.concod = cp.concod
                                   JOIN credigy_parcela cp2 ON cp2.creparseq = cp.conparseq
                              WHERE con.devcod = dev.devcod
                                AND cp.conparvalsal > 0)';    
        END IF;
   
      ELSEIF _nivel = 2 THEN

        IF _texto01 = 'S' THEN 
           _query = '
             AND con.procod = 28';
        ELSE 
            _query = '
             AND NOT EXISTS (SELECT NULL 
                               FROM contrato con 
                                   JOIN contrato_parcela cp ON con.concod = cp.concod
                                   JOIN credigy_parcela cp2 ON cp2.creparseq = cp.conparseq
                              WHERE con.devcod = dev.devcod
                                AND cp.conparvalsal > 0)';    
        END IF;
      END IF;
    END IF;

    /* Dias de atraso da novação */
    IF _segfilcod = 10004 /*10032*/ THEN        
        SELECT ''''||(current_date - s.segfili02::int)::varchar||'''',
               ''''||(current_date - s.segfili01::int)::varchar||''''
          INTO _texto01,
               _texto02
          FROM segmentacaolevel1level2 s
         WHERE s.segcod = _segcod
           AND s.segfilcod = _segfilcod;
            
         IF _nivel <= 1 THEN 
           _query = '
             AND (SELECT min(conpardatven)
                    FROM contrato con 
                        JOIN contrato_parcela cp ON con.concod = cp.concod
                   WHERE con.devcod = dev.devcod
                     AND cp.conparvalsal > 0
                     AND con.procod = 28) BETWEEN '||_texto01||' AND '||_texto02||' ';   
         ELSEIF _nivel = 2 THEN 
          
           _query = '
             AND (SELECT min(cpa.conpardatven)
                    FROM contrato_parcela cpa 
                   WHERE cpa.concod = con.concod
                     AND cpa.conparvalsal > 0
                     AND con.procod = 28) BETWEEN '||_texto01||' AND '||_texto02||' '; 
         END IF;
        
    END IF;

	IF _segfilcod = 10006 THEN 
		SELECT s.segfili01,
 			   s.segfili02
		  INTO _inteiro01,
	           _inteiro01
		  FROM segmentacaolevel1level2 s 
         WHERE s.segcod = _segcod
           AND s.segfilcod = _segfilcod;

	   _query = '
		 AND current_date - dev.devvenmaisantigo BETWEEN '||_inteiro01||' AND '||_inteiro01||' '; 
	END IF;

    RETURN _query;
END;

$function$
;

CREATE OR REPLACE FUNCTION public.segmentacao_parametros_custom(_segcod bigint, _segfilcod bigint, _nivel integer)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
    _query TEXT = '';
    _string TEXT = '';
    _count integer = 0;
    _inteiro01 integer = 0;
    _inteiro02 integer = 0;
    _texto01 TEXT = '';
    _texto02 TEXT = '';
    _ponteiro record;
    _valor01 NUMERIC(14,2);
    _valor02 NUMERIC(14,2);
    contadorsegfilD integer = 0;
    contadorsegfilC integer = 0;
    contadorsegfilI integer = 0;
    contadorsegfilV integer = 0;
BEGIN
  
    RETURN _query;
END;

$function$
;

INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (1, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (2, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (3, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (5, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (6, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (7, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (8, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (18, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (21, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (24, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (25, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (26, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (30, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (31, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (38, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (39, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (41, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (48, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (65, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (260, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (713, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10000, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10001, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10002, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10003, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10004, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10005, 200) ON CONFLICT DO NOTHING;
INSERT INTO segmentacao_filtro_base (segfilcod, segbasfiltro) VALUES (10006, 200) ON CONFLICT DO NOTHING;

INSERT INTO segmentacao_base_filtro (segbasfiltro, segbasnom, segbastip, segbasusuatu, segbasdatatu, segbasusuinc, segbasdatinc, segbasati, segbasqtdexesim, segbasincarqmon, segbastipexe) 
VALUES(200, 'BASE ARQUIVO DINÂMICA', 'B', 9547, '2025-06-03 10:57:34.000', 9547, '2025-05-13 15:16:28.000', 0, 1, 0, 1) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION cancela_segmentacao(_segcod INT)
RETURNS void 
LANGUAGE SQL 
AS $function$

WITH id AS (
	SELECT pid 
	  FROM pg_stat_activity 
	 WHERE query ILIKE '%DO $$ DECLARE BEGIN PERFORM executa_segmentacao%' 
	   AND query ILIKE '%'||_segcod||'%'
	   AND query NOT LIKE '%pg_cancel_backend%'
)
SELECT pg_cancel_backend(pid) 
  FROM id;
$function$;

CREATE OR REPLACE FUNCTION public.executa_segmentacao(_segcod integer, _segbasfiltro integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    _segcaminho TEXT = '';
    _carcod INT = 0;
BEGIN
 
    SELECT s.segcaminho
      INTO _segcaminho
      FROM segmentacao s
     WHERE s.segcod = _segcod;

    IF _segbasfiltro = 1 THEN
        PERFORM base_filtro_contrato(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 2 THEN
        PERFORM base_filtro_cliente(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 3 THEN
        PERFORM base_filtro_acordo(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 5 THEN
        PERFORM extrator_layout_procob(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 7 THEN
        PERFORM extrator_layout_tactium(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 8 THEN
        PERFORM fu_distribuicao_extrator(_segcod, '1', '1', _segcaminho);
    END IF;

    IF _segbasfiltro = 9 THEN
        PERFORM extrator_layout_ura(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 15 THEN
        PERFORM fu_distribuicao_situacao(_segcod);
    END IF;

    IF _segbasfiltro = 16 THEN
        PERFORM base_filtro_telefones(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 20 THEN
        _segcaminho := substr(CAST(current_timestamp AS VARCHAR), 1, 19);
        PERFORM extrator_geracao_sms_mensagem(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 25 THEN
        PERFORM extrator_layout_ura2(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 34 THEN
        PERFORM extrator_layout_talktelecom(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 41 THEN
        PERFORM extrator_layout_callflex(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 50 THEN
        PERFORM extrator_layout_ura3(_segcod, _segcaminho);
    END IF;

    IF _segbasfiltro = 200 THEN
        PERFORM base_arquivo_dinamico(_segcod, _segcaminho);
    END IF;

END;
$function$
;

INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1000, 200, 'dev.devnom', 'Nome do Devedor', 9547, '2025-05-15 15:36:57.000', 9547, '2025-05-13 15:53:43.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1001, 200, 'dev.devcod', 'Codigo do Devedor', 9547, '2025-05-15 15:36:54.000', 9547, '2025-05-13 15:54:14.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1002, 200, 'formatar_cpf_cnpj(dev.devcpf)', 'CPF do Devedor', 9547, '2025-05-15 15:36:51.000', 9547, '2025-05-13 15:55:28.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1003, 200, 'to_char(dev.devdatinc, ''DD/MM/YYYY HH24:MI:SS'')', 'Data/hora de Inclusão do Devedor', 9547, '2025-05-15 15:36:47.000', 9547, '2025-05-13 15:55:54.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1004, 200, 'formatar_valor(dev.devsal)', 'Saldo do Devedor', 9547, '2025-05-15 15:36:43.000', 9547, '2025-05-13 15:56:43.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1005, 200, 'formatar_valor(devedor_saldo_vencido(dev.devcod, dev.carcod))', 'Saldo Vencido do Devedor', 9547, '2025-05-15 15:36:40.000', 9547, '2025-05-13 15:58:08.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1006, 200, 'dev.carcod', 'Código da Carteira', 9547, '2025-05-15 15:36:36.000', 9547, '2025-05-15 10:43:24.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1007, 200, 'SELECT car.carnom FROM carteira car WHERE car.carcod = dev.carcod', 'Nome da Carteira', 9547, '2025-05-15 15:36:32.000', 9547, '2025-05-15 10:44:09.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1008, 200, 'to_char(dev.devvenmaisantigo, ''DD/MM/YYYY'')', 'Vencimento mais antigo do Devedor', 9547, '2025-05-15 15:36:28.000', 9547, '2025-05-15 14:44:12.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1009, 200, 'to_char(dev.devvenmaisrecente, ''DD/MM/YYYY'')', 'Vencimento mais recente do Devedor', 9547, '2025-05-15 15:36:24.000', 9547, '2025-05-15 14:44:31.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1010, 200, 'dev.devid', 'ID do Devedor', 9547, '2025-05-15 16:31:02.000', 9547, '2025-05-15 16:31:02.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1011, 200, 'formatar_valor(aco.acoval)', 'Valor do Acordo', 9547, '2025-05-15 15:37:21.000', 9547, '2025-05-15 15:37:21.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1012, 200, 'formatar_valor(aco.acovalsal)', 'Saldo do Acordo', 9547, '2025-05-15 15:37:40.000', 9547, '2025-05-15 15:37:40.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1013, 200, 'formatar_valor(aco.acovalacr)', 'Valor de Acrescimo do Acordo', 9547, '2025-05-15 17:15:53.000', 9547, '2025-05-15 15:38:19.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1014, 200, 'CASE WHEN aco.acoati = 0 THEN ''Ativo'' WHEN aco.acoati = 1 THEN ''Inativo'' ELSE '''' END', 'Status do Acordo', 9547, '2025-05-15 16:32:51.000', 9547, '2025-05-15 15:40:01.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1015, 200, 'formatar_valor(valor_pago(3, aco.acocod))', 'Valor total pago do Acordo', 9547, '2025-06-04 08:58:00.000', 9547, '2025-05-15 15:40:25.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1017, 200, 'to_char(aco.acodatcad, ''DD/MM/YYYY'')', 'Data de Inclusao do Acordo', 9547, '2025-05-15 17:15:45.000', 9547, '2025-05-15 15:56:27.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1018, 200, 'con.connumcon', 'Número do Contrato', 9547, '2025-05-15 16:26:48.000', 9547, '2025-05-15 16:26:48.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1019, 200, 'formatar_valor(cp.conparvallan)', 'Valor da Parcela', 9547, '2025-05-15 16:27:31.000', 9547, '2025-05-15 16:27:13.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1020, 200, 'formatar_valor(cp.conparvalsal)', 'Saldo da Parcela', 9547, '2025-05-15 16:27:48.000', 9547, '2025-05-15 16:27:48.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1021, 200, 'to_char(cp.conpardatven, ''DD/MM/YYYY'')', 'Vencimento da Parcela', 9547, '2025-05-15 16:30:46.000', 9547, '2025-05-15 16:28:16.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1022, 200, 'aco.aconumcon', 'Número do Acordo', 9547, '2025-05-15 17:15:59.000', 9547, '2025-05-15 15:13:49.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1023, 200, 'aco.aconumpar', 'Quantidade de Parcelas do Acordo', 9547, '2025-05-15 17:15:59.000', 9547, '2025-05-15 15:13:49.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1024, 200, 'parcelas_pagas(3, aco.acocod)', 'Quantidade de Parcelas Pagas do Acordo', 9547, '2025-06-04 09:50:07.000', 9547, '2025-06-04 08:48:52.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1025, 200, 'parcelas_pagas(2, con.concod)', 'Quantidade de Parcelas Pagas do Contrato', 9547, '2025-06-04 09:50:19.000', 9547, '2025-06-04 08:49:25.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1026, 200, 'parcelas_pagas(1, dev.devcod)', 'Quantidade de Parcelas Pagas do Devedor', 9547, '2025-06-04 09:50:28.000', 9547, '2025-06-04 08:49:39.000', 1);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1027, 200, 'CASE WHEN aco.aconumpar = 1 THEN ''À vista'' ELSE ''Parcelado'' END', 'Tipo de Pagamento (À vista / Parcelado)', 9547, '2025-06-04 10:57:11.000', 9547, '2025-06-04 08:51:23.000', 7);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1028, 200, 'cp.conparnum', 'Número da Parcela', 9547, '2025-06-04 08:51:39.000', 9547, '2025-06-04 08:51:39.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1029, 200, 'cp.conparidt', 'ID da Parcela', 9547, '2025-06-04 08:51:51.000', 9547, '2025-06-04 08:51:51.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1030, 200, 'cp.conpardadadi', 'Dados adicionais da Parcela', 9547, '2025-06-04 08:52:03.000', 9547, '2025-06-04 08:52:03.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1031, 200, 'cp.conparacocod', 'Código do Acordo vinculado', 9547, '2025-06-04 08:52:20.000', 9547, '2025-06-04 08:52:20.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1032, 200, 'valor_pago(2, con.concod)', 'Valor total pago do Contrato', 9547, '2025-06-04 10:57:48.000', 9547, '2025-06-04 09:01:09.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1033, 200, 'valor_pago(1, dev.devcod)', 'Valor total pago do Devedor', 9547, '2025-06-04 09:07:07.000', 9547, '2025-06-04 09:07:07.000', 6);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1034, 200, 'status_parcela(cp.conparseq)', 'Status da Parcela', 9547, '2025-06-04 09:10:13.000', 9547, '2025-06-04 09:10:13.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1035, 200, 'to_char(cp.conpardatcad, ''DD/MM/YYYY'')', 'Data de Cadastro da Parcela', 9547, '2025-06-04 09:10:47.000', 9547, '2025-06-04 09:10:47.000', 3);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1036, 200, 'formatar_valor(con.convalcon)', 'Valor do Contrato', 9547, '2025-06-04 09:14:33.000', 9547, '2025-06-04 09:14:33.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1037, 200, 'formatar_valor(con.convalsal)', 'Saldo do Contrato', 9547, '2025-06-04 09:14:43.000', 9547, '2025-06-04 09:14:43.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1038, 200, 'con.conid', 'ID do Contrato', 9547, '2025-06-04 09:14:54.000', 9547, '2025-06-04 09:14:54.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1039, 200, 'con.connumpar', 'Número de Parcelas', 9547, '2025-06-04 09:15:04.000', 9547, '2025-06-04 09:15:04.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1040, 200, 'con.condadadi', 'Dados adicionais do Contrato', 9547, '2025-06-04 09:15:15.000', 9547, '2025-06-04 09:15:15.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1041, 200, 'CASE WHEN con.conati = 0 THEN ''Ativo'' ELSE ''Inativo'' END', 'Status (Ativo / Inativo)', 9547, '2025-06-04 09:15:43.000', 9547, '2025-06-04 09:15:43.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1042, 200, 'SELECT pro.pronom FROM produto pro WHERE pro.procod = con.procod', 'Produto', 9547, '2025-06-04 09:16:15.000', 9547, '2025-06-04 09:16:15.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1043, 200, 'SELECT fil.filnom FROM filial fil WHERE fil.filcod = con.filcod AND fil.carcod = con.carcod', 'Filial', 9547, '2025-06-04 09:17:39.000', 9547, '2025-06-04 09:17:39.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1044, 200, 'to_char(con.condatcad, ''DD/MM/YYYY'')', 'Data de Cadastro do Contrato', 9547, '2025-06-04 09:18:07.000', 9547, '2025-06-04 09:18:07.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1045, 200, 'to_char(con.condatpredev, ''DD/MM/YYYY'')', 'Data prevista de Devolução', 9547, '2025-06-04 09:18:26.000', 9547, '2025-06-04 09:18:26.000', 2);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1046, 200, 'SELECT to_char(ope.opedatpag, ''DD/MM/YYYY'') FROM operacao ope WHERE ope.devcod = dev.devcod AND ope.carcod = dev.carcod AND ope.opeatican = 0 ORDER BY ope.opedatpag DESC LIMIT 1', 'Data do Pagamento mais Recente', 9547, '2025-06-04 10:59:31.000', 9547, '2025-06-04 09:20:19.000', 6);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1047, 200, 'SELECT to_char(ope.opedatpag, ''DD/MM/YYYY'') FROM operacao ope WHERE ope.devcod = dev.devcod AND ope.carcod = dev.carcod AND ope.opeatican = 0 AND EXTRACT(MONTH FROM ope.opedatpag) = EXTRACT(MONTH FROM current_date) ORDER BY ope.opedatpag DESC LIMIT 1', 'Data do Pagamento no mês atual', 9547, '2025-06-04 10:59:34.000', 9547, '2025-06-04 09:22:18.000', 6);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1048, 200, 'SELECT formatar_valor(ope.opevalrec) FROM operacao ope WHERE ope.devcod = dev.devcod AND ope.carcod = dev.carcod AND ope.opeatican = 0 AND EXTRACT(MONTH FROM ope.opedatpag) = EXTRACT(MONTH FROM current_date) ORDER BY ope.opedatpag DESC LIMIT 1', 'Valor do Pagamento do mês atual', 9547, '2025-06-04 10:59:37.000', 9547, '2025-06-04 09:23:38.000', 6);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1049, 200, 'SELECT formatar_valor(ope.opevalrec) FROM operacao ope WHERE ope.devcod = dev.devcod AND ope.carcod = dev.carcod AND ope.opeatican = 0 ORDER BY ope.opedatpag DESC LIMIT 1', 'Valor do Pagamento mais recente', 9547, '2025-06-04 10:59:40.000', 9547, '2025-06-04 09:24:19.000', 6);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1050, 200, 'telefone[1]', 'Telefone 1 (Mais atualizado)', 9547, '2025-06-04 09:30:49.000', 9547, '2025-06-04 09:30:49.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1051, 200, 'telefone[2]', 'Telefone 2', 9547, '2025-06-04 09:30:56.000', 9547, '2025-06-04 09:30:56.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1052, 200, 'telefone[3]', 'Telefone 3', 9547, '2025-06-04 09:31:07.000', 9547, '2025-06-04 09:31:07.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1053, 200, 'telefone[4]', 'Telefone 4', 9547, '2025-06-04 09:31:13.000', 9547, '2025-06-04 09:31:13.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1054, 200, 'telefone[5]', 'Telefone 5', 9547, '2025-06-04 09:31:22.000', 9547, '2025-06-04 09:31:22.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1055, 200, 'telefone[6]', 'Telefone 6', 9547, '2025-06-04 09:31:28.000', 9547, '2025-06-04 09:31:28.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1056, 200, 'telefone[7]', 'Telefone 7', 9547, '2025-06-04 09:31:45.000', 9547, '2025-06-04 09:31:45.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1057, 200, 'telefone[8]', 'Telefone 8', 9547, '2025-06-04 09:31:53.000', 9547, '2025-06-04 09:31:53.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1058, 200, 'telefone[9]', 'Telefone 9', 9547, '2025-06-04 09:32:01.000', 9547, '2025-06-04 09:32:01.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1059, 200, 'telefone[10]', 'Telefone 10', 9547, '2025-06-04 09:32:10.000', 9547, '2025-06-04 09:32:10.000', 4);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1060, 200, 'logradouro[1]', 'Endereço mais atualizado - Logradouro', 9547, '2025-06-04 09:46:03.000', 9547, '2025-06-04 09:46:03.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1061, 200, 'bairro[1]', 'Endereço mais atualizado - Bairro', 9547, '2025-06-04 09:46:13.000', 9547, '2025-06-04 09:46:13.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1062, 200, 'complemento[1]', 'Endereço mais atualizado - Complemento', 9547, '2025-06-04 09:46:21.000', 9547, '2025-06-04 09:46:21.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1063, 200, 'cep[1]', 'Endereço mais atualizado - CEP', 9547, '2025-06-04 09:46:56.000', 9547, '2025-06-04 09:46:56.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1064, 200, 'cidade[1]', 'Endereço mais atualizado - Cidade', 9547, '2025-06-04 09:47:07.000', 9547, '2025-06-04 09:47:07.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1065, 200, 'uf[1]', 'Endereço mais atualizado - UF', 9547, '2025-06-04 09:47:25.000', 9547, '2025-06-04 09:47:25.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1066, 200, 'logradouro[2]', 'Endereço 2 - Logradouro', 9547, '2025-06-04 09:47:36.000', 9547, '2025-06-04 09:47:36.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1067, 200, 'bairro[2]', 'Endereço 2 - Bairro', 9547, '2025-06-04 09:47:45.000', 9547, '2025-06-04 09:47:45.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1068, 200, 'complemento[2]', 'Endereço 2 - Complemento', 9547, '2025-06-04 09:47:54.000', 9547, '2025-06-04 09:47:54.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1069, 200, 'cep[2]', 'Endereço 2 - CEP', 9547, '2025-06-04 09:48:02.000', 9547, '2025-06-04 09:48:02.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1070, 200, 'cidade[2]', 'Endereço 2 - Cidade', 9547, '2025-06-04 09:48:11.000', 9547, '2025-06-04 09:48:11.000', 5);
INSERT INTO base_filtro (basfilseq, segbasfiltro, segfilcol, segfilcoldes, basfilusuatu, basfildatatu, basfilusuinc, basfildatinc, basfilniv) VALUES(1071, 200, 'uf[2]', 'Endereço 2 - UF', 9547, '2025-06-04 09:48:20.000', 9547, '2025-06-04 09:48:20.000', 5);


ALTER TABLE segmentacao_campo ADD CONSTRAINT segcod_fkey FOREIGN KEY (segcod) REFERENCES segmentacao(segcod) ON DELETE CASCADE;
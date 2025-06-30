CREATE FUNCTION parcelas_pagas(_tipo int, _identificador int8)
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
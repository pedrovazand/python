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

DROP TRIGGER ativa_inativa_devedor ON devedor;

CREATE TRIGGER ativa_inativa_devedor BEFORE
UPDATE
    ON
    public.devedor FOR EACH ROW EXECUTE PROCEDURE devedor_devati();

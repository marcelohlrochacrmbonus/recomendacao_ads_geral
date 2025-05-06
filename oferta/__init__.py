import logging
import azure.functions as func
import json
import os
import re
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Azure Function 'oferta' iniciada.")

    try:
        import clickhouse_connect

        clickhouse_host = os.getenv('CLICKHOUSE_HOST')
        clickhouse_user = os.getenv('CLICKHOUSE_USER')
        clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')

        logging.info(f"[DEBUG] CLICKHOUSE_HOST={clickhouse_host}")
        logging.info(f"[DEBUG] CLICKHOUSE_USER={clickhouse_user}")

        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            user=clickhouse_user,
            password=clickhouse_password,
            secure=True,
            connect_timeout=5
        )
    except Exception as e:
        logging.error(f"[ClickHouse Init] Erro ao inicializar cliente: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Erro ao inicializar o cliente do ClickHouse."}),
            mimetype="application/json",
            status_code=500
        )

    campanha = req.params.get('campanha')
    celular = req.params.get('celular')
    local_id = req.params.get('local_id')
    genero = req.params.get('genero')
    nascimento = req.params.get('nascimento')

    if not campanha or not celular or local_id is None:
        try:
            req_body = req.get_json()
            campanha = campanha or req_body.get('campanha')
            celular = celular or req_body.get('celular')
            local_id = local_id or req_body.get('local_id')
            genero = genero or req_body.get('genero')
            nascimento = nascimento or req_body.get('nascimento')
        except ValueError:
            pass

    if celular:
        celular = re.sub(r'\D', '', celular)

    if not campanha or not celular or local_id is None:
        return func.HttpResponse(
            json.dumps({"error": "Parâmetros 'campanha', 'celular' e 'local_id' são obrigatórios."}),
            mimetype="application/json",
            status_code=400
        )

    try:
        local_id = int(local_id)
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "O parâmetro 'local_id' deve ser um número válido."}),
            mimetype="application/json",
            status_code=400
        )

    try:
        consulta = f'''
            WITH 
            r1 AS (
                SELECT DISTINCT ordem+100 AS ordem, pangeia_offer_id AS oferta
                FROM recomendacao_ads.geral_cliente
                WHERE campaign_id = '{campanha}' 
                AND celular = '{celular}'
                AND local_id = {local_id}  
            ),
            r2 AS (
                SELECT DISTINCT ordem+200 AS ordem, pangeia_offer_id AS oferta
                FROM recomendacao_ads.geral_cliente_segmento
                WHERE campaign_id = '{campanha}'
                AND celular = '{celular}'
                AND local_id = {local_id}
                AND pangeia_offer_id NOT IN (SELECT oferta FROM r1)
            ),
            r3 AS (
                SELECT ordem+300 AS ordem, pangeia_offer_id AS oferta
                FROM recomendacao_ads.geral_perfil
                WHERE campaign_id = '{campanha}' 
                AND local_id = {local_id}
        '''

        if genero is None:
            consulta += " AND genero IS NULL"
        else:
            consulta += f" AND genero = '{genero}'"

        # ✅ Aqui está o único trecho ajustado para tratar nascimento inválido
        try:
            if nascimento and nascimento != "0000-00-00":
                nascimento_dt = datetime.strptime(nascimento, "%Y-%m-%d")
                idade = round((datetime.today() - nascimento_dt).days / 365)

                faixa = (
                    'F1' if idade <= 27 else
                    'F2' if idade <= 37 else
                    'F3' if idade <= 47 else
                    'F4'
                )
                consulta += f" AND faixa_etaria = '{faixa}'"
            else:
                consulta += " AND faixa_etaria IS NULL"
        except Exception as e:
            logging.warning(f"[NASCIMENTO] Ignorado por formato inválido: {nascimento} ({e})")
            consulta += " AND faixa_etaria IS NULL"

        consulta += f'''
                AND pangeia_offer_id NOT IN (SELECT oferta FROM r1 UNION ALL SELECT oferta FROM r2)
            ),
            r AS (
                SELECT * FROM r1
                UNION ALL
                SELECT * FROM r2 
                UNION ALL
                SELECT * FROM r3
                UNION ALL
                SELECT ordem, pangeia_offer_id 
                FROM recomendacao_ads.geral_ofertas_priorizacao
                WHERE local_id = {local_id}  
                AND pangeia_offer_id NOT IN (SELECT DISTINCT oferta FROM r1)
                AND pangeia_offer_id NOT IN (SELECT DISTINCT oferta FROM r2)
                AND pangeia_offer_id NOT IN (SELECT DISTINCT oferta FROM r3)
            )
            SELECT row_number() OVER(ORDER BY ordem) AS ordem, oferta 
            FROM r
        '''

        logging.info(f"Query construída:\n{consulta}")

        rows = client.query(consulta)
        columns = rows.column_names
        data = [dict(zip(columns, row)) for row in rows.result_set]

        return func.HttpResponse(
            body=json.dumps(data, separators=(',', ':')),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Erro ao executar a consulta no ClickHouse: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Erro ao executar a consulta: {str(e)}"}),
            mimetype="application/json",
            status_code=500
        )

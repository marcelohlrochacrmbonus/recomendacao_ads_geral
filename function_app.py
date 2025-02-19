import azure.functions as func
import logging
import clickhouse_connect
import json
import os
import re
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# Inicializa o cliente ClickHouse
client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST'),
    user=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD'),
    secure=True,
    connect_timeout=5
)

# Inicializa a Azure Function
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="oferta")
def oferta(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Azure Function 'oferta' iniciada.")

    # Obtém os parâmetros da requisição
    campanha = req.params.get('campanha')
    celular = req.params.get('celular')
    local_id = req.params.get('local_id')  # Corrigido para garantir que venha corretamente
    genero = req.params.get('genero')
    nascimento = req.params.get('nascimento')

    # Captura parâmetros do corpo da requisição, se não vierem na URL
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

    # Limpa o número do celular (remove caracteres não numéricos)
    if celular:
        celular = re.sub(r'\D', '', celular)

    # Se não houver campanha, celular ou local_id, retorna erro
    if not campanha or not celular or local_id is None:
        return func.HttpResponse(
            json.dumps({"error": "Parâmetros 'campanha', 'celular' e 'local_id' são obrigatórios."}),
            mimetype="application/json",
            status_code=400
        )

    # Certifica que local_id é um número válido
    try:
        local_id = int(local_id)
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "O parâmetro 'local_id' deve ser um número válido."}),
            mimetype="application/json",
            status_code=400
        )

    try:
        # Construção da query para ClickHouse
        consulta = f'''
            WITH 
            r1 AS
            (
                SELECT DISTINCT ordem+100 AS ordem, pangeia_offer_id AS oferta
                FROM recomendacao_geral.cliente
                WHERE campaign_id = '{campanha}' 
                AND celular = '{celular}'
                AND local_id = {local_id}  
            ),
            r2 AS
            (
                SELECT DISTINCT ordem+200 AS ordem, pangeia_offer_id AS oferta
                FROM recomendacao_geral.cliente_segmento
                WHERE campaign_id = '{campanha}'
                AND celular = '{celular}'
                AND local_id = {local_id}  
                AND pangeia_offer_id NOT IN (SELECT oferta FROM r1)
            ),
            r3 AS
            (
                SELECT ordem+300 AS ordem, pangeia_offer_id AS oferta
                FROM recomendacao_geral.perfil
                WHERE campaign_id = '{campanha}' 
                AND local_id = {local_id}  
        '''

        if genero is None:
            consulta += " AND genero IS NULL"
        else:
            consulta += f" AND genero = '{genero}'"

        if nascimento is None:
            consulta += " AND faixa_etaria IS NULL"
        else:
            nascimento = datetime.strptime(nascimento, "%Y-%m-%d")
            dias = datetime.today() - nascimento
            idade = round(dias.days / 365, 0)

            if idade <= 27:
                nascimento = 'F1'
            elif 28 <= idade <= 37:
                nascimento = 'F2'
            elif 38 <= idade <= 47:
                nascimento = 'F3'
            else:
                nascimento = 'F4'

            consulta += f" AND faixa_etaria = '{nascimento}'"

        consulta += f'''
                AND pangeia_offer_id NOT IN (SELECT oferta FROM r1 UNION ALL SELECT oferta FROM r2)
            ),
            r AS 
            (
                SELECT * FROM r1
                UNION ALL
                SELECT * FROM r2 
                UNION ALL
                SELECT * FROM r3
                UNION ALL
                SELECT ordem, pangeia_offer_id 
                FROM recomendacao_geral.ofertas_priorizacao
                WHERE local_id = {local_id}  
                AND pangeia_offer_id NOT IN (SELECT DISTINCT oferta FROM r1)
                AND pangeia_offer_id NOT IN (SELECT DISTINCT oferta FROM r2)
                AND pangeia_offer_id NOT IN (SELECT DISTINCT oferta FROM r3)
            )
            SELECT row_number() OVER(ORDER BY ordem) AS ordem, oferta 
            FROM r
        '''

        logging.info(f"Query construída:\n{consulta}")

        # Executa a query no ClickHouse
        rows = client.query(consulta)

        # Processa os resultados
        columns = rows.column_names
        data = [dict(zip(columns, row)) for row in rows.result_set]

        # Converte para JSON e retorna
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

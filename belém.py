import datetime
from urllib.parse import urlencode

import scrapy

from gazette.items import Gazette
from gazette.spiders.base import BaseGazetteSpider


class PaBelemSpider(BaseGazetteSpider):
    TERRITORY_ID = "1501402"
    name = "pa_belem"
    allowed_domains = ["sistemas.belem.pa.gov.br"]
    start_date = datetime.date(2005, 2, 1) #data inicial do diário oficial, pode ser alterado na chamada por terminal (regra de precedência do scrapy)
    download_file_headers = {"Accept": "application/octet-stream"} #tipo de arquivo aceitado como download, protocolo de correio no formato MIME.

    BASE_URL = "https://sistemas.belem.pa.gov.br/diario-consulta-api/diarios" #Nota, utiliza-se API no código.

    def start_requests(self): 
        #strftime: converte a data e hora para string no formato definido (no caso, Ano-mês-diaThora) 
        initial_date = self.start_date.strftime("%Y-%m-%dT00:00:00.000Z")
        end_date = datetime.date.today().strftime("%Y-%m-%dT00:00:00.000Z")

        #params recebe os parâmetros a serem passados na url(datas e start), precisam ser encodadas antes disso
        params = {
            "dataRecebidoInicio": initial_date,
            "dataRecebidoFim": end_date,
            "start": "0",
        }
        encoded_params = urlencode(params)
        url = f"{self.BASE_URL}?{encoded_params}"
        #scrapy.Request faz a requisição para a url e passa a resposta para a função callback
        yield scrapy.Request(url, callback=self.parse_get_number_of_items)


    def parse_get_number_of_items(self, response):
        #esta função pega o número de documentos encontrados no periodo definido, passa ele na url e faz a requisição destes documentos 
        number_of_documents = response.json()["response"]["numFound"]
        url = f"{self.BASE_URL}?start=0&rows={number_of_documents}"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        #recebe a resposta da função anterior
        data = response.json()["response"]

        #para cada documento há um objeto dentro de uma list chamada docs (possivel verificar colocando a base url no browser), este 
        #for pega as informações de cada objeto e a prepara
        for gazette_data in data["docs"]:
            #a data de publicação está em string, então é convertida para date com um formato definido
            gazette_date = datetime.datetime.strptime(
                gazette_data["data_publicacao"], "%Y-%m-%dT%H:%M:%SZ"
            ).date()
            #recebe o número da edição do diário
            edition_number = gazette_data["id"]

            #manipula a url do download com os dados conseguidos pela API, assim permitindo o download direto.
            url = f"{self.BASE_URL}/{edition_number}"
            #por fim o comando yield faz o download, passando as informações para a classe Gazette (locallizada em items.py)
            yield Gazette(
                date=gazette_date,
                edition_number=edition_number,
                file_urls=[url],
                is_extra_edition=False,
                power="executive",
            )

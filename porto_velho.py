import datetime as dt
import json

from dateparser import parse
from dateutil.rrule import MONTHLY, rrule
from scrapy.http import Request
from scrapy.selector import Selector

from gazette.items import Gazette
from gazette.spiders.base import BaseGazetteSpider


class RoPortoVelho(BaseGazetteSpider):
    TERRITORY_ID = "1100205"
    BASE_URL = "https://www.portovelho.ro.gov.br/dom/datatablearquivosmes/"#Não há uso de API, portanto se utiliza uma variação de Selector (Scrapy)
    AVAILABLE_FROM = dt.datetime(2007, 1, 1) #Data inicial fornecida pelo diário eletrônico

    name = "ro_porto_velho"
    allowed_domains = ["portovelho.ro.gov.br"]

    def start_requests(self):
        #O autor criou uma frequência que se repete em meses para vasculhar os documentos no diário eletrônico, o -1 inverte 
        #a ordem para ficar o mais recente por primeiro (dezembro até janeiro)
        interval = rrule(MONTHLY, dtstart=self.AVAILABLE_FROM, until=dt.date.today())[
            ::-1
        ]
        for date in interval:
            #requisição do scrapy para poder vasculhar a página com Selector
            yield Request(f"{self.BASE_URL}{date.year}/{date.month}")

    def parse(self, response):
        #Após o yield, o programa vem automaticamente para o parse.
        paragraphs = json.loads(response.body_as_unicode())["aaData"]
        #json.loads retorna um objeto python a partir da lista Json "aaData"
        #E então é criado um laço de cada paragrafo que designa um documento.
        for paragraph, *_ in paragraphs:
            selector = Selector(text=paragraph)
            #Uso do Selector no texto a partir de CSS, pegando um paragrafo 'p' e ancora 'a' com o link 'href'.
            #Dessa forma se extrai o link de download da página html.
            url = selector.css("p a ::attr(href)").extract_first()
            #Mesma ideia do link anterior, só que agora com o título do documento.
            text = selector.css("p strong ::text")
            #Edição é auto-explicativa, é relevante para várias versões ou complementos de uma publicação do diário.
            is_extra_edition = text.extract_first().startswith("Suplemento")
            #Aqui se trabalha Regex para buscar argumentos (números) que se encaixam no formato de data:
            #A primeira busca é de minimo 1 e máximo 2 números, indicando um dia; também pode-se notar que está na ordem dia/mes/ano;
            #A segunda é uma forma de indicar a posição do mês dentro da página;
            #Por fim, se pega os 4 dígitos do ano.
            date = text.re_first(r"\d{1,2} de \w+ de \d{4}")
            date = parse(date, languages=["pt"]).date()
            
            #Com todos os dados necessários, o spider faz o crawling nos campos indicados.
            yield Gazette(
                date=date,
                file_urls=[url],
                is_extra_edition=is_extra_edition,
                power="executive_legislative",
            )

import json # Para poder trabajar con objetos JSON

from elasticsearch import Elasticsearch
from elasticsearch import helpers

def main():
    # Password para el usuario 'lectura' asignada por nosotros
    #
    READONLY_PASSWORD = "17VsbJJf0y*71FJ9YcZB"

    # Creamos el cliente y lo conectamos a nuestro servidor
    #
    global es

    es = Elasticsearch(
        "https://localhost:9200",
        ca_certs="./elasticsearch-8.5.1/config/certs/http_ca.crt",
        basic_auth=("elastic", READONLY_PASSWORD),
    )

    # Lanzamos el scaneo
    results = helpers.scan(es,
        index="tweets-20090624-20090626-en_es-10percent",
        query={
            "query": {
                "range": {
                    "created_at": {
                        "gt": "Wed Jun 24 00:00:00 +0000 2009",
                        "lt": "Wed Jun 24 01:00:00 +0000 2009"
                    }
                }
            },
            "aggs": {
                "trendingtopics": {
                    "significant_terms": {
                        "field": "text",
                        "size": 5
                    }
                }
            }

        }
    )

    from pprint import pprint

    for hit in results:
        pprint(hit)

#    f=open("scan-dump.txt","wb")

    # Iteramos sobre los resultados, no es preciso preocuparse de las
    # conexiones consecutivas que hay que hacer con el servidor ES
#    for hit in results:
#        text = hit["_source"]["text"]

        # Para visualizar mejor los tuits se sustituyen los saltos de línea
        # por espacios en blanco *y* se añade un salto de línea tras cada tuit
#        text = text.replace("\n"," ")+"\n"
#        f.write(text.encode("UTF-8"))

 #   f.close()

if __name__ == '__main__':
    main()
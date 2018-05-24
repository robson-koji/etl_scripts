# -*- coding: utf-8 -*-

"""
Este script faz o downloa dos arquivos do INEP e descompacta-os na pasta dos respectivos anos.
Deve-se escolher o ano de inicio e o ano final dos arquivos a serem baixados.
Antes de 2009, a nomenclatura e formato dos arquivos nao batem.
"""
from urlparse import urlparse, parse_qs

import os, errno
import requests
import zipfile
import urllib
import re

ano_inicio = 2009
ano_fim = 2010
dir_destino = '/var/tmp/inep/docentes/arquivos/'

while ano_inicio < ano_fim:
    print ano_inicio

    ano_str = str(ano_inicio)
    nome_arquivo = ano_str + '.zip'

    # Fazendo o download do arquivo
    url = 'http://download.inep.gov.br/microdados/microdados_censo_superior_' + nome_arquivo
    resp = requests.get(url)

    # Criando o diretorio de destino dos arquivos baixados
    try:
        os.makedirs(dir_destino)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # Gravando o arquivo zip
    with open(nome_arquivo, 'wb') as f:
        f.write(resp.content)

    # Descompactando o arquivo zip
    zip_ref = zipfile.ZipFile(nome_arquivo, 'r')
    zip_ref.extractall(dir_destino)
    zip_ref.close()

    # Apagando o arquivo zip
    os.remove(nome_arquivo)

    ano_inicio += 1

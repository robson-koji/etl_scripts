# -*- coding: utf-8 -*-

"""
Esse script captura os dados do arquivo de microdados de IES e de DOCENTES (vinculos) do censo do INEP
e converte para um csv que pode ser utilizado em uma collection do Solr schemaless.
O CSV gerado concatena os dados da IES no registro do vinculo para facilitar as buscas no Solr.

A estrutura de arquivos descompactados no diretorio PATH_ORIGEM tem os arquivos de IES e DOCENTES em formato .rar
Os mesmos precisam ser desempacotados manualmente:
unrar e  DM_DOCENTE.rar
unrar e  DM_IES.rar

Para otimizar o processamento, organizar o CSV original pela oitava coluna CO_DOCENTE mantando o header no topo.
( head -n1 DM_DOCENTE.CSV; tail -n+2 DM_DOCENTE.CSV | sort -n --field-separator='|' --key=9 ) > DM_DOCENTE_SORTED.CSV

Existe uma questao de um incremento de alguns bilhoes em um campo identificador. Para nao tornar o processamento
extremamente lento, uma range imensa de ids estah sendo desconsiderada por nao conter nada. Hah um risco aqui de
colocarem algum dado nessa range que serah desconsiderado.

Codificacao de saida eh latin-1 (ISO-8859-1)
"""



import codecs
import csv
import os



"""
Criacao e atulizacao de arquivo CSV de destino.
Talvez passar isso para um classe generica.
"""
csv.register_dialect('excel-inep', delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

def cria_csv(ano, cabecalho):
    nome_csv = 'inep_docentes_vinculos' + ano + '.csv'
    with open(nome_csv, 'wb') as create_file:
        dict_writer = csv.DictWriter(create_file, cabecalho, dialect='excel-inep')
        dict_writer.writeheader()

def atualiza_csv(ano, dados, cabecalho):
    nome_csv = 'inep_docentes_vinculos' + ano + '.csv'
    with open(nome_csv, 'a') as atualizar_file:
        dict_writer = csv.DictWriter(atualizar_file, fieldnames=cabecalho, dialect='excel-inep')
        dict_writer.writerows(dados)



"""
Variaveis para conversao de nomes dos campos que estao codificados
"""
CHAVES_SIM_NAO = ['IN_CAPITAL_IES','IN_DEF_CEGUEIRA','IN_DEF_BAIXA_VISAO','IN_DEF_SURDEZ','IN_DEF_AUDITIVA','IN_DEF_FISICA','IN_DEF_SURDOCEGUEIRA','IN_DEF_MULTIPLA','IN_DEF_INTELECTUAL',
                    'IN_ATU_EAD', 'IN_ATU_POS_EAD', 'IN_ATU_EXTENSAO','IN_ATU_GESTAO','IN_ATU_GRAD_PRESENCIAL','IN_ATU_GRAD_PRESENCIAL','IN_ATU_POS_PRESENCIAL','IN_ATU_SEQUENCIAL','IN_ATU_PESQUISA',
                    'IN_BOLSA_PESQUISA','IN_SUBSTITUTO','IN_EXERCICIO_DT_REF','IN_VISITANTE']
SIM_NAO = {'0':'Não', '1':'Sim'}
ESCOLARIDADE = {'1':'Sem graduação','2':'Graduação','3':'Especialização','4':'Mestrado','5':'Doutorado'}
DEFICIENCIA_FISICA = {'0':'Não', '1':'Sim', '2':'Não dispõe de informação'}
IN_VISITANTE_IFES_VINCULO  = {'1':'Em folha', '2':'Bolsista'}

def converte_SIM_NAO(docente):
    """ Converte os codigos 0/1 para texto sim/nao """
    for key in docente:
        if key in CHAVES_SIM_NAO:
            if not docente[key]:
                continue
            # Para padronizar pq o CSV de origem eh latin-1
            docente[key] = SIM_NAO[docente[key]].decode('utf-8').encode('latin-1')
    return docente


# Pasta de origem, onde o download.py gravou os arquivos.
PATH_ORIGEM = '/var/tmp/inep/docentes/arquivos/'

def pega_arquivo_por_ano(ano):
    """ Para cada ano solicitado, retorna dict com o csv de docentes e csv de ies. """
    path_ano = PATH_ORIGEM + ano + '/DADOS/'
    arquivos = {'docentes':'', 'ies':''}

    for arquivo_fs in os.listdir(path_ano):
        arquivo_path = path_ano + arquivo_fs

        # Nao usa o with open pq precisa manipular o arquivo fora.
        arquivo = codecs.open(arquivo_path, 'r')#, encoding='latin-1')

        # !!! O script procura o arquivo CSV ordenado.
        # ( head -n1 DM_DOCENTE.CSV; tail -n+2 DM_DOCENTE.CSV | sort -n --field-separator='|' --key=9 ) > DM_DOCENTE_SORTED.CSV
        if arquivo_fs == 'DM_DOCENTE_SORTED.CSV':
            arquivos['docentes'] = csv.DictReader(arquivo, delimiter='|')
        elif arquivo_fs == 'DM_IES.CSV':
            arquivos['ies'] = csv.DictReader(arquivo, delimiter='|')
    return arquivos






def adiciona_instituicao(ano, lista_docentes, dict_ies):
    csv_lista_final = []
    for docente in lista_docentes:
        # Merge de docente com ies
        if 'CO_DOCENTE_IES' in docente:
            chave = docente['CO_DOCENTE_IES']
        else:
            chave = docente['CO_DOCENTE']
        docente['id'] = ano + '_' + chave
        docente['ano_vigencia_inep'] = ano

        # Converte de codigo para texto
        docente['CO_ESCOLARIDADE_DOCENTE'] = ESCOLARIDADE[docente['CO_ESCOLARIDADE_DOCENTE']].decode('utf-8').encode('latin-1')
        docente['IN_DOCENTE_DEFICIENCIA'] = DEFICIENCIA_FISICA[docente['IN_DOCENTE_DEFICIENCIA']].decode('utf-8').encode('latin-1')
        if 'IN_VISITANTE_IFES_VINCULO' in docente and docente['IN_VISITANTE_IFES_VINCULO']:
            docente['IN_VISITANTE_IFES_VINCULO'] = IN_VISITANTE_IFES_VINCULO[docente['IN_VISITANTE_IFES_VINCULO']].decode('utf-8').encode('latin-1')
        docente = converte_SIM_NAO(docente)

        docente.update(dict_ies[docente['CO_IES']])
        csv_lista_final.append(docente)
    return csv_lista_final



def pega_cabecalho(ano):
    """ Cria um cabecalho que tenha os campos de todos os CSVS  """
    cabecalho = []
    arquivos = pega_arquivo_por_ano(ano)
    if arquivos['docentes']:
        cabecalho += arquivos['docentes'].fieldnames
    if arquivos['ies']:
        cabecalho += arquivos['ies'].fieldnames

    # Faz rearranjo para montar cabecalho com dados consolidade de IES para o facet.
    # Exclui os campos que foram agregados e inclui os campos que agregam os anteriores.
    menos_cabecalho_ies = []

    # id para o Solr. O identificador unico eh o CO_DOCENTE_IES
    # 'GEOGRAFICO' e'MANT_IES' sao dados agregados para facilitar o faceting.
    mais_cabecalho_ies = ['GEOGRAFICO', 'MANT_IES', 'id', 'ano_vigencia_inep']

    cabecalho = list(set(cabecalho) - set(menos_cabecalho_ies))
    cabecalho += mais_cabecalho_ies
    return cabecalho



def trata_ies(ano):
    """
    Cria dict para ser utilizado na desnormalizacao de docentes.
    Organiza os campos de localizacao e de mantenedora/instituicao para facet no Solr.
    """
    dict_ies = {}

    dict_ies[ano] = {}
    arquivos = pega_arquivo_por_ano(ano)
    if arquivos['ies']:
        cabecalho = arquivos['ies'].fieldnames
        for row in arquivos['ies']:
            row['GEOGRAFICO'] = row['NO_REGIAO_IES'] + '|' + row['SGL_UF_IES'] + '|' + row['NO_MUNICIPIO_IES']
            row['MANT_IES'] = row['NO_MANTENEDORA'] + '|' + row['NO_IES']

            row['GEOGRAFICO'] = row['GEOGRAFICO'].replace('"', '')
            row['MANT_IES'] = row['MANT_IES'].replace('"', '')

            entriesToRemove = ['NO_IES']

            for k in entriesToRemove:
                row.pop(k, None)
            dict_ies[ano][row['CO_IES']] = row

    return dict_ies


LIMITE = 100000000


if __name__ == "__main__":
    anos = os.listdir(PATH_ORIGEM)
    anos.sort()

    for ano in anos:
        print ano

        id_inicial = 1

        # Verifica incremento de bilhos no id do docente
        teste_bilhao = False

        # Cria arquivo csv
        cabecalho = pega_cabecalho(ano)
        cria_csv(ano, cabecalho)

        # Pega o arquivo IES para agregar aos docentes
        dict_ies = trata_ies(ano)
        arquivo = pega_arquivo_por_ano(ano)

        # Atualmente o id CO_DOCENTE_IES nao passa de 119 bilhoes.
        # Tem uma margem de aproximadamente 100 milhoes de registros para crescer,
        # caso os ids sejam incrementados sem grandes pulos.
        while (id_inicial < 120000000000):
            if arquivo['docentes']:
                csv_lista_final = adiciona_instituicao(ano, arquivo['docentes'], dict_ies[ano])
                atualiza_csv(ano, csv_lista_final, cabecalho)

            # incrementa o id inicial de acordo com o LIMITE estabelecido.
            id_inicial += LIMITE

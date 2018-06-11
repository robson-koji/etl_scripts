# -*- coding: utf-8 -*-
import codecs
import csv
import os

"""Atenção! : Arquivos abertos com 'open' não estão sendo fechados"""

PATH = '/var/tmp/inep_anos/'

csv.register_dialect('excel-inep', delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')


def cria_csv(ano, cabecalho):
    nome_csv = 'inep_alunos_' + ano + '_.csv'
    with open(nome_csv, 'wb') as create_file:
        dict_writer = csv.DictWriter(create_file, cabecalho, dialect='excel-inep')
        dict_writer.writeheader()

def atualiza_csv(ano, dados, cabecalho):
    nome_csv = 'inep_alunos_' + ano + '_.csv'
    with open(nome_csv, 'a') as atualizar_file:

        dict_writer = csv.DictWriter(atualizar_file, fieldnames=cabecalho, dialect='excel-inep')
        dict_writer.writerows(dados)

def pega_arquivo_por_ano(ano):
    """ Para cada ano solicitado, retorna dict com o csv de alunos e csv de ies. """

    """
    !!!!
    Para funcionar por ano, precisa passar o ano na geracao do arquivo e gravar para cada ano.
    """

    path_ano = PATH + ano + '/DADOS/'
    arquivos = {'alunos':'', 'ies':''}

    print ano
    print path_ano

    for arquivo_fs in os.listdir(path_ano):
        arquivo_path = path_ano + arquivo_fs

        # Nao usa o with open pq precisa manipular o arquivo fora.
        #usar mecanismo yeld para fechar file
        arquivo = codecs.open(arquivo_path, 'r')#, encoding='latin-1')



        # !!! O script procura o arquivo CSV ordenado.
        # ( head -n1 DM_ALUNO.CSV; tail -n+2 DM_ALUNO.CSV | sort -n --field-separator='|' --key=9 ) > DM_ALUNO_SORTED.CSV
        #if arquivo_fs == 'DM_ALUNO_SORTED.CSV':
        if arquivo_fs == 'DM_ALUNO.CSV':
            arquivos['alunos'] = csv.DictReader(arquivo, delimiter='|')

        elif arquivo_fs == 'DM_IES.CSV':
            arquivos['ies'] = csv.DictReader(arquivo, delimiter='|')

    #import pdb; pdb.set_trace()
    return arquivos

def pega_cabecalho(ano):
    """ Cria um cabecalho que tenha os campos de todos os CSVS  """
    cabecalho = []

    arquivos = pega_arquivo_por_ano(ano)
    if arquivos['alunos']:
        cabecalho += arquivos['alunos'].fieldnames
    if arquivos['ies']:
        cabecalho += arquivos['ies'].fieldnames

    # Faz rearranjo para montar cabecalho com dados consolidade de IES para o facet.
    # Exclui os campos que foram agregados e inclui os campos que agregam os anteriores.
    menos_cabecalho_ies = []

    # id para o Solr. O identificador unico eh o CO_ALUNO_IES
    # 'GEOGRAFICO' e'MANT_IES' sao dados agregados para facilitar o faceting.
    mais_cabecalho_ies = ['GEOGRAFICO', 'MANT_IES', 'id', 'ano_vigencia_inep']

    cabecalho = list(set(cabecalho) - set(menos_cabecalho_ies))
    cabecalho += mais_cabecalho_ies
    return cabecalho


def trata_ies(ano):
    """
    Cria dict para ser utilizado na desnormalizacao de alunos.
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

def adiciona_instituicao(ano, lista_alunos, dict_ies,cabecalho):
    csv_lista_final = []
    for ALUNO in lista_alunos:

        # import pdb; pdb.set_trace()
        # Merge de ALUNO com ies
        try:
            chave = ALUNO['CO_IES'] +'|'+ ALUNO['CO_CURSO'] +'|'+ ALUNO['CO_ALUNO']
        except:
            raise NameError('Faltando campos na geração do Id')

        ALUNO['id'] = ano + '_' + chave
        # import pdb; pdb.set_trace()
        ALUNO['ano_vigencia_inep'] = ano

        # Converte de codigo para texto
        # ALUNO['CO_ESCOLARIDADE_ALUNO'] = ESCOLARIDADE[ALUNO['CO_ESCOLARIDADE_ALUNO']].decode('utf-8').encode('latin-1')
        # ALUNO['IN_ALUNO_DEFICIENCIA'] = DEFICIENCIA_FISICA[ALUNO['IN_ALUNO_DEFICIENCIA']].decode('utf-8').encode('latin-1')

        # if 'IN_VISITANTE_IFES_VINCULO' in ALUNO and ALUNO['IN_VISITANTE_IFES_VINCULO']:
        #     ALUNO['IN_VISITANTE_IFES_VINCULO'] = IN_VISITANTE_IFES_VINCULO[ALUNO['IN_VISITANTE_IFES_VINCULO']].decode('utf-8').encode('latin-1')
        # ALUNO = converte_SIM_NAO(ALUNO)
        # import pdb; pdb.set_trace()
        ALUNO.update( dict_ies[ ALUNO['CO_IES'] ] )
        # import pdb; pdb.set_trace()
        atualiza_csv(ano, [ALUNO], cabecalho)
        # csv_lista_final.append(ALUNO)
        # print len(csv_lista_final)
    return csv_lista_final


LIMITE = 10

if __name__ == "__main__":
    anos = os.listdir(PATH)
    anos.sort()
    for ano in anos:
        print ano

        id_inicial = 1

        # Verifica incremento de bilhos no id do ALUNO
        teste_bilhao = False

        # Cria arquivo csv
        cabecalho = pega_cabecalho(ano)
        cria_csv(ano, cabecalho) #ok

        # Pega o arquivo IES para agregar aos alunos
        dict_ies = trata_ies(ano)
        arquivo = pega_arquivo_por_ano(ano)

        # Atualmente o id CO_ALUNO_IES nao passa de 500 bilhoes.
        # Tem uma margem de aproximadamente 100 milhoes de registros para crescer,
        # caso os ids sejam incrementados sem grandes pulos.
        while (id_inicial < 15000000):
            # print id_inicial

            if arquivo['alunos']:
                # Recupera uma quantidade de registros menor ou igual ao LIMITE.
                # lista_alunos = pega_linhas(ano, arquivo['alunos'], id_inicial)

                # salvar cabecalho primeiro ? ja nao foi incluido na criação?

                csv_lista_final = adiciona_instituicao(ano, arquivo['alunos'], dict_ies[ano], cabecalho)
                # import pdb; pdb.set_trace()
                # import pdb; pdb.set_trace()
                # atualiza_csv(ano, csv_lista_final, cabecalho)

            # incrementa o id inicial de acordo com o LIMITE estabelecido.
            id_inicial += 1#LIMITE

            # O CO_ALUNO eh incrementado em 118 bilhoes nos CSV originais.
            # Para nao ficar iterando a toa no pega_linhas(), incrementa o contador.
            """
            if not teste_bilhao and id_inicial > 400000:
                #id_inicial = 118980000000
                id_inicial = 115000000000
                teste_bilhao = True
            """

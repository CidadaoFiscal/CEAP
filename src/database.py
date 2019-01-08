import csv
import sys
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

file_path =  '/home/kenelly/workspaces/cidadaofiscal/CEAP/resources/Ano-2018.csv'

csv.field_size_limit(sys.maxsize)

try:
    #connection setting
    connection = mysql.connector.connect(host='*****',
                                         database = '*****',
                                         user = '***',
                                         password = '*****')

    #check connection
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected on",db_Info)

        #insert query
        sql_insert = 'INSERT INTO raw_ceap (ceap_nome_parlamentar, ceap_cadastro , ceap_carteira_parlamentar, ' \
                     'ceap_candidatura , ceap_uf , ceap_partido , ceap_cod_legislatura , ceap_subcota , ' \
                     'ceap_descricao , ceap_espec_subcota , ceap_descricao_espec , ceap_fornecedor , ceap_cnpj_cpf,' \
                     ' ceap_numero, ceap_tipo_doc , ceap_data_emissao, ceap_vlr_doc , ceap_vlr_glosa ,' \
                     ' ceap_vlr_liquido , ceap_mes , ceap_ano , ceap_parcela , ceap_passageiro , ceap_trecho ,' \
                     ' ceap_lote , ceap_num_ressarcimento , ceap_vlr_restituicao , ceap_num_deputado , ' \
                     'ceap_id_documento) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
                     '%s,%s,%s,%s,%s,%s,%s,%s)'

        with open(file_path) as f:
            data = csv.reader(f, delimiter=';')
            #skip header
            next(data)
            for i, line in enumerate(data):
                print(i)
                for i in [16, 17, 18, 26]:
                    line[i] = line[i].replace(',','.')
                for i in range(len(line)):
                    if line[i] == '':
                        line[i] = None
                cursor = connection.cursor(prepared=True)
                result = cursor.execute(sql_insert, line)
                connection.commit()
                print(cursor.rowcount,'record inserted')

except mysql.connector as error:
    connection.rollback()
    print("Fail when inserting the record".format(error))

finally:
    #close connection
    if(connection.is_connected()):
        cursor.close()
        connection.close()
        print("Connection closed")






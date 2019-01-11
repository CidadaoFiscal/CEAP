import csv
import mysql.connector


file_path =  '~/resources/Ano-2018.csv'

try:
    #connection setting
    connection = mysql.connector.connect(host='******',
                                         database = '********',
                                         user = '******',
                                         password = '**********')

    #check connection
    if connection.is_connected():
        db_Info = connection.get_server_info()
        cursor = connection.cursor(prepared=True)
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
            data = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
            #skip header
            next(data)
            for c, line in enumerate(data):
                #Replace ',' to '.' for the values in the monetary columns
                for i in [16, 17, 18, 26]:
                    line[i] = line[i].replace(',','.')
                #Replace '' for NULL
                for i in range(len(line)):
                    if line[i] == '':
                        line[i] = None
                result = cursor.execute(sql_insert, line)
                if c % 1000 == 0:
                    connection.commit()
                    print(c)
            connection.commit()


except mysql.connector as error:
    connection.rollback()
    print("Fail when inserting the record".format(error))
except Exception as exp:
    print(exp)
finally:
    #close connection
    if(connection.is_connected()):
        cursor.close()
        connection.close()
        print("Connection closed")
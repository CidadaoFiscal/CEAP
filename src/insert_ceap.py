import os
import csv
import mysql.connector


FILE_PATH = os.getenv('FILE_PATH')
MONETARY_COLUMNS = [16, 17, 18, 26]
BATCH_SIZE = 1000

SQL_INSERT = """
INSERT INTO ceap_despesas (
    ceap_nome_parlamentar, 
    ceap_cadastro, 
    ceap_carteira_parlamentar, 
    ceap_candidatura, 
    ceap_uf, 
    ceap_partido, 
    ceap_cod_legislatura, 
    ceap_subcota,
    ceap_descricao,
    ceap_espec_subcota,
    ceap_descricao_espec, 
    ceap_fornecedor,
    ceap_cnpj_cpf,
    ceap_numero,
    ceap_tipo_doc,
    ceap_data_emissao,
    ceap_vlr_doc,
    ceap_vlr_glosa,
    ceap_vlr_liquido,
    ceap_mes,
    ceap_ano,
    ceap_parcela,
    ceap_passageiro,
    ceap_trecho,
    ceap_lote,
    ceap_num_ressarcimento,
    ceap_vlr_restituicao,
    ceap_num_deputado,
    ceap_id_documento
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);            
"""


def main():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('HOST_NAME'),
            database=os.getenv('DATABASE_NAME'),
            user=os.getenv('USER_NAME'),
            password=os.getenv('PASSWORD')
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            cursor = connection.cursor(prepared=True)
            print("Connected on", db_info)

            with open(FILE_PATH) as f:
                data = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
                next(data)  # skip header
                for c, line in enumerate(data):
                    # Replace ',' to '.' for the values in the monetary columns
                    for column in MONETARY_COLUMNS:
                        line[column] = line[column].replace(',', '.')
                    for i in range(len(line)):
                        if not line[i]:
                            line[i] = None

                    cursor.execute(SQL_INSERT, line)

                    if c % BATCH_SIZE == 0:
                        connection.commit()
                        print('Processing message {}'.format(c))

                connection.commit()


    except mysql.connector as error:
        print("Fail when inserting the record".format(error))
    except Exception as exp:
        print(exp)
    finally:
        cursor.close()
        connection.close()
        print("Connection closed")


if __name__ == '__main__':
    main()

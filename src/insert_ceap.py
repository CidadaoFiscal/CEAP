import os
import csv
import pyodbc
import codecs


FILE_PATH = os.getenv('FILE_PATH')
MONETARY_COLUMNS = [16, 17, 18, 26]
#BATCH_SIZE = 10
STRING_COLUMNS = [0, 4, 5, 8, 10, 11, 22, 23]

SQL_INSERT = """
    BULK INSERT ceap_despesas_staging 
    FROM '/home/kenelly/workspaces/cidadaofiscal/CEAP/src/tempfile'
    WITH (
    FIELDTERMINATOR=',',
    ROWTERMINATOR='\\n'
    );         
"""


def main():
    try:

        server = os.getenv('HOST_NAME')
        database = os.getenv('DATABASE_NAME')
        username = os.getenv('USER_NAME')
        password = os.getenv('PASSWORD')
        driver = '{ODBC Driver 13 for SQL Server}'
        connection = pyodbc.connect(
            'Driver=' + driver + ';Server=tcp:' + server +
            ',1433;Database=' + database + ';Uid=' + username +
            ';Pwd=' + password
        )
        # connection.setencoding(encoding='latin-1')
        cursor = connection.cursor()
        source = FILE_PATH.split('/')[-1]

        with open(FILE_PATH) as f:
            data = csv.reader(f, delimiter=';')
            next(data)  # skip header
            lines = []

            for c, line in enumerate(data):
                # Replace ',' to '.' for the values in the monetary columns
                for column in MONETARY_COLUMNS:
                    line[column] = line[column].replace(',', '.')
                for i in range(len(line)):
                    if not line[i]:
                        line[i] = ' '
                # for string in STRING_COLUMNS:
                #     #print(string)
                #     if line[string]:
                #         line[string] = line[string].encode(encoding = 'latin-1'
                #                                            ,errors = 'ignore')
                    # else:
                    #     line[string] = " "
                # from dateutil import parser
                # line[15] = parser.parse(line[15])

                #lines.append([source] + line)
                lines = [source] + line
                writecsv(lines)
                cursor.execute(SQL_INSERT)
                connection.commit()
                lines = []
                print('Processing message {}'.format(c))

                #if c - BATCH_SIZE == 0:
                    # cursor.fast_executemany = True



            connection.commit()
    except pyodbc.DataError as error:
        print("Fail when inserting the record {}".format(error))
    except Exception as exp:
        print(exp)
    finally:
        cursor.close()
        connection.close()
        print("Connection closed")


def writecsv(lines):

    filename = './tempfile'
    with codecs.open(filename, 'a') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerow(lines)

if __name__ == '__main__':
    main()

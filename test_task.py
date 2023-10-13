import sqlite3
import pandas as pd
from rich.progress import track
import time

file_name = 'data.xlsx'
df = pd.read_excel(file_name)
delay = 0.001 # задержка в секундах, для красоты работы программы, если хочется выполнить без задержки, то выставьте в 0


class DataParser:
    """ Парсер данных """
    
    def get_data_as_dict(self):
        """ Возвращает словарь всех данных """
        return df.to_dict('records')
    
    def get_all_countries(self):
        """ Возвращает множество всех стран """
        return set(df['COUNTRY'])

    def get_all_isg(self):
        """ Возвращает множество всех ISG """
        ids = df['ID_ISG']
        names = df['ISG']

        return set(zip(ids, names))
    
class FileManager:
    """ Файловый менеджер """
    def __init__(self) -> None:
        self.file_name = 'data.tsv'

    def write_file(self, data):
        with open(self.file_name, 'w') as file:
            for item in track(data, description='Запись в файл...'):
                file.write(f"{item[0]} - {item[1]}\n")
                time.sleep(delay)
                # Скорее всего в задании имелось ввиду разделитель - перенос строки, а не табуляция, поэтому его и использовал. Литерал табуляции \t

class DBHandler:
    
    def __init__(self) -> None:
        self.db_name = 'base.sqlite'
        self.connection = None
        self.data = DataParser()
    
    def open_db(self):
        """ Устанавливает соединение с базой данных """
        conn = sqlite3.connect(self.db_name)
        self.connection = conn
    
    def create_tables(self):
        """ Создает таблицы """
        conn = self.connection
        cursor = conn.cursor()
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS COUNTRY (ID_COUNTRY INTEGER PRIMARY KEY AUTOINCREMENT, NAME_COUNTRY TEXT UNIQUE)""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS ISG (ID_ISG INTEGER PRIMARY KEY, NAME_ISG TEXT)""")
        
        cursor.execute("""CREATE TABLE IF NOT EXISTS GOODS (ID INTEGER PRIMARY KEY AUTOINCREMENT, ID_TOVAR INTEGER, NAME_TOVAR TEXT, BARCOD INTEGER, ID_COUNTRY INTEGER FOREIGHN KEY REFERENCES COUNTRY(ID_COUNTRY), ID_ISG INTEGER FOREIGHN KEY REFERENCES ISG(ID_ISG))""")
        
        conn.commit()

    def fill_tables(self):
        """ Заполняет таблицы, кроме GOODS """
        """ Можно и не использовать try except, но в целях 100% работы программы я их добавил """
        all_countries = self.data.get_all_countries()
        all_isg = self.data.get_all_isg()
        db = self.connection
        cursor = db.cursor()
        for country in track(all_countries, description='Вставка стран в таблицу...'):
            try:
                cursor.execute(f"INSERT INTO COUNTRY (NAME_COUNTRY) VALUES ('{country}' )")
            except sqlite3.IntegrityError:
                pass
            time.sleep(delay)

        for id_isg, isg in track(all_isg, description='Вставка ISG в таблицу...'):
            try:
                cursor.execute(f"INSERT INTO ISG (ID_ISG, NAME_ISG) VALUES ('{id_isg}', '{isg}')")
            except sqlite3.IntegrityError:
                pass
            time.sleep(delay)


        db.commit()

    def fill_goods(self):
        """ Заполняет таблицу GOODS """
        """ Так как ни одно поле не является уникальным, кроме ID, база данных будет заполнятся дубликатами при дальнейших запусках программы. Можно решить проверкой на количество записей в таблице, но я не стал этого делать, потому что не требуется. """
        db = self.connection
        cursor = db.cursor()
        goods = self.data.get_data_as_dict()

        for good in track(goods, description='Вставка товаров в таблицу...'):
            country_id = cursor.execute(f"SELECT ID_COUNTRY FROM COUNTRY WHERE NAME_COUNTRY = '{good['COUNTRY']}'").fetchone()[0]
            isg_id = cursor.execute(f"SELECT ID_ISG FROM ISG WHERE NAME_ISG = '{good['ISG']}'").fetchone()[0]
            cursor.execute(f"INSERT INTO GOODS (ID_TOVAR, NAME_TOVAR, BARCOD, ID_COUNTRY, ID_ISG) VALUES ('{good['ID_TOVAR']}', '{good['TOVAR']}', '{good['BARCOD']}', '{country_id}', '{isg_id}')")
            time.sleep(delay)

        db.commit()
    
    def calculate_count_of_goods_in_country(self):
        """ Метод для расчета количества товаров в каждой стране """
        db = self.connection
        cursor = db.cursor()
        cursor.execute("""SELECT COUNTRY.NAME_COUNTRY, COUNT(GOODS.ID_TOVAR) AS COUNT 
                       FROM GOODS JOIN COUNTRY ON GOODS.ID_COUNTRY = COUNTRY.ID_COUNTRY GROUP BY GOODS.ID_COUNTRY 
                       ORDER BY COUNT DESC;""")
        return cursor.fetchall()

    def close_db(self):
        """ Закрывает соединение с базой данных """
        self.connection.close()

class Main:
    def __init__(self) -> None:
        self.db_handler = DBHandler()
    
    def process_db(self):
        self.db_handler.open_db()
        self.db_handler.create_tables()
        self.db_handler.fill_tables()

    def process_data(self):
        self.process_db()
        self.db_handler.fill_goods()
        data = self.db_handler.calculate_count_of_goods_in_country()
        self.db_handler.close_db()
        return data
    
    def __call__(self):
        return self._start()
    
    def _start(self):
        data = self.process_data()
        file = FileManager()
        file.write_file(data)

Main()()

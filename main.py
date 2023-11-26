import os
from tinkoff.invest import Client, CandleInterval
import datetime
import json
import random
import psycopg2


# TOKEN = ''
TOKEN = os.getenv('TINVEST_TOKEN')
# HOST="localhost"
HOST = os.getenv('HOST')
# USER="postgres"
USER = os.getenv('USER')
# PASSWORD="1234"
PASSWORD = os.getenv('PASSWORD')
# DB_NAME="postgres"
DB_NAME = os.getenv('DB_NAME')
# PORT=5432
PORT = os.getenv('PORT')

       
def run_script():
    try:
        connection = psycopg2.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB_NAME
        )
        connection.autocommit = True
        
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS lastupdates(
                        id   SERIAL PRIMARY KEY
                        ,time TIMESTAMP  NOT NULL
                        );
                """
            )
            
            cursor.execute(
                """
                    SELECT id, time FROM public.lastupdates ORDER BY id DESC LIMIT 1;
                """
            )
            last_update = cursor.fetchone()
            
            if not last_update:
                print("[INFO]: INSERT FIRST DATE")
                cursor.execute(
                    f"""
                        INSERT INTO lastupdates(id,time) VALUES (1,to_timestamp({datetime.datetime(2020, 4, 22, 12, 0, 0, 0).timestamp()}));
                    """
                )
                last_update = cursor.fetchone()
                
            last_id = last_update[0]
            from_ = last_update[1]
            to_ = datetime.datetime.now()
            interval_ = CandleInterval(13)
            
            print(
                f"""
                [INFO]: Last update date = {last_update}
                  """
                  )
            
            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS figis(
                        figi_id   SERIAL PRIMARY KEY 
                        ,name VARCHAR(40) NOT NULL
                        ,figi VARCHAR(12) NOT NULL
                    );                
                """
            )
            
            cursor.execute(
                """
                    SELECT * FROM public.figis
                    ORDER BY figi_id ASC
                """
            )
            figis = cursor.fetchall()
            
            if not figis:
                cursor.execute(
                    """
                        INSERT INTO figis(figi_id,name,figi) VALUES (1,'Берлинская ФБ','BBG000DH0MN3');
                        INSERT INTO figis(figi_id,name,figi) VALUES (2,'Дюссельдорфская ФБ','BBG000DH0L82');
                        INSERT INTO figis(figi_id,name,figi) VALUES (3,'Мюнхенская ФБ','BBG000DH0MF2');
                        INSERT INTO figis(figi_id,name,figi) VALUES (4,'Гамбургская ФБ','BBG000DH0P83');
                        INSERT INTO figis(figi_id,name,figi) VALUES (5,'Ганноверская ФБ','BBG000DH0NM2');
                        INSERT INTO figis(figi_id,name,figi) VALUES (6,'Штутгартская ФБ','BBG000DH0LW5');
                        INSERT INTO figis(figi_id,name,figi) VALUES (7,'Франкфуртская ФБ','BBG000DH0KP5');
                        INSERT INTO figis(figi_id,name,figi) VALUES (8,'Американский рынок','BBG000B9XRY4');
                        INSERT INTO figis(figi_id,name,figi) VALUES (9,'Немецкий рынок','BBG000DGZJ27');
                    """
                )
                figis = cursor.fetchall()
            print(figis)
            
            
            cursor.execute(
                """
                    CREATE TABLE IF NOT EXISTS candles(
                        id          SERIAL PRIMARY KEY
                        ,figi_id      INTEGER  NOT NULL
                        ,openunits   INTEGER  NOT NULL
                        ,opennano    INTEGER  NOT NULL
                        ,highunits   INTEGER  NOT NULL
                        ,highnano    INTEGER  NOT NULL
                        ,lowunits    INTEGER  NOT NULL
                        ,lownano     INTEGER  NOT NULL
                        ,closeunits  INTEGER  NOT NULL
                        ,closenano   INTEGER  NOT NULL
                        ,volume      INTEGER  NOT NULL
                        ,time        VARCHAR(45) NOT NULL
                        ,is_complete BOOLEAN  NOT NULL
                        ,CONSTRAINT fk_figi
                            FOREIGN KEY(figi_id) 
                                REFERENCES figis(figi_id)
                    );
                """
            )
            
            with Client(TOKEN) as client:
                for item in figis:
                    candles = client.market_data.get_candles(from_=from_, to=to_, interval=interval_, figi=item[2]).candles
                    if not candles:
                        print(f'[INFO]: Ничего не получили по {item[1]}')
                    else:
                        print(f'[PROCESS]: Обработка {item[1]} - {item[2]}')
                        for candle in candles:
                            print(f'[INFO]: Добавляем в базу {candle}')
                            cursor.execute(
                                """
                                    SELECT id FROM public.candles ORDER BY id DESC LIMIT 1;
                                """
                            )
                            last_candle_id = cursor.fetchone()[0]
                            if not last_candle_id:
                                last_candle_id = 0
                            print(f"[INFO]: Last candle id = {last_candle_id}")
                            cursor.execute(
                                f"""
                                    INSERT INTO public.candles(
                                        id, figi_id, openunits, opennano, highunits, highnano, lowunits, lownano, closeunits, closenano, volume, "time", is_complete)
                                        VALUES ({last_candle_id+1}
                                        ,{item[0]}
                                        ,{candle.open.units}
                                        ,{candle.open.nano}
                                        ,{candle.high.units}
                                        ,{candle.high.nano}
                                        ,{candle.low.units}
                                        ,{candle.low.nano}
                                        ,{candle.close.units}
                                        ,{candle.close.nano}
                                        ,{candle.volume}
                                        ,to_timestamp({candle.time.replace(tzinfo=None).timestamp()})
                                        ,{candle.is_complete});
                                """
                            )
                        print(cursor.fetchall())
                        cursor.execute(
                            f"""
                                INSERT INTO lastupdates(id,time) VALUES ({last_id+1},to_timestamp({to_.timestamp()}));
                            """
                        )
                        print(cursor.fetchone())
    
    except Exception as _ex:
        print(f'[ERROR]: {_ex}')
        
    finally:
        if connection:
            connection.close()
            print('[INFO]: Script ended')
    
        
def main():
    print('[INFO]: Script start')
    run_script()
    
    
if __name__ == '__main__':
    main()
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Text,
    Integer,
    select
)

from config import MYSQL_URL

engine = create_engine(MYSQL_URL)
meta = MetaData()


class HouseManager():

    def __init__(self, engine) -> None:
        self.engine = engine
        self.house = self.get_table_schema()

    def get_table_schema(self):
        house = Table(
            "houses5", meta,
            Column("id", Integer, primary_key=True),
            Column("title", String(200)),
            Column("som", Integer),
            Column("dollar", Integer),
            Column("mobile", String(50)),
            Column("description", Text),
            Column('link', String(255), nullable=False, unique=True)
            # extend_existing=True
        )
        return house

    def create_table(self):
        meta.create_all(self.engine, checkfirst=True)
        print("Таблица успешно создана")

    def insert_house(self, data):
        ins = self.house.insert().values(
            **data
        )
        connect = self.engine.connect()
        result = connect.execute(ins)
        connect.commit()

    def check_house_in_db(self, url):
        query = select(self.house).where(self.house.c.link == url)
        connect = self.engine.connect()
        result = connect.execute(query)
        result = result.fetchone()
        return result is not None

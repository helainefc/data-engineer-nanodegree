from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#Scheme: "postgres+psycopg2://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>"
db_uri = 'postgres+psycopg2://postgres:postgres@172.17.0.2:5432/play_list'

engine = create_engine(db_uri)
Session = sessionmaker(bind=engine)

Base = declarative_base()

session = Session()
import csv
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Création de la base de données
engine = create_engine('sqlite:///leaderboard_SQL_DB.db', echo=True)
Base = declarative_base()

class Leaderboard(Base):
    __tablename__ = 'leaderboard'
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    message_id = Column(String, unique=True)
    received_at = Column(DateTime)
    content = Column(String)

# Création des tables dans la base de données
Base.metadata.create_all(engine)

# Ouverture d'une session pour interagir avec la base de données
Session = sessionmaker(bind=engine)
session = Session()

# Lecture du fichier CSV et insertion des données dans la base de données
with open('pipeline_result.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        received_at = datetime.strptime(row['received_at'], '%Y-%m-%d %H:%M:%S.%f')
        message = Leaderboard(user_id=row['user_id'], first_name=row['first_name'], last_name=row['last_name'],
                          message_id=row['message_id'], received_at=received_at, content=row['content'])
        if not session.query(Leaderboard).filter_by(message_id=row['message_id']).first():
            session.add(message)
            session.commit()
            print(f"Le message avec l'ID {row['message_id']} a été ajouté à la base de données.")
        else:
            print(f"Le message avec l'ID {row['message_id']} est déjà présent dans la base de données et ne sera pas ajouté.")
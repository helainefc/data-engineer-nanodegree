from sqlalchemy import Column, String, Float
from bd import Base

class Artist(Base):
    __tablename__ = 'artist'
    artist_id   = Column(String, primary_key=True)
    name        = Column(String)
    location    = Column(String)
    latitude    = Column(Float)
    longitude   = Column(Float)
    
    def _init_(self,artist_id,name,location,latitude,longitude):
        self.artist_id = artist_id
        self.name = name
        self.location = location
        self.latitude = latitude
        self.longitude = longitude
        
    def __repr__(self):
        return "<Artist(artist_id='{}', name='{}', location='{}', latitude={}, longitude={})>"\
                .format(self.artist_id, self.name, self.location, self.latitude,self.longitude)
    
    def __eq__(self, otro):
        return self.artist_id == otro.artist_id
    
    def __hash__(self):
        return hash((self.artist_id))
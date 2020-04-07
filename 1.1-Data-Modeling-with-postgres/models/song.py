from sqlalchemy import Column, String, Integer, Float
from bd import Base

class Song(Base):
    __tablename__ = 'song'
    song_id   = Column(String, primary_key=True)
    title     = Column(String)
    artist_id = Column(String)
    year      = Column(Integer)
    duration  = Column(Float)
    
    def _init_(self,song_id,title,artist_id, year,duration):
        self.song_id = song_id
        self.title = title
        self.last_name = artist_id
        self.year = year
        self.duration = duration
        
    def __repr__(self):
        return "<Song(song_id='{}', title='{}', artist_id='{}', year={}, duration={})>"\
                .format(self.song_id, self.title, self.artist_id, self.year,self.duration)
    
    def __eq__(self, otro):
        return self.song_id == otro.song_id
    
    def __hash__(self):
        return hash((self.song_id))
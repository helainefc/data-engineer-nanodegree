from sqlalchemy import Column, BigInteger, Integer,String
from bd import Base

class SongPlay(Base):
    __tablename__ = 'song_play'
    
    songplay_id   =  Column(Integer,primary_key=True)
    user_id       =  Column(Integer)
    song_id       =  Column(String) 
    artist_id     =  Column(String) 
    session_id    =  Column(Integer)
    start_time    =  Column(BigInteger)
    level         =  Column(String)
    location      =  Column(String)
    user_agent    =  Column(String)
        
    def _init_(self,songplay_id,user_id,song_id,artist_id,session_id,start_time,level,location,user_agent):
        self.songplay_id  = songplay_id
        self.user_id      = user_id
        self.song_id      = song_id
        self.artist_id    = artist_id
        self.session_id   = session_id
        self.start_time   = start_time
        self.level        = level
        self.location     = location
        self.user_agent   = user_agent
        
    def __repr__(self):
        return "<SongPlay(songplay_id={}, user_id={}, song_id='{}', artist_id='{}', session_id={},start_time={},level={},location={},user_agent={})>"\
                .format(self.songplay_id, self.user_id, self.song_id, self.artist_id,self.session_id,self.start_time,self.level,self.location,self.user_agent)
        
    def __eq__(self, otro):
        return self.songplay_id == otro.songplay_id
    
    def __hash__(self):
        return hash((self.songplay_id))
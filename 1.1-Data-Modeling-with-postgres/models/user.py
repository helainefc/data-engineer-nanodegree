from sqlalchemy import Column, String, Integer
from bd import Base

class User(Base):
    __tablename__ = 'user'
    user_id   = Column(Integer, primary_key=True)
    firstName = Column(String)
    lastName  = Column(String)
    gender    = Column(String)
    level     = Column(String)
    
    def _init_(self,user_id,firstName,lastName, gender,level):
        self.user_id = user_id
        self.firstName = firstName
        self.last_name = lastName
        self.gender = gender
        self.level = level
        
    def __repr__(self):
        return "<User(user_id={}, firstName={}, lastName={}, gender={}, level={})>"\
                .format(self.user_id, self.firstName, self.lastName, self.gender,self.level)
        
    def __eq__(self, otro):
        return self.user_id == otro.user_id
    
    def __hash__(self):
        return hash((self.user_id))
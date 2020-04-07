from sqlalchemy import Column, BigInteger, Integer
from bd import Base

class TimeFormat(Base):
    __tablename__ = 'time_format'
    
    start_time = Column(BigInteger, primary_key=True)
    hour       = Column(Integer)
    day        = Column(Integer)
    week       = Column(Integer)
    month      = Column(Integer) 
    year       = Column(Integer) 
    weekday    = Column(Integer)
        
    def _init_(self,start_time,hour,day,week,month,year,weelday):
        self.start_time = start_time
        self.hour       = hour
        self.day        = day
        self.week       = week
        self.month      = month
        self.year       = year
        self.weelday    = weelday
        
    def __repr__(self):
        return "<TimeFormat(start_time='{}', hour='{}', day='{}', week={}, month={},year={}, weekday={})>"\
                .format(self.start_time, self.hour, self.day, self.week,self.month,self.year,self.weekday)
    
    def __eq__(self, otro):
        return self.start_time == otro.start_time
    
    def __hash__(self):
        return hash((self.start_time))
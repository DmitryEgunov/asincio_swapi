from sqlalchemy import String, Integer, Column
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker

PG_DSN = 'postgresql+asyncpg://user:12345@127.0.0.1:5431/swapi'
engine = create_async_engine(PG_DSN)

Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class SwapiPeople(Base):

    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


async def paste_to_db(people_list):
    async with Session() as session:
        for item in people_list:
            if item.get('detail') is None:
                swapi_people = SwapiPeople(birth_year=item['birth_year'],
                                eye_color=item['eye_color'],
                                films=', '.join(item['films']),
                                gender=item['gender'],
                                hair_color=item['hair_color'],
                                height=item['height'],
                                homeworld=item['homeworld'],
                                mass=item['mass'],
                                name=item['name'],
                                skin_color=item['skin_color'],
                                species=', '.join(item['species']),
                                starships=', '.join(item['starships']),
                                vehicles=', '.join(item['vehicles']))
                session.add(swapi_people)
        await session.commit()
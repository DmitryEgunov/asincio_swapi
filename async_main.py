import asyncio

import aiohttp

from db import paste_to_db, engine, Base


async def get_links(links_list, client_session):
    coros = [client_session.get(link) for link in links_list]
    http_responces = await asyncio.gather(*coros)
    json_coros = [http_response.json() for http_response in http_responces]
    return await asyncio.gather(*json_coros)


async def get_people(people_id, client_session):
    async with client_session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        films_coro = get_links(json_data.get('films', []), client_session)
        vehicles_coro = get_links(json_data.get('vehicles', []), client_session)
        species_coro = get_links(json_data.get('species', []), client_session)
        starships_coro = get_links(json_data.get('starships', []), client_session)
        fields = await asyncio.gather(films_coro, vehicles_coro, species_coro, starships_coro)
        films, vehicles, species, starships = fields
        json_data['films'] = films = [films[i]['title'] for i in range(len(films))]
        json_data['vehicles'] = vehicles = [vehicles[i]['name'] for i in range(len(vehicles))]
        json_data['species'] = species = [species[i]['name'] for i in range(len(species))]
        json_data['starships'] = starships = [starships[i]['name'] for i in range(len(starships))]
        return json_data


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(1, 10)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(11, 20)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(21, 30)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(31, 40)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(41, 50)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(51, 60)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(61, 70)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(71, 80)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(81, 90)]
        results = await asyncio.gather(*coros)

    asyncio.create_task(paste_to_db(people_list=results))

    all_tasks = asyncio.all_tasks()
    all_tasks = all_tasks - {asyncio.current_task()}
    await asyncio.gather(*all_tasks)

asyncio.run(main())


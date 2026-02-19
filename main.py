import asyncio

from loguru import logger

from graphql_requests import graphql_post_request
from query_builder import QueryBuilder
from data_manager import DataManager
from db import DatabaseManager

async def hacktivity_search(size: int, offset: int):
    query = await QueryBuilder.build_hacktivity_search_query(size=size, offset=offset)
    response = await graphql_post_request(query)

    nodes = response['data']['search']['nodes']
    processed_data = await DataManager.process_nodes(nodes)

    async with DatabaseManager('data.db') as db:
        for obj in processed_data:
            await db.add_primary_record(obj)

async def profile_info_search():
    async with DatabaseManager('data.db') as db:
        primary_data = await db.get_all_primary_records()
        for hunter in primary_data:
            username = hunter.username
        
            query = await QueryBuilder.build_profile_info_query(username)
            response = await graphql_post_request(query)
            user_data = response['data']['user']

            processed_data = await DataManager.process_profile(user_data)
            await db.add_finally_record(processed_data)

async def primary_search():
    await hacktivity_search(100, 0)
    logger.success('Первичные данные получены.')

async def finally_search():
    await profile_info_search()
    logger.success('Итоговые данные получены.')

async def main():
    await primary_search()
    await finally_search()

if __name__ == "__main__":
    asyncio.run(main())
import httpx
from loguru import logger

client = httpx.AsyncClient()

async def graphql_post_request(query_data: dict) -> (dict | None):
    try:
        logger.info('Запрос к graphql.')
        response = await client.post(
                "https://hackerone.com/graphql",
                json=query_data,
                timeout=30
                # TODO здесь прокси можно добавить
            )
        if response.status_code == 200:
            logger.success('Успешно. 200')
            return response.json()
        else:
            logger.error(response.json)
            return None
    except Exception as e:
        logger.error(e)
        return None

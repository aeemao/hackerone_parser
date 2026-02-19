from typing import Any, Dict

from loguru import logger

from db import PrimaryData, FinallyData

class DataManager():
    async def process_nodes(nodes: list[dict]) -> list[PrimaryData]:
        '''Обрабатывает выводы из хактивити.'''
        data = []
        for node in nodes:
            if node['reporter']:
                username = node['reporter']['username']
                source = node['__typename']
                jsondata = node
                obj = PrimaryData(None, username, source, jsondata)
                data.append(obj)
        return data
    
    async def process_edges(edges: list[dict]) -> list[PrimaryData]:
        '''Обрабатывает выводы из лидерборда.'''
        data = []
        for edge in edges:
            obj = PrimaryData(None, edge['node']['user']['username'], edge['__typename'], edge)
            data.append(obj)
        return data
    
    async def process_profile(user_data: Dict[str, Any]) -> 'FinallyData':
        '''Обрабатывает общую информацию профиля.'''
        if user_data is None:
            logger.warning('Данные профиля в ответе не найдены.')
            return None
        
        obj = FinallyData.from_user_data(user_data)
        return obj

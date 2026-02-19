import asyncio
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict, field

import aiosqlite
from loguru import logger

@dataclass
class PrimaryData:
    """Датакласс для primary_table"""
    id: Optional[int] = None
    username: str = ""
    source: str = ""
    json_info: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PrimaryData':
        """Создание объекта из словаря"""
        return cls(
            id=data.get('id'),
            username=data.get('username', ''),
            source=data.get('source', ''),
            json_info=data.get('json_info', {}),
            created_at=data.get('created_at')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь"""
        return asdict(self)

@dataclass
class FinallyData:
    """Датакласс для finally_table"""
    id: Optional[int] = None
    username: str = ""
    name: Optional[str] = None
    intro: Optional[str] = None
    profileActivated: bool = False
    profile_created_at: Optional[str] = None
    location: Optional[str] = None
    website: Optional[str] = None
    bio: Optional[str] = None
    bugcrowd_handle: Optional[str] = None
    hack_the_box_handle: Optional[str] = None
    github_handle: Optional[str] = None
    gitlab_handle: Optional[str] = None
    linkedin_handle: Optional[str] = None
    twitter_handle: Optional[str] = None
    cleared: bool = False
    verified: bool = False
    open_for_employment: Optional[bool] = None
    mark_as_company_on_leaderboards: bool = False
    resolved_report_count: int = 0
    thanks_items_total_count: int = 0
    badges_json: List[Dict[str, Any]] = field(default_factory=list)
    public_reviews_json: List[Dict[str, Any]] = field(default_factory=list)
    created_at: Optional[str] = None
    
    @classmethod
    def from_user_data(cls, user_data: Dict[str, Any]) -> 'FinallyData':
        """Создание объекта из сырых данных пользователя"""

        badges = user_data.get('badges', {})
        if isinstance(badges, dict):
            badges_json = badges.get('edges', [])
        else:
            badges_json = []

        public_reviews = user_data.get('public_reviews', {})
        if isinstance(public_reviews, dict):
            public_reviews_json = public_reviews.get('edges', [])
        else:
            public_reviews_json = []
        
        return cls(
            username=user_data.get('username', ''),
            name=user_data.get('name'),
            intro=user_data.get('intro'),
            profileActivated=user_data.get('profileActivated', False),
            profile_created_at=user_data.get('created_at'),
            location=user_data.get('location'),
            website=user_data.get('website'),
            bio=user_data.get('bio'),
            bugcrowd_handle=user_data.get('bugcrowd_handle'),
            hack_the_box_handle=user_data.get('hack_the_box_handle'),
            github_handle=user_data.get('github_handle'),
            gitlab_handle=user_data.get('gitlab_handle'),
            linkedin_handle=user_data.get('linkedin_handle'),
            twitter_handle=user_data.get('twitter_handle'),
            cleared=user_data.get('cleared', False),
            verified=user_data.get('verified', False),
            open_for_employment=user_data.get('open_for_employment'),
            mark_as_company_on_leaderboards=user_data.get('mark_as_company_on_leaderboards', False),
            resolved_report_count=user_data.get('resolved_report_count', 0),
            thanks_items_total_count=user_data.get('thanks_items_total_count', 0),
            badges_json=badges_json,
            public_reviews_json=public_reviews_json
        )
    
    def to_db_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь для вставки в БД"""
        badges_json_str = json.dumps(self.badges_json, ensure_ascii=False)
        reviews_json_str = json.dumps(self.public_reviews_json, ensure_ascii=False)
        
        return {
            'username': self.username,
            'name': self.name,
            'intro': self.intro,
            'profileActivated': self.profileActivated,
            'profile_created_at': self.profile_created_at,
            'location': self.location,
            'website': self.website,
            'bio': self.bio,
            'bugcrowd_handle': self.bugcrowd_handle,
            'hack_the_box_handle': self.hack_the_box_handle,
            'github_handle': self.github_handle,
            'gitlab_handle': self.gitlab_handle,
            'linkedin_handle': self.linkedin_handle,
            'twitter_handle': self.twitter_handle,
            'cleared': self.cleared,
            'verified': self.verified,
            'open_for_employment': self.open_for_employment,
            'mark_as_company_on_leaderboards': self.mark_as_company_on_leaderboards,
            'resolved_report_count': self.resolved_report_count,
            'thanks_items_total_count': self.thanks_items_total_count,
            'badges_json': badges_json_str,  # JSON строка
            'public_reviews_json': reviews_json_str  # JSON строка
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в обычный словарь"""
        return asdict(self)

class DatabaseManager:
    def __init__(self, db_path: str = 'database.db'):
        self.db_path = db_path
    
    async def __aenter__(self):
        self.connection = await aiosqlite.connect(self.db_path)
        await self.connection.execute('PRAGMA foreign_keys = ON')
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.connection.close()
    
    async def initialize(self):
        """Инициализация базы данных и создание таблиц"""
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS primary_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                source TEXT NOT NULL,
                json_info TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS finally_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                name TEXT,
                intro TEXT,
                profileActivated BOOLEAN,
                profile_created_at TIMESTAMP,
                location TEXT,
                website TEXT,
                bio TEXT,
                bugcrowd_handle TEXT,
                hack_the_box_handle TEXT,
                github_handle TEXT,
                gitlab_handle TEXT,
                linkedin_handle TEXT,
                twitter_handle TEXT,
                cleared BOOLEAN,
                verified BOOLEAN,
                open_for_employment BOOLEAN,
                mark_as_company_on_leaderboards BOOLEAN,
                resolved_report_count INTEGER,
                thanks_items_total_count INTEGER,
                badges_json TEXT NOT NULL,
                public_reviews_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await self.connection.commit()
        logger.info('Таблицы созданы или уже существуют.')
    
    async def add_primary_record(self, data: PrimaryData) -> int:
        """Добавление записи в primary_table"""
        try:
            print(data.source)
            async with self.connection.cursor() as cursor:
                await cursor.execute(
                    '''INSERT INTO primary_table (username, source, json_info) 
                       VALUES (?, ?, ?)''',
                    (data.username, data.source, json.dumps(data.json_info, ensure_ascii=False))
                )
                await self.connection.commit()
                record_id = cursor.lastrowid
                logger.info(f'Добавлена запись в primary_table с ID: {record_id}')
                return record_id
        except aiosqlite.IntegrityError as e:
            logger.error(f'Ошибка целостности данных: {e}')
            raise
        except Exception as e:
            logger.error(f'Ошибка при добавлении записи: {e}')
            raise
    
    async def get_primary_by_id(self, record_id: int) -> Optional[PrimaryData]:
        """Получение записи из primary_table по ID"""
        try:
            async with self.connection.execute(
                'SELECT * FROM primary_table WHERE id = ?',
                (record_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return PrimaryData(
                        id=row[0],
                        username=row[1],
                        source=row[2],
                        json_info=json.loads(row[3]),
                        created_at=row[4]
                    )
                return None
        except Exception as e:
            logger.error(f'Ошибка при получении записи: {e}')
            return None
    
    async def get_primary_by_username(self, username: str) -> Optional[PrimaryData]:
        """Получение записи из primary_table по имени пользователя"""
        try:
            async with self.connection.execute(
                'SELECT * FROM primary_table WHERE username = ?',
                (username,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return PrimaryData(
                        id=row[0],
                        username=row[1],
                        source=row[2],
                        json_info=json.loads(row[3]),
                        created_at=row[4]
                    )
                return None
        except Exception as e:
            logger.error(f'Ошибка при получении записи: {e}')
            return None
    
    async def get_all_primary_records(self, limit: int = 100) -> List[PrimaryData]:
        """Получение всех записей из primary_table"""
        try:
            async with self.connection.execute(
                f'SELECT * FROM primary_table ORDER BY created_at DESC LIMIT {limit}'
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    PrimaryData(
                        id=row[0],
                        username=row[1],
                        source=row[2],
                        json_info=json.loads(row[3]),
                        created_at=row[4]
                    ) for row in rows
                ]
        except Exception as e:
            logger.error(f'Ошибка при получении записей: {e}')
            return []
    
    async def update_primary_record(self, record_id: int, data: PrimaryData) -> bool:
        """Обновление записи в primary_table"""
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(
                    'UPDATE primary_table SET username = ?, source = ?, json_info = ? WHERE id = ?',
                    (data.username, data.source, json.dumps(data.json_info, ensure_ascii=False), record_id)
                )
                await self.connection.commit()
                updated = cursor.rowcount > 0
                if updated:
                    logger.info(f'Обновлена запись в primary_table с ID: {record_id}')
                return updated
        except Exception as e:
            logger.error(f'Ошибка при обновлении записи: {e}')
            return False
    
    async def delete_primary_record(self, record_id: int) -> bool:
        """Удаление записи из primary_table"""
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute('DELETE FROM primary_table WHERE id = ?', (record_id,))
                await self.connection.commit()
                deleted = cursor.rowcount > 0
                if deleted:
                    logger.info(f'Удалена запись из primary_table с ID: {record_id}')
                return deleted
        except Exception as e:
            logger.error(f'Ошибка при удалении записи: {e}')
            return False
    
    async def add_finally_record(self, data: FinallyData) -> int:
        """Добавление записи в finally_table"""
        try:
            if data is None:
                logger.warning('Данные профиля в ответе не найдены.')
                return None

            db_data = data.to_db_dict()
            async with self.connection.cursor() as cursor:
                await cursor.execute('''
                    INSERT INTO finally_table (
                        username, name, intro, profileActivated, profile_created_at,
                        location, website, bio, bugcrowd_handle, hack_the_box_handle,
                        github_handle, gitlab_handle, linkedin_handle, twitter_handle,
                        cleared, verified, open_for_employment, mark_as_company_on_leaderboards,
                        resolved_report_count, thanks_items_total_count,
                        badges_json, public_reviews_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    db_data['username'],
                    db_data['name'],
                    db_data['intro'],
                    db_data['profileActivated'],
                    self._parse_timestamp(db_data['profile_created_at']),
                    db_data['location'],
                    db_data['website'],
                    db_data['bio'],
                    db_data['bugcrowd_handle'],
                    db_data['hack_the_box_handle'],
                    db_data['github_handle'],
                    db_data['gitlab_handle'],
                    db_data['linkedin_handle'],
                    db_data['twitter_handle'],
                    db_data['cleared'],
                    db_data['verified'],
                    db_data['open_for_employment'],
                    db_data['mark_as_company_on_leaderboards'],
                    db_data['resolved_report_count'],
                    db_data['thanks_items_total_count'],
                    db_data['badges_json'],
                    db_data['public_reviews_json']
                ))
                await self.connection.commit()
                record_id = cursor.lastrowid
                logger.info(f'Добавлена запись в finally_table с ID: {record_id}')
                return record_id
        except aiosqlite.IntegrityError as e:
            logger.error(f'Ошибка целостности данных: {e}')
            raise
        except Exception as e:
            logger.error(f'Ошибка при добавлении записи: {e}')
            raise
    
    async def get_finally_by_id(self, record_id: int) -> Optional[FinallyData]:
        """Получение записи из finally_table по ID"""
        try:
            async with self.connection.execute(
                'SELECT * FROM finally_table WHERE id = ?',
                (record_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return FinallyData.from_db_row(row)
                return None
        except Exception as e:
            logger.error(f'Ошибка при получении записи: {e}')
            return None
    
    async def get_finally_by_username(self, username: str) -> Optional[FinallyData]:
        """Получение записи из finally_table по имени пользователя"""
        try:
            async with self.connection.execute(
                'SELECT * FROM finally_table WHERE username = ?',
                (username,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return FinallyData.from_db_row(row)
                return None
        except Exception as e:
            logger.error(f'Ошибка при получении записи: {e}')
            return None
    
    async def get_all_finally_records(self, limit: int = 100) -> List[FinallyData]:
        """Получение всех записей из finally_table"""
        try:
            async with self.connection.execute(
                f'SELECT * FROM finally_table ORDER BY created_at DESC LIMIT {limit}'
            ) as cursor:
                rows = await cursor.fetchall()
                return [FinallyData.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f'Ошибка при получении записей: {e}')
            return []
    
    async def search_finally_records(self, search_query: str, field: str = 'username') -> List[FinallyData]:
        """Поиск записей в finally_table"""
        try:
            valid_fields = ['username', 'name', 'location', 'github_handle']
            if field not in valid_fields:
                field = 'username'
            
            async with self.connection.execute(
                f'SELECT * FROM finally_table WHERE {field} LIKE ?',
                (f'%{search_query}%',)
            ) as cursor:
                rows = await cursor.fetchall()
                return [FinallyData.from_db_row(row) for row in rows]
        except Exception as e:
            logger.error(f'Ошибка при поиске записей: {e}')
            return []
    
    async def update_finally_record(self, username: str, data: FinallyData) -> bool:
        """Обновление записи в finally_table"""
        try:
            db_data = data.to_db_dict()
            
            async with self.connection.cursor() as cursor:
                await cursor.execute('''
                    UPDATE finally_table SET
                        name = ?,
                        intro = ?,
                        profileActivated = ?,
                        profile_created_at = ?,
                        location = ?,
                        website = ?,
                        bio = ?,
                        bugcrowd_handle = ?,
                        hack_the_box_handle = ?,
                        github_handle = ?,
                        gitlab_handle = ?,
                        linkedin_handle = ?,
                        twitter_handle = ?,
                        cleared = ?,
                        verified = ?,
                        open_for_employment = ?,
                        mark_as_company_on_leaderboards = ?,
                        resolved_report_count = ?,
                        thanks_items_total_count = ?,
                        badges_json = ?,
                        public_reviews_json = ?
                    WHERE username = ?
                ''', (
                    db_data['name'],
                    db_data['intro'],
                    db_data['profileActivated'],
                    self._parse_timestamp(db_data['profile_created_at']),
                    db_data['location'],
                    db_data['website'],
                    db_data['bio'],
                    db_data['bugcrowd_handle'],
                    db_data['hack_the_box_handle'],
                    db_data['github_handle'],
                    db_data['gitlab_handle'],
                    db_data['linkedin_handle'],
                    db_data['twitter_handle'],
                    db_data['cleared'],
                    db_data['verified'],
                    db_data['open_for_employment'],
                    db_data['mark_as_company_on_leaderboards'],
                    db_data['resolved_report_count'],
                    db_data['thanks_items_total_count'],
                    db_data['badges_json'],
                    db_data['public_reviews_json'],
                    username
                ))
                await self.connection.commit()
                updated = cursor.rowcount > 0
                if updated:
                    logger.info(f'Обновлена запись в finally_table для пользователя: {username}')
                return updated
        except Exception as e:
            logger.error(f'Ошибка при обновлении записи: {e}')
            return False
    
    async def delete_finally_record(self, username: str) -> bool:
        """Удаление записи из finally_table"""
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute('DELETE FROM finally_table WHERE username = ?', (username,))
                await self.connection.commit()
                deleted = cursor.rowcount > 0
                if deleted:
                    logger.info(f'Удалена запись из finally_table для пользователя: {username}')
                return deleted
        except Exception as e:
            logger.error(f'Ошибка при удалении записи: {e}')
            return False
    
    async def delete_finally_by_id(self, record_id: int) -> bool:
        """Удаление записи из finally_table по ID"""
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute('DELETE FROM finally_table WHERE id = ?', (record_id,))
                await self.connection.commit()
                deleted = cursor.rowcount > 0
                if deleted:
                    logger.info(f'Удалена запись из finally_table с ID: {record_id}')
                return deleted
        except Exception as e:
            logger.error(f'Ошибка при удалении записи: {e}')
            return False
    
    async def add_or_update_finally_record(self, user_data: Dict[str, Any]) -> int:
        """Добавить или обновить запись в finally_table из сырых данных"""
        try:
            # Создаем объект FinallyData из сырых данных
            data = FinallyData.from_user_data(user_data)
            
            # Проверяем существование пользователя
            existing = await self.get_finally_by_username(data.username)
            
            if existing:
                # Обновляем существующую запись
                data.id = existing.id  # Сохраняем существующий ID
                data.created_at = existing.created_at  # Сохраняем время создания
                updated = await self.update_finally_record(data.username, data)
                return existing.id if updated else 0
            else:
                # Добавляем новую запись
                return await self.add_finally_record(data)
        except Exception as e:
            logger.error(f'Ошибка при добавлении/обновлении записи: {e}')
            raise
    
    async def clear_all_data(self):
        """Очистка всех данных из таблиц"""
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute('DELETE FROM finally_table')
                await cursor.execute('DELETE FROM primary_table')
                await self.connection.commit()
                logger.info('Все данные очищены')
        except Exception as e:
            logger.error(f'Ошибка при очистке данных: {e}')
            raise
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики по базе данных"""
        try:
            stats = {}
            
            async with self.connection.execute('SELECT COUNT(*) FROM primary_table') as cursor:
                stats['primary_count'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT COUNT(*) FROM finally_table') as cursor:
                stats['finally_count'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT COUNT(DISTINCT source) FROM primary_table') as cursor:
                stats['unique_sources'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT MAX(created_at) FROM primary_table') as cursor:
                stats['last_primary'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT MAX(created_at) FROM finally_table') as cursor:
                stats['last_finally'] = (await cursor.fetchone())[0]
            
            return stats
        except Exception as e:
            logger.error(f'Ошибка при получении статистики: {e}')
            return {}
    
    async def get_finally_statistics(self) -> Dict[str, Any]:
        """Получение статистики по finally_table"""
        try:
            stats = {}
            
            async with self.connection.execute('SELECT COUNT(*) FROM finally_table') as cursor:
                stats['total_users'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT COUNT(*) FROM finally_table WHERE verified = 1') as cursor:
                stats['verified_users'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT COUNT(*) FROM finally_table WHERE resolved_report_count > 0') as cursor:
                stats['users_with_reports'] = (await cursor.fetchone())[0]
            
            async with self.connection.execute('SELECT AVG(resolved_report_count) FROM finally_table') as cursor:
                result = await cursor.fetchone()
                stats['avg_reports'] = round(result[0] or 0, 2)
            
            async with self.connection.execute('''
                SELECT username, resolved_report_count 
                FROM finally_table 
                WHERE resolved_report_count > 0 
                ORDER BY resolved_report_count DESC 
                LIMIT 5
            ''') as cursor:
                rows = await cursor.fetchall()
                stats['top_reporters'] = [{'username': row[0], 'count': row[1]} for row in rows]
            
            return stats
        except Exception as e:
            logger.error(f'Ошибка при получении статистики: {e}')
            return {}
    
    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[str]:
        """Парсинг timestamp строки в формат SQLite"""
        if not timestamp_str:
            return None
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return None

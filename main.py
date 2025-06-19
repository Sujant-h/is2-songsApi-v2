from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import aiosqlite
import json
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'songs.db')
DATA_FILE = os.path.join(os.path.dirname(__file__), 'data', 'data.json')

app = FastAPI(title="Songs API")

class Translation(BaseModel):
    language: str
    title: str
    text: str

class Song(BaseModel):
    song_number: str
    title_ta: str
    text_ta: str
    translations: List[Translation]

class SongUpdate(BaseModel):
    title_ta: Optional[str] = None
    text_ta: Optional[str] = None
    translations: Optional[List[Translation]] = None

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS songs (
                song_number TEXT PRIMARY KEY,
                title_ta TEXT NOT NULL,
                text_ta TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS translations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                song_number TEXT NOT NULL,
                language TEXT NOT NULL,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                UNIQUE(song_number, language),
                FOREIGN KEY(song_number) REFERENCES songs(song_number) ON DELETE CASCADE
            )
            """
        )
        await db.commit()

async def load_initial_data():
    if not os.path.exists(DATA_FILE):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for item in data:
            await db.execute(
                "INSERT OR IGNORE INTO songs(song_number, title_ta, text_ta) VALUES (?, ?, ?)",
                (item['song_number'], item['title_ta'], item['text_ta'])
            )
            for t in item.get('translations', []):
                await db.execute(
                    "INSERT OR IGNORE INTO translations(song_number, language, title, text) VALUES (?, ?, ?, ?)",
                    (item['song_number'], t['language'], t['title'], t['text'])
                )
        await db.commit()

@app.on_event("startup")
async def startup_event():
    await init_db()
    await load_initial_data()

@app.get("/songs", response_model=List[Song])
async def get_songs(language: Optional[str] = None):
    songs = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT song_number, title_ta, text_ta FROM songs") as cursor:
            async for song_number, title_ta, text_ta in cursor:
                translations = []
                if language:
                    async with db.execute(
                        "SELECT language, title, text FROM translations WHERE song_number=? AND language=?",
                        (song_number, language),
                    ) as cur_t:
                        async for lang, title, text in cur_t:
                            translations.append(Translation(language=lang, title=title, text=text))
                else:
                    async with db.execute(
                        "SELECT language, title, text FROM translations WHERE song_number=?",
                        (song_number,),
                    ) as cur_t:
                        async for lang, title, text in cur_t:
                            translations.append(Translation(language=lang, title=title, text=text))
                songs.append(Song(song_number=song_number, title_ta=title_ta, text_ta=text_ta, translations=translations))
    return songs

@app.get("/songs/{song_number}", response_model=Song)
async def get_song(song_number: str, language: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT song_number, title_ta, text_ta FROM songs WHERE song_number=?",
            (song_number,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Song not found")
            sn, title_ta, text_ta = row
            translations = []
            if language:
                async with db.execute(
                    "SELECT language, title, text FROM translations WHERE song_number=? AND language=?",
                    (sn, language),
                ) as cur_t:
                    async for lang, title, text in cur_t:
                        translations.append(Translation(language=lang, title=title, text=text))
            else:
                async with db.execute(
                    "SELECT language, title, text FROM translations WHERE song_number=?",
                    (sn,),
                ) as cur_t:
                    async for lang, title, text in cur_t:
                        translations.append(Translation(language=lang, title=title, text=text))
            return Song(song_number=sn, title_ta=title_ta, text_ta=text_ta, translations=translations)

@app.post("/songs", response_model=Song, status_code=201)
async def create_song(song: Song):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO songs(song_number, title_ta, text_ta) VALUES (?, ?, ?)",
                (song.song_number, song.title_ta, song.text_ta),
            )
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="Song already exists")
        for t in song.translations:
            await db.execute(
                "INSERT INTO translations(song_number, language, title, text) VALUES (?, ?, ?, ?)",
                (song.song_number, t.language, t.title, t.text),
            )
        await db.commit()
    return song

@app.patch("/songs/{song_number}", response_model=Song)
async def update_song(song_number: str, song: SongUpdate):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT song_number FROM songs WHERE song_number=?", (song_number,)) as cur:
            if not await cur.fetchone():
                raise HTTPException(status_code=404, detail="Song not found")
        if song.title_ta or song.text_ta:
            fields = []
            params = []
            if song.title_ta:
                fields.append("title_ta=?")
                params.append(song.title_ta)
            if song.text_ta:
                fields.append("text_ta=?")
                params.append(song.text_ta)
            params.append(song_number)
            await db.execute(f"UPDATE songs SET {', '.join(fields)} WHERE song_number=?", params)
        if song.translations:
            for t in song.translations:
                await db.execute(
                    "INSERT INTO translations(song_number, language, title, text) VALUES (?, ?, ?, ?)\nON CONFLICT(song_number, language) DO UPDATE SET title=excluded.title, text=excluded.text",
                    (song_number, t.language, t.title, t.text),
                )
        await db.commit()
        return await get_song(song_number)

@app.delete("/songs/{song_number}", status_code=204)
async def delete_song(song_number: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM songs WHERE song_number=?", (song_number,))
        await db.commit()
    return

import io
import json
from datetime import datetime
from pydub import AudioSegment
from openai import OpenAI
from .config import OPENAI_API_KEY, TZINFO

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = (
 "Ты помощник по задачам. Из входного текста выдели: "
 "title (краткий глагол+существительное), description (1–2 предложения), "
 "due (естественная дата/время на русском, если явно указано), "
 "context (метка из: AI, Horien, Дом, Финансы, Здоровье, Семья, System, Другое). "
 "Верни строго JSON с ключами: title, description, due, context."
)

def transcribe_ogg_to_text(ogg_bytes: bytes) -> str:
    audio = AudioSegment.from_file(io.BytesIO(ogg_bytes), format="ogg")
    wav_buf = io.BytesIO()
    audio.export(wav_buf, format="wav")
    wav_buf.seek(0)
    wav_buf.name = "voice.wav"
    tr = client.audio.transcriptions.create(
        model="whisper-1",
        file=wav_buf,
        response_format="text",
        language="ru",
    )
    return tr

def parse_task(text: str) -> dict:
    r = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text.strip()}
        ]
    )
    content = r.choices[0].message.content
    try:
        data = json.loads(content)
        return {
            "title": (data.get("title") or text.strip())[:200],
            "description": data.get("description") or "",
            "due": data.get("due") or "",
            "context": data.get("context") or "Другое",
        }
    except Exception:
        return {"title": text.strip()[:200], "description": "", "due": "", "context": "Другое"}

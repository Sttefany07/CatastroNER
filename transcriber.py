import assemblyai as aai
import os
from tempfile import NamedTemporaryFile


aai.settings.api_key = "15d836ea8999402391868018349ae35a"


config = aai.TranscriptionConfig(
    speech_model=aai.SpeechModel.best,
    language_code="es"
)

def transcribe_audio(audio_bytes: bytes):
    if not aai.settings.api_key:
        raise RuntimeError("ASSEMBLYAI_API_KEY no está configurada.")
    with NamedTemporaryFile(delete=False, suffix=".mp3") as temp_audio:
        temp_audio.write(audio_bytes)
        temp_audio.flush()
        path = temp_audio.name
    try:
        transcriber = aai.Transcriber(config=config)
        transcript = transcriber.transcribe(path)
        if transcript.status == "error":
            raise RuntimeError(f"Transcripción fallida: {transcript.error}")
        return {
            "text": transcript.text,
            "confidence": transcript.confidence,
            "words": [w.text for w in transcript.words],
            "utterances": transcript.utterances
        }
    finally:
        try:
            os.remove(path)
        except Exception:
            pass
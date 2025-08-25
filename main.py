from fastapi import FastAPI, File, UploadFile
from transcriber import transcribe_audio

app = FastAPI()

@app.post("/transcribir/")
async def transcribir(file: UploadFile = File(...)):
    audio_bytes = await file.read()
    resultado = transcribe_audio(audio_bytes)
    return resultado

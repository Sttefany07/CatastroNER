# 🏙️ CatastroNER

**CatastroNER** es un sistema desarrollado en **Python** con **FastAPI** que permite la **automatización del llenado de fichas catastrales** a partir de audio.  

El sistema integra:
- 🎙️ **AssemblyAI**: transcripción de voz a texto.
- 🧠 **spaCy (NER)**: extracción de entidades como nombre, DNI, dirección, códigos catastrales, etc.
- ✅ **Regex y validaciones**: control de formato para 41 campos de la ficha catastral.
- 📄 **Salida estructurada en JSON** (y próximamente exportación a Excel).

---

## 🚀 Características principales
- Procesa un archivo de audio y devuelve una ficha catastral completa.
- Los campos no detectados se devuelven como `null`.
- Validaciones específicas (DNI = 8 dígitos, teléfono = 9 dígitos iniciando en 9, región en mayúsculas, etc.).
- API documentada automáticamente con **Swagger UI** en `/docs`.
 

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
 
---

## 📂 Estructura del proyecto
```
CatastroNER/
 ├── app/                        # Código principal de la aplicación
 │   ├── main.py                  # Punto de entrada con FastAPI
 │   ├── pipeline_catastral.py    # Pipeline para procesamiento catastral
 │   ├── transcriber.py           # Módulo de transcripción de audio
 ├── data/                        # Archivos de datos y modelos
 │   ├── ubigeo.xlsx
 │   └── model-last-tuned/        # Carpeta con tu modelo entrenado
 ├── tests/                       # Pruebas unitarias y de integración
 ├── requirements.txt             # Dependencias del proyecto
 ├── README.md                    # Documentación
 ├── .gitignore                   # Archivos a ignorar en Git


## ⚙️ Instalación y uso

1. **Clonar el repositorio**
   ```bash
   git clone https://github.com/Sttefany07/CatastroNER.git
   cd CatastroNER
   ```

2. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   ```

3. **Ejecutar el servidor**
   ```bash
   uvicorn main:app --reload
   ```

4. **Abrir la API en el navegador**
   ```
   http://127.0.0.1:8000/docs
   ```

---

## 👩‍💻 Autor
Proyecto desarrollado por **Sttefany07**, enfocado en la aplicación de **IA y NLP en catastro urbano**.

---

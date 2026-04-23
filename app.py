import asyncio
import logging
import os
import tempfile

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse

# Patch openpyxl's number caster to tolerate NaN/Inf in numeric cells.
# Some Excel files (e.g. exports from external tools) store the literal
# string "NaN" with type="n", which crashes openpyxl's int() cast.
# Must run before `from markitdown import MarkItDown` so the patched
# function is in place when MarkItDown loads openpyxl.
import openpyxl.worksheet._reader as _openpyxl_reader

_original_cast_number = _openpyxl_reader._cast_number


def _safe_cast_number(value):
    try:
        return _original_cast_number(value)
    except ValueError:
        try:
            return float(value)
        except (ValueError, TypeError):
            return value


_openpyxl_reader._cast_number = _safe_cast_number

from markitdown import MarkItDown  # noqa: E402

# Patch XlsxConverter/XlsConverter.convert() to render NaN cells as empty
# strings in the intermediate HTML, instead of the literal "NaN". Reduces
# token noise and avoids confusing downstream LLM consumers that treat
# "NaN" as actual data.
import pandas as pd  # noqa: E402
from markitdown._base_converter import DocumentConverterResult  # noqa: E402
from markitdown.converters import _xlsx_converter  # noqa: E402


def _clean_xlsx_convert(self, file_stream, stream_info, **kwargs):
    sheets = pd.read_excel(file_stream, sheet_name=None, engine="openpyxl")
    md_content = ""
    for s in sheets:
        md_content += f"## {s}\n"
        html_content = sheets[s].to_html(index=False, na_rep="")
        md_content += (
            self._html_converter.convert_string(
                html_content, **kwargs
            ).markdown.strip()
            + "\n\n"
        )
    return DocumentConverterResult(markdown=md_content.strip())


def _clean_xls_convert(self, file_stream, stream_info, **kwargs):
    sheets = pd.read_excel(file_stream, sheet_name=None, engine="xlrd")
    md_content = ""
    for s in sheets:
        md_content += f"## {s}\n"
        html_content = sheets[s].to_html(index=False, na_rep="")
        md_content += (
            self._html_converter.convert_string(
                html_content, **kwargs
            ).markdown.strip()
            + "\n\n"
        )
    return DocumentConverterResult(markdown=md_content.strip())


_xlsx_converter.XlsxConverter.convert = _clean_xlsx_convert
_xlsx_converter.XlsConverter.convert = _clean_xls_convert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Max seconds a single conversion may run before we give up and return 504.
# Should be a bit lower than the client-side timeout so clients get a proper
# HTTP response instead of an abort.
CONVERT_TIMEOUT_SECONDS = int(os.getenv("CONVERT_TIMEOUT_SECONDS", "540"))

app = FastAPI(
    title="MarkItDown API Server",
    description="API endpoint to extract text and convert it to markdown, using MarkItDown (https://github.com/microsoft/markitdown).",
)


FORBIDDEN_EXTENSIONS = [
    # Executable and Script Files (Security Risk)
    "exe",
    "msi",
    "bat",
    "cmd",  # Windows
    "dmg",
    "pkg",
    "app",  # macOS
    "bin",
    "sh",
    "run",  # Linux/Unix
    "dll",
    "so",
    "dylib",  # Dynamic libraries
    "jar",
    "apk",  # Java/Android packages
    "vbs",
    "ps1",  # Windows scripting
    "pyc",
    "pyo",  # Compiled Python
    # System and Configuration Files
    "sys",
    "drv",  # System and driver files
    "config",
    "ini",  # Configuration files
    # Binary Data Files
    "dat",
    "bin",  # Generic binary data
    "db",
    "sqlite",
    "mdb",  # Database files
    "dbf",
    "myd",  # Database format files
    # CAD and Specialized Technical Files
    "dxf",
    "dwg",  # AutoCAD files
    "stl",
    "obj",
    "3ds",  # 3D model files
    "blend",  # Blender 3D files
    # Encrypted/Protected Files
    "gpg",
    "asc",
    "pgp",  # Encrypted files
    # Virtual Machine and Container Files
    "vdi",
    "vmdk",
    "ova",  # Virtual machine disks
    "docker",
    "containerd",  # Container images
    # Other Binary Formats
    "class",  # Java class files
    "o",
    "a",  # Object and archive files
    "lib",
    "obj",  # Compiled library files
    "ttf",
    "otf",  # Font files
    "fon",  # Windows font resource
]


def is_forbidden_file(filename):
    return (
        "." in filename and filename.rsplit(".", 1)[1].lower() in FORBIDDEN_EXTENSIONS
    )


def convert_to_md(filepath: str) -> str:
    logger.info(f"Converting file: {filepath}")
    markitdown = MarkItDown()
    result = markitdown.convert(filepath)
    logger.info(f"Conversion result: {result.text_content[:100]}")
    return result.text_content


@app.get("/")
def read_root():
    return {"MarkItDown API Server": "hit /docs for endpoint reference"}


@app.post("/process_file")
async def process_file(file: UploadFile = File(...)):
    if is_forbidden_file(file.filename):
        return JSONResponse(content={"error": "File type not allowed"}, status_code=400)

    try:
        # Save the file to a temporary directory
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(await file.read())
            temp_file_path = temp_file.name
            logger.info(f"Temporary file path: {temp_file_path}")

        # Convert the file to markdown in a worker thread so the event loop
        # stays responsive, with a hard timeout to avoid stuck workers.
        loop = asyncio.get_event_loop()
        markdown_content = await asyncio.wait_for(
            loop.run_in_executor(None, convert_to_md, temp_file_path),
            timeout=CONVERT_TIMEOUT_SECONDS,
        )
        logger.info("File converted to markdown successfully")

    except asyncio.TimeoutError:
        logger.error(
            f"Conversion timed out after {CONVERT_TIMEOUT_SECONDS}s: {file.filename}"
        )
        return JSONResponse(
            content={
                "error": f"Conversion timed out after {CONVERT_TIMEOUT_SECONDS}s"
            },
            status_code=504,
        )

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        # Ensure the temporary file is deleted
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"Temporary file deleted: {temp_file_path}")

    return JSONResponse(content={"markdown": markdown_content})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8490)

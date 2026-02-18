import sys
from pathlib import Path

ROOT = Path(__file__).parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from facturador.mail_trigger_service import app


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)


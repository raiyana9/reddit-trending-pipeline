import uvicorn

from services.api.app import create_app
from shared.config import get_settings

if __name__ == "__main__":
    s = get_settings()
    uvicorn.run(create_app(s), host="0.0.0.0", port=s.api_port, log_level=s.log_level.lower(),
                ws_ping_interval=None)

"""
Default header
"""
DEFAULT_HEADERS = {
    "Connection": "keep-alive",
    "Expires": str(-1),
    "Upgrade-Insecure-Requests": str(1),
    # Google Chrome:
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ),
}

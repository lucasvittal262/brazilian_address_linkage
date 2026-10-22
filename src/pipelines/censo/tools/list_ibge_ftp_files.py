from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, unquote
from urllib.request import Request, urlopen
 

 
 
class _LinkParser(HTMLParser):
    """Collects the href of every <a> tag."""
 
    def __init__(self):
        super().__init__()
        self.hrefs = []
 
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value)
 
 
def list_files(url: str, timeout: int = 30) -> list[str]:
    """Fetch the HTML page at `url` and return the file names listed in it.
 
    Skips sorting links (?C=N;O=D), parent directory links and sub-directories.
    """
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        html = resp.read().decode(charset, errors="replace")
 
    parser = _LinkParser()
    parser.feed(html)
 
    base_path = urlparse(url).path
    files = []
    for href in parser.hrefs:
        if href.startswith(("?", "#", "mailto:", "javascript:")):
            continue
        full = urljoin(url, href)
        parsed = urlparse(full)
        # keep only links that live directly inside the listed directory
        if parsed.netloc != urlparse(url).netloc:
            continue
        if not parsed.path.startswith(base_path) or parsed.path == base_path:
            continue
        name = parsed.path[len(base_path):]
        if not name or "/" in name:  # sub-directory or parent
            continue
        name = unquote(name)
        if name not in files:
            files.append(name)
    return files
import pytest

from serena.connection import open_connection
from serena.enums import ReplyCode
from serena.exc import UnexpectedCloseError
from serena.payloads.header import BasicHeader

pytestmark = pytest.mark.anyio



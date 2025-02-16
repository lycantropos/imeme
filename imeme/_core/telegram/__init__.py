from typing import TypeAlias

import imeme._core.telegram.fetching
import imeme._core.telegram.peer

from . import main as _messages

RawPeer: TypeAlias = imeme._core.telegram.peer.RawPeer
sync_images = _messages.sync_images

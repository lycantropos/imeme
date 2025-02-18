from typing import TypeAlias

from . import main as _main, peer as _peer

RawPeer: TypeAlias = _peer.RawPeer
Peer: TypeAlias = _peer.Peer
classify_peer_language = _main.classify_peer_languages
sync_images = _main.sync_images
sync_images_ocr = _main.sync_images_ocr

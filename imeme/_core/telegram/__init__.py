from typing import TypeAlias

from . import image as _image, main as _main, peer as _peer

Image: TypeAlias = _image.Image
RawPeer: TypeAlias = _peer.RawPeer
Peer: TypeAlias = _peer.Peer
classify_peer_language = _main.classify_peer_languages
iter_images = _main.iter_images
sync_images = _main.sync_images
sync_images_ocr = _main.sync_images_ocr

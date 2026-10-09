"""Bound validated Provider declarations independently of live sessions."""

from __future__ import annotations

from collections import OrderedDict

from stateforge_agent._sfp._services.manifest.declaration import (
    ManifestCacheKey,
    ManifestDeclaration,
)
from stateforge_agent._sfp._services.manifest.limits import (
    DEFAULT_CACHE_ENTRIES,
)


class ManifestCache:
    """Retain a bounded least-recently-used set of validated declarations."""

    def __init__(self, capacity: int = DEFAULT_CACHE_ENTRIES) -> None:
        """Create an empty cache with a positive entry limit."""
        if capacity <= 0:
            raise ValueError("Manifest cache capacity must be positive")
        self._capacity = capacity
        self._entries: OrderedDict[ManifestCacheKey, ManifestDeclaration] = (
            OrderedDict()
        )

    def get(self, key: ManifestCacheKey) -> ManifestDeclaration | None:
        """Return and refresh a validated declaration when present."""
        declaration = self._entries.get(key)
        if declaration is not None:
            self._entries.move_to_end(key)
        return declaration

    def put(self, declaration: ManifestDeclaration) -> None:
        """Store one validated declaration and evict the oldest if needed."""
        self._entries[declaration.key] = declaration
        self._entries.move_to_end(declaration.key)
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        """Return the number of retained validated declarations."""
        return len(self._entries)

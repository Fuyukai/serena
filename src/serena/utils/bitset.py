from collections.abc import Iterator
from math import ceil


class BitSet:
    """
    An indexable, compact array of booleans that is stored using integers.
    """

    def __init__(self, size: int) -> None:
        self._items: list[int] = [0] * ceil(size / 64)

    def _get_index(self, idx: int) -> int:
        index = idx // 64
        if index > len(self._items):
            raise IndexError(f"index {idx} out of range (max: {len(self._items) * 64})") from None

        return index

    def __len__(self) -> int:
        return len(self._items) * 64

    def __iter__(self) -> Iterator[bool]:
        for idx in range(0, len(self)):
            yield self[idx]

    def __setitem__(self, key: int, value: bool) -> None:
        index = self._get_index(key)
        item = self._items[index]

        bit_idx = key % 64
        item &= ~(1 << bit_idx)
        if value:
            item |= 1 << bit_idx

        self._items[index] = item

    def __getitem__(self, item: int) -> bool:
        index = self._get_index(item)
        bit_idx = item % 64
        return (self._items[index] & (1 << bit_idx)) != 0

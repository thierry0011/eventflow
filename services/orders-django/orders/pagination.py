"""
Pagination.

WHY OFFSET PAGINATION BREAKS — a thing every senior engineer has been bitten by.

`LIMIT 25 OFFSET 100000` makes Postgres read and discard 100,000 rows before
returning 25. Page 1 is instant, page 4000 times out. Worse, if a new row is
inserted while a user pages, every subsequent page SHIFTS and the user sees a
duplicate or misses a record entirely.

CURSOR pagination fixes both: it asks for "the 25 rows after this exact point"
(`WHERE created_at < :cursor ORDER BY created_at DESC LIMIT 25`), which uses an
index and never shifts. Constant time at any depth.

The trade-off, and it is a real one: you lose "jump to page 7" and you lose a
total count. For infinite-scroll feeds and APIs, that is the right trade. For an
admin table with page numbers, offset is acceptable — IF the dataset is small
and bounded.

Both are offered here so you can feel the difference.
"""

from rest_framework.pagination import CursorPagination, PageNumberPagination
from rest_framework.response import Response


class CursorPageNumberPagination(CursorPagination):
    page_size = 25
    page_size_query_param = "page_size"
    max_page_size = 100
    # max_page_size matters: without it a client sends ?page_size=1000000 and
    # pages your service to death. Always bound client-controlled limits.

    ordering = "-created_at"
    # The cursor field MUST be indexed and effectively unique. `-created_at`
    # alone can tie if two rows share a timestamp; in a high-volume system you
    # order by (created_at, id) so the cursor is unambiguous.


class StandardPageNumberPagination(PageNumberPagination):
    """Offset pagination, for small bounded sets where page numbers are wanted."""

    page_size = 25
    page_size_query_param = "page_size"
    max_page_size = 100

    def get_paginated_response(self, data):
        return Response({
            "count": self.page.paginator.count,
            "page": self.page.number,
            "pages": self.page.paginator.num_pages,
            "next": self.get_next_link(),
            "previous": self.get_previous_link(),
            "results": data,
        })

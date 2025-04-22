import uvloop
from uuid import UUID

from prs_connector_core.buffer import BufferManager

async def main ():
    con_id = "550e8400-e29b-41d4-a716-446655440000"
    buffer = BufferManager(UUID(con_id))

    # Call the load method
    result = await buffer.load()

    print(result)

if __name__ == "__main__":
    uvloop.run(main())
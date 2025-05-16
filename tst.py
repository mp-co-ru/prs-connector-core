import asyncio
import aiomqtt

async def main():
    while True:
        try:
            async with aiomqtt.Client("server") as client:
                await asyncio.Future()
        except:
            pass

asyncio.run(main())

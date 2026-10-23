import asyncio
import aiohttp


async def main():
    print("Проверяем aiohttp без proxy из окружения...")

    timeout = aiohttp.ClientTimeout(
        total=15,
        connect=10
    )

    connector = aiohttp.TCPConnector(
        family=2,
        ssl=True
    )

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            trust_env=False
        ) as session:

            async with session.get(
                "https://api.telegram.org"
            ) as response:

                print("Статус:", response.status)
                print("aiohttp работает!")

    except Exception as e:
        print("ОШИБКА:")
        print(type(e).__name__)
        print(repr(e))


asyncio.run(main())